package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/engine"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

const kafkaRequestTimeout = 10 * time.Second

type Worker struct {
	docs   *repo.KBRepo
	engine *engine.Engine
	minio  *minio.Client
	bucket string
	kafka  config.Kafka
	cfg    config.Worker
}

func New(docs *repo.KBRepo, eng *engine.Engine, mc *minio.Client, bucket string, kafka config.Kafka, cfg config.Worker) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.TaskTimeout <= 0 {
		cfg.TaskTimeout = 10 * time.Minute
	}
	return &Worker{docs: docs, engine: eng, minio: mc, bucket: bucket, kafka: kafka, cfg: cfg}
}

type documentIndexEvent struct {
	DocID int64 `json:"doc_id"`
}

func (w *Worker) Run(ctx context.Context) error {
	if len(w.kafka.Brokers) == 0 || w.kafka.ConsumerGroup == "" || w.kafka.DocTopic == "" ||
		w.kafka.DocDLQTopic == "" || w.kafka.DocTopic == w.kafka.DocDLQTopic {
		return fmt.Errorf("kafka: configure brokers, consumer group and distinct document and DLQ topics")
	}
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(w.kafka.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(kafkaRequestTimeout),
	)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer producer.Close()

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		w.publishOutbox(ctx, producer)
		return nil
	})
	group.Go(func() error {
		w.recoverUploads(ctx)
		return nil
	})
	for range w.cfg.Concurrency {
		group.Go(func() error { return w.consumeLoop(ctx, producer) })
	}
	return group.Wait()
}

func (w *Worker) consumeLoop(ctx context.Context, producer *kgo.Client) error {
	for ctx.Err() == nil {
		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(w.kafka.Brokers...),
			kgo.ConsumerGroup(w.kafka.ConsumerGroup),
			kgo.ConsumeTopics(w.kafka.DocTopic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.RebalanceTimeout(w.cfg.TaskTimeout+time.Minute),
		)
		if err != nil {
			return fmt.Errorf("kafka consumer: %w", err)
		}
		err = w.consume(ctx, consumer, producer)
		consumer.CloseAllowingRebalance()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			log.Printf("[worker] kafka consume: %v", err)
		}
		// A fresh consumer resumes at the last committed offset. A failed
		// record must not be skipped by a commit for a later record.
		wait(ctx, time.Second)
	}
	return nil
}

func (w *Worker) consume(ctx context.Context, consumer, producer *kgo.Client) error {
	for ctx.Err() == nil {
		fetches := consumer.PollRecords(ctx, 1)
		if err := fetches.Err(); err != nil {
			return err
		}
		for _, record := range fetches.Records() {
			err := w.processRecord(ctx, producer, record)
			if err == nil {
				commitCtx, cancel := context.WithTimeout(ctx, kafkaRequestTimeout)
				err = consumer.CommitRecords(commitCtx, record)
				cancel()
			}
			consumer.AllowRebalance()
			if err != nil {
				return fmt.Errorf("%s partition %d offset %d: %w", record.Topic, record.Partition, record.Offset, err)
			}
		}
	}
	return ctx.Err()
}

func (w *Worker) processRecord(ctx context.Context, producer *kgo.Client, record *kgo.Record) error {
	var event documentIndexEvent
	if err := json.Unmarshal(record.Value, &event); err != nil || event.DocID <= 0 {
		return w.publishDLQ(ctx, producer, record, 0, fmt.Errorf("invalid document index message"))
	}

	taskCtx, cancel := context.WithTimeout(ctx, w.cfg.TaskTimeout)
	defer cancel()
	var err error
	for retry := 0; ; retry++ {
		err = w.handleDocETL(taskCtx, event.DocID)
		if err == nil {
			return nil
		}
		if retry >= w.cfg.MaxRetry || !wait(taskCtx, retryDelay(retry)) {
			break
		}
		log.Printf("[worker] retry doc-etl %d: %v", event.DocID, err)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	log.Printf("[worker] doc-etl %d failed: %v", event.DocID, err)
	if updateErr := w.docs.FailDocumentIndex(ctx, event.DocID, errorText(err)); updateErr != nil {
		return fmt.Errorf("save index failure: %w", updateErr)
	}
	return w.publishDLQ(ctx, producer, record, event.DocID, err)
}

func (w *Worker) publishDLQ(ctx context.Context, producer *kgo.Client, record *kgo.Record, docID int64, cause error) error {
	payload, err := json.Marshal(struct {
		DocID     int64  `json:"doc_id,omitempty"`
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Offset    int64  `json:"offset"`
		Error     string `json:"error"`
	}{docID, record.Topic, record.Partition, record.Offset, errorText(cause)})
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("%s/%d/%d", record.Topic, record.Partition, record.Offset))
	if err := publish(ctx, producer, &kgo.Record{Topic: w.kafka.DocDLQTopic, Key: key, Value: payload}); err != nil {
		return fmt.Errorf("publish dead letter: %w", err)
	}
	return nil
}

func publish(ctx context.Context, producer *kgo.Client, record *kgo.Record) error {
	callCtx, cancel := context.WithTimeout(ctx, kafkaRequestTimeout)
	defer cancel()
	return producer.ProduceSync(callCtx, record).FirstErr()
}

func (w *Worker) publishIndexJob(ctx context.Context, producer *kgo.Client, job repo.DocumentIndexJob) error {
	payload, err := json.Marshal(documentIndexEvent{DocID: job.DocID})
	if err != nil {
		return err
	}
	return publish(ctx, producer, &kgo.Record{
		Topic: w.kafka.DocTopic,
		Key:   []byte(strconv.FormatInt(job.DocID, 10)),
		Value: payload,
	})
}

func retryDelay(retry int) time.Duration {
	return time.Second * time.Duration(1<<min(retry, 6))
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func errorText(err error) string {
	runes := []rune(err.Error())
	return string(runes[:min(len(runes), 500)])
}
