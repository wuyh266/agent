package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (w *Worker) publishOutbox(ctx context.Context, producer *kgo.Client) {
	for ctx.Err() == nil {
		job, found, err := w.docs.ClaimDocumentIndexJob(ctx)
		if err != nil {
			log.Printf("[worker] claim document index job: %v", err)
		}
		if err != nil || !found {
			wait(ctx, time.Second)
			continue
		}
		if err := w.publishIndexJob(ctx, producer, job); err != nil {
			log.Printf("[worker] publish index job %d: %v", job.DocID, err)
			delay := int(retryDelay(job.Attempts-1) / time.Second)
			if err := w.docs.RescheduleIndexJob(ctx, job, delay); err != nil {
				log.Printf("[worker] reschedule index job %d: %v", job.DocID, err)
			}
			continue
		}
		if err := w.docs.MarkIndexJobPublished(ctx, job); err != nil {
			log.Printf("[worker] mark index job %d published: %v", job.DocID, err)
		}
	}
}

func (w *Worker) recoverUploads(ctx context.Context) {
	for ctx.Err() == nil {
		if err := w.reconcileUploads(ctx); err != nil {
			log.Printf("[worker] reconcile uploads: %v", err)
		}
		wait(ctx, 30*time.Second)
	}
}

func (w *Worker) reconcileUploads(ctx context.Context) error {
	before := time.Now().Add(-5 * time.Minute)
	var afterID int64
	for ctx.Err() == nil {
		docs, err := w.docs.StaleUploads(ctx, before, afterID)
		if err != nil || len(docs) == 0 {
			return err
		}
		for i := range docs {
			doc := &docs[i]
			if err := w.recoverUpload(ctx, doc); err != nil {
				log.Printf("[worker] recover upload %d: %v", doc.ID, err)
			}
			afterID = doc.ID
		}
	}
	return ctx.Err()
}

func (w *Worker) recoverUpload(ctx context.Context, doc *model.Document) error {
	const missingObject = "原文件上传未完成，请重新上传"
	if doc.ObjectKey == "" {
		return w.docs.FailUpload(ctx, doc, missingObject)
	}
	statCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	info, err := w.minio.StatObject(statCtx, w.bucket, doc.ObjectKey, minio.StatObjectOptions{})
	cancel()
	if err != nil {
		switch minio.ToErrorResponse(err).Code {
		case "NoSuchKey", "NoSuchObject", "NotFound":
			return w.docs.FailUpload(ctx, doc, missingObject)
		default:
			return fmt.Errorf("stat object: %w", err)
		}
	}
	if info.Size != doc.FileSize {
		return w.docs.FailUpload(ctx, doc, "原文件大小与上传记录不符")
	}
	return w.docs.QueueDocumentIndex(ctx, doc.ID)
}
