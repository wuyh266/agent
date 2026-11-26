package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/loader"
	"github.com/minio/minio-go/v7"
	"gorm.io/gorm"
)

// handleDocETL 是与消息队列无关的文档索引流程。
func (w *Worker) handleDocETL(ctx context.Context, docID int64) error {
	doc, err := w.docs.GetDoc(ctx, docID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil // 文档已删除，消费在途任务。
	}
	if err != nil {
		return fmt.Errorf("get doc %d: %w", docID, err)
	}
	if doc.Status == model.DocStatusReady {
		return nil
	}
	kb, err := w.docs.GetKB(ctx, doc.KBID)
	if err != nil {
		return fmt.Errorf("get kb %d: %w", doc.KBID, err)
	}

	doc.Status = model.DocStatusParsing
	doc.Error = ""
	if err := w.docs.UpdateDoc(ctx, doc); err != nil {
		return fmt.Errorf("set parsing: %w", err)
	}
	data, err := w.getObject(ctx, doc.ObjectKey)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	loaded, err := loader.Load(ctx,doc.Filename, data)
	if err != nil {
		return fmt.Errorf("parse %s: %w", doc.Filename, err)
	}
	doc.Status = model.DocStatusChunking
	if err := w.docs.UpdateDoc(ctx, doc); err != nil {
		return fmt.Errorf("set chunking: %w", err)
	}
	if err := w.engine.IndexDocument(ctx, kb, doc, loaded); err != nil {
		return fmt.Errorf("index: %w", err)
	}

	doc.Status = model.DocStatusReady
	if err := w.docs.UpdateDoc(ctx, doc); err != nil {
		return fmt.Errorf("set ready: %w", err)
	}
	log.Printf("[worker] doc-etl %d ready: chunks=%d tokens=%d", doc.ID, doc.ChunkCount, doc.TokenCount)
	return nil
}

func (w *Worker) getObject(ctx context.Context, key string) ([]byte, error) {
	obj, err := w.minio.GetObject(ctx, w.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	return io.ReadAll(obj)
}
