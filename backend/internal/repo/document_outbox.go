package repo

import (
	"context"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DocumentIndexJob 是一条已领取的待发任务。Attempts 同时标识本次领取。
type DocumentIndexJob struct {
	DocID    int64
	Attempts int
}

// QueueDocumentIndex 在同一笔事务中记录上传完成和待发任务。
// 正常上传和后台补偿可以同时调用，每篇文档只会创建一条任务。
func (r *KBRepo) QueueDocumentIndex(ctx context.Context, docID int64) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var doc model.Document
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id", "status").First(&doc, docID).Error; err != nil {
			return err
		}
		if doc.Status != model.DocStatusUploading && doc.Status != model.DocStatusFailed {
			var job DocumentIndexJob
			return tx.Table("document_index_outbox").Select("doc_id").Where("doc_id = ?", docID).Take(&job).Error
		}
		job := tx.Exec("INSERT INTO document_index_outbox (doc_id) VALUES (?) ON CONFLICT (doc_id) DO NOTHING", docID)
		if job.Error != nil || job.RowsAffected == 0 {
			return job.Error
		}
		return tx.Model(&doc).Updates(map[string]any{"status": model.DocStatusPending, "error": ""}).Error
	})
}

// StaleUploads 按 ID 分页检查未记录上传完成的文档。
func (r *KBRepo) StaleUploads(ctx context.Context, before time.Time, afterID int64) ([]model.Document, error) {
	var docs []model.Document
	err := r.db.WithContext(ctx).
		Where("status = ? AND updated_at < ? AND id > ?", model.DocStatusUploading, before, afterID).
		Order("id").Limit(100).Find(&docs).Error
	return docs, err
}

// FailUpload 仅更新本次核对时观察到的上传状态。
func (r *KBRepo) FailUpload(ctx context.Context, doc *model.Document, message string) error {
	return r.db.WithContext(ctx).Model(&model.Document{}).
		Where("id = ? AND status = ? AND updated_at = ?", doc.ID, model.DocStatusUploading, doc.UpdatedAt).
		Updates(map[string]any{"status": model.DocStatusFailed, "error": message}).Error
}

// ClaimDocumentIndexJob 原子领取一条任务，租期内释放数据库锁后再发送。
func (r *KBRepo) ClaimDocumentIndexJob(ctx context.Context) (DocumentIndexJob, bool, error) {
	var job DocumentIndexJob
	result := r.db.WithContext(ctx).Raw(`
		WITH next_job AS (
			SELECT doc_id FROM document_index_outbox
			WHERE published_at IS NULL AND next_attempt_at <= NOW()
			ORDER BY next_attempt_at, doc_id
			LIMIT 1 FOR UPDATE SKIP LOCKED
		)
		UPDATE document_index_outbox AS job
		SET attempts = job.attempts + 1, next_attempt_at = NOW() + INTERVAL '30 seconds'
		FROM next_job
		WHERE job.doc_id = next_job.doc_id
		RETURNING job.doc_id, job.attempts
	`).Scan(&job)
	return job, result.RowsAffected == 1, result.Error
}

func (r *KBRepo) MarkIndexJobPublished(ctx context.Context, job DocumentIndexJob) error {
	return r.db.WithContext(ctx).Exec(`
		UPDATE document_index_outbox SET published_at = NOW()
		WHERE doc_id = ? AND attempts = ? AND published_at IS NULL
	`, job.DocID, job.Attempts).Error
}

func (r *KBRepo) RescheduleIndexJob(ctx context.Context, job DocumentIndexJob, seconds int) error {
	return r.db.WithContext(ctx).Exec(`
		UPDATE document_index_outbox SET next_attempt_at = NOW() + (? * INTERVAL '1 second')
		WHERE doc_id = ? AND attempts = ? AND published_at IS NULL
	`, seconds, job.DocID, job.Attempts).Error
}

func (r *KBRepo) FailDocumentIndex(ctx context.Context, docID int64, message string) error {
	return r.db.WithContext(ctx).Model(&model.Document{}).
		Where("id = ? AND status <> ?", docID, model.DocStatusReady).
		Updates(map[string]any{"status": model.DocStatusFailed, "error": message}).Error
}
