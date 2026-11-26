CREATE TABLE document_index_outbox (
    doc_id          BIGINT PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX idx_document_index_outbox_pending
    ON document_index_outbox (next_attempt_at, doc_id)
    WHERE published_at IS NULL;

ALTER TABLE document_chunks ADD COLUMN index_position INT;
CREATE UNIQUE INDEX idx_document_chunks_index_position
    ON document_chunks (document_id, index_position);

-- 旧版尚未完成的 Redis Streams 任务由新 Worker 核对 MinIO 后补投。
UPDATE documents SET status = 'uploading'
WHERE status IN ('pending', 'parsing', 'chunking', 'embedding');

CREATE INDEX idx_documents_uploading ON documents (id) WHERE status = 'uploading';
