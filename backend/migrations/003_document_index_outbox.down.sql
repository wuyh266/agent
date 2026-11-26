DROP INDEX idx_documents_uploading;
UPDATE documents SET status = 'pending' WHERE status = 'uploading';

DROP INDEX idx_document_chunks_index_position;
ALTER TABLE document_chunks DROP COLUMN index_position;
DROP TABLE document_index_outbox;
