-- NASA-018 Gate B.1 (C2/C3/C4): proveniencia por registro-fonte e historico detalhado de reingestao.
-- Colunas permanecem NULL para linhas legadas pre-V3 sem fabricacao de checksum ou numero de linha.

ALTER TABLE detection
    ADD COLUMN source_line_number bigint,
    ADD COLUMN source_row_hash text,
    ADD COLUMN source_file_sha256 text;

CREATE UNIQUE INDEX uq_detection_source_line_hash
    ON detection (acq_date, source_file, source_line_number, source_row_hash)
    WHERE source_line_number IS NOT NULL AND source_row_hash IS NOT NULL;

CREATE INDEX idx_detection_source_scope
    ON detection (source_file, acq_date);

ALTER TABLE ingestion_log
    ADD COLUMN source_sha256 text,
    ADD COLUMN rows_inserted bigint,
    ADD COLUMN rows_unchanged bigint,
    ADD COLUMN rows_deleted bigint;
