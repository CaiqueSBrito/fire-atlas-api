-- NASA-018 Gate B.2 (N2): assinatura semantica canonica pos-parsing (considerando cabecalho/esquema).
-- Registros legados anteriores a V4 preservam NULL sem fabricacao retroativa.

ALTER TABLE detection
    ADD COLUMN source_semantic_hash text;

CREATE INDEX idx_detection_source_semantic
    ON detection (acq_date, source_file, source_line_number, source_semantic_hash)
    WHERE source_line_number IS NOT NULL AND source_semantic_hash IS NOT NULL;
