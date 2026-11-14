-- NASA-018 Gate B.3 (C2-F01): alinha a unicidade por linha-fonte com a interpretacao semantica
-- efetiva (source_semantic_hash) em vez do hash textual bruto da linha, evitando conflitos
-- indevidos em revisoes parciais por bbox com mudanca de cabecalho.
-- Preserva registros legados pre-V3/V4 com colunas NULL sem fabricacao retroativa.

DROP INDEX IF EXISTS uq_detection_source_line_hash;

DROP INDEX IF EXISTS idx_detection_source_semantic;

CREATE UNIQUE INDEX uq_detection_source_line_semantic
    ON detection (acq_date, source_file, source_line_number, source_semantic_hash)
    WHERE source_line_number IS NOT NULL AND source_semantic_hash IS NOT NULL;

CREATE INDEX idx_detection_source_raw_hash
    ON detection (acq_date, source_file, source_line_number, source_row_hash)
    WHERE source_line_number IS NOT NULL AND source_row_hash IS NOT NULL;
