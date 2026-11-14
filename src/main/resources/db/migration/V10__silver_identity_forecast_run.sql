-- Prata -> detection: identidade do foco pelo record_id da prata (sha256 do arquivo de origem + numero da linha) e
-- chave de juncao h3_r6 com a atividade harmonizada e a previsao do gold.
-- Linhas que vieram do CSV da FIRMS ficam com record_id e h3_r6 nulos (nada e fabricado).

ALTER TABLE detection
    ADD COLUMN IF NOT EXISTS record_id text,
    ADD COLUMN IF NOT EXISTS h3_r6 bigint;

-- A tabela e particionada por acq_date: a chave unica inclui a data. Permite reimportar a mesma prata sem duplicar.
CREATE UNIQUE INDEX IF NOT EXISTS uq_detection_record_id
    ON detection (acq_date, record_id)
    WHERE record_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_detection_h3_r6
    ON detection (h3_r6)
    WHERE h3_r6 IS NOT NULL;

COMMENT ON COLUMN detection.record_id IS 'Identidade do foco na prata: <sha256 do arquivo de origem>:<numero da linha>. Nulo nas linhas do CSV da FIRMS.';
COMMENT ON COLUMN detection.h3_r6 IS 'Celula H3 de resolucao 6 do foco, como veio da prata (a API nao a calcula). Chave de juncao com gold/harmonized e gold/forecast (h3_cell com resolution = 6).';

-- Metadados de cada emissao da previsao, como o pipeline de dados gravou no commit do Delta (nada e inventado).
CREATE TABLE IF NOT EXISTS forecast_run (
    issue_date     date        PRIMARY KEY,
    source_key     text        NOT NULL,
    model_sha256   text,
    method_version text,
    silver_version text,
    imported_at    timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE forecast_run IS 'Metadados da emissao da previsao (gold/forecast): modelo, metodo e versao da prata usados pelo pipeline.';
