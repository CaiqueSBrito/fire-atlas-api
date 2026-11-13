-- RN03: integridade no proprio banco (defesa em profundidade; a validacao principal e na ingestao).
ALTER TABLE detection
    ADD CONSTRAINT detection_lat_chk  CHECK (lat BETWEEN -90 AND 90),
    ADD CONSTRAINT detection_lon_chk  CHECK (lon BETWEEN -180 AND 180),
    ADD CONSTRAINT detection_frp_chk  CHECK (frp IS NULL OR frp >= 0),
    ADD CONSTRAINT detection_conf_chk CHECK (confidence_class IS NULL OR confidence_class IN ('l', 'n', 'h'));

-- RN05: medicoes originais sao imutaveis. Correcoes entram por nova carga (DELETE + COPY), nunca por UPDATE.
CREATE FUNCTION detection_block_update() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'detection e imutavel: valores originais nao podem ser alterados (RN05)';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_detection_immutable
    BEFORE UPDATE ON detection
    FOR EACH ROW EXECUTE FUNCTION detection_block_update();

-- RN05: resultados derivados (harmonizacao etc.) ficam em tabela propria, sem tocar nas deteccoes originais.
CREATE TABLE harmonization_run (
    id             bigserial   PRIMARY KEY,
    created_at     timestamptz NOT NULL DEFAULT now(),
    method         text        NOT NULL,
    method_version text        NOT NULL,
    bbox           text        NOT NULL,
    period_start   date        NOT NULL,
    period_end     date        NOT NULL,
    params         jsonb       NOT NULL,
    result         jsonb       NOT NULL
);
CREATE INDEX idx_harmonization_run_period ON harmonization_run (period_start, period_end);

-- RN03/RN04: rejeicoes por carga ficam registradas junto da origem.
ALTER TABLE ingestion_log
    ADD COLUMN rows_rejected bigint NOT NULL DEFAULT 0,
    ADD COLUMN rejects       jsonb  NOT NULL DEFAULT '{}'::jsonb;
