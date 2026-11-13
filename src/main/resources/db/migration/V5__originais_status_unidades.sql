-- RN04 (card de ingestao): campos originais da FIRMS que antes eram descartados.
ALTER TABLE detection
    ADD COLUMN instrument text,        -- coluna "instrument" do CSV (ex.: MODIS, VIIRS, SNPP)
    ADD COLUMN fire_type smallint;     -- coluna "type" do CSV (so nos arquivos archive)

-- Imutabilidade (RN05): os novos campos tambem sao medicoes originais.
CREATE OR REPLACE FUNCTION detection_block_update() RETURNS trigger AS $$
BEGIN
    IF ROW(NEW.sensor, NEW.satellite, NEW.acq_date, NEW.acq_ts, NEW.lat, NEW.lon, NEW.brightness,
           NEW.bright_t31, NEW.scan, NEW.track, NEW.frp, NEW.confidence_raw, NEW.confidence_class,
           NEW.daynight, NEW.version, NEW.source_product, NEW.source_file, NEW.instrument, NEW.fire_type)
       IS DISTINCT FROM
       ROW(OLD.sensor, OLD.satellite, OLD.acq_date, OLD.acq_ts, OLD.lat, OLD.lon, OLD.brightness,
           OLD.bright_t31, OLD.scan, OLD.track, OLD.frp, OLD.confidence_raw, OLD.confidence_class,
           OLD.daynight, OLD.version, OLD.source_product, OLD.source_file, OLD.instrument, OLD.fire_type)
    THEN
        RAISE EXCEPTION 'detection e imutavel: valores originais nao podem ser alterados (RN05)';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- RN04: unidades dos valores originais documentadas no proprio esquema.
COMMENT ON TABLE detection IS 'Deteccoes de focos de calor MODIS/VIIRS (FIRMS) em esquema comum; valores originais preservados.';
COMMENT ON COLUMN detection.sensor IS 'MODIS ou VIIRS (catalogo sensor)';
COMMENT ON COLUMN detection.satellite IS 'Valor original da coluna satellite: Terra, Aqua, SNPP, N20, N21';
COMMENT ON COLUMN detection.acq_ts IS 'Instante de aquisicao em UTC (acq_date + acq_time originais)';
COMMENT ON COLUMN detection.lat IS 'Latitude WGS84 em graus decimais';
COMMENT ON COLUMN detection.lon IS 'Longitude WGS84 em graus decimais';
COMMENT ON COLUMN detection.brightness IS 'Temperatura de brilho em kelvin (MODIS: canal 21/22; VIIRS: banda I-4)';
COMMENT ON COLUMN detection.bright_t31 IS 'Temperatura de brilho em kelvin (MODIS: canal 31; VIIRS: banda I-5)';
COMMENT ON COLUMN detection.scan IS 'Tamanho do pixel ao longo da varredura, em km';
COMMENT ON COLUMN detection.track IS 'Tamanho do pixel ao longo da trilha, em km';
COMMENT ON COLUMN detection.frp IS 'Fire Radiative Power em megawatts (MW)';
COMMENT ON COLUMN detection.confidence_raw IS 'Confianca original da FIRMS: MODIS 0-100 (%), VIIRS l/n/h';
COMMENT ON COLUMN detection.confidence_class IS 'Confianca harmonizada l/n/h (derivada; MODIS: 0-29 l, 30-79 n, 80-100 h)';
COMMENT ON COLUMN detection.daynight IS 'D (dia) ou N (noite)';
COMMENT ON COLUMN detection.version IS 'Versao/stream do produto (ex.: 6.1NRT, 2.0NRT)';
COMMENT ON COLUMN detection.fire_type IS 'FIRMS type: 0 incendio de vegetacao presumido, 1 vulcao ativo, 2 fonte estatica em terra, 3 offshore';
COMMENT ON COLUMN detection.source_product IS 'Produto de origem no data lake (ex.: VIIRS_S-NPP)';
COMMENT ON COLUMN detection.source_file IS 'Chave do arquivo de origem no data lake (proveniencia)';
COMMENT ON COLUMN detection.h3 IS 'Indice H3 na resolucao 8 (derivado de lat/lon)';

-- RN06 / CA04: o log registra tambem as falhas, e a conferencia de integridade da fonte.
ALTER TABLE ingestion_log
    ADD COLUMN status        text    NOT NULL DEFAULT 'COMPLETED' CHECK (status IN ('COMPLETED', 'FAILED')),
    ADD COLUMN error         text,                  -- motivo da falha (sem credenciais)
    ADD COLUMN expected_rows bigint,                -- linhas declaradas no manifesto da fonte
    ADD COLUMN source_sha256 text,                  -- sha256 do arquivo original, do manifesto da fonte
    ADD COLUMN verified      boolean NOT NULL DEFAULT false;  -- arquivo lido por inteiro e conferido com o manifesto
