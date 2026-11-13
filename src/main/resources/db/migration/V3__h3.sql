-- H3: cada deteccao recebe o indice H3 na resolucao 8 (aresta ~460 m, compativel com o pixel VIIRS de 375 m).
-- Os ancestrais (resolucoes 3 a 7) sao derivados do indice por operacoes de bits (ver H3Index.parentSql).
ALTER TABLE detection ADD COLUMN h3 bigint;
CREATE INDEX idx_detection_h3 ON detection (h3);

-- RN05: continua proibido alterar medicoes originais; apenas o indice derivado h3 pode ser preenchido.
CREATE OR REPLACE FUNCTION detection_block_update() RETURNS trigger AS $$
BEGIN
    IF ROW(NEW.sensor, NEW.satellite, NEW.acq_date, NEW.acq_ts, NEW.lat, NEW.lon, NEW.brightness,
           NEW.bright_t31, NEW.scan, NEW.track, NEW.frp, NEW.confidence_raw, NEW.confidence_class,
           NEW.daynight, NEW.version, NEW.source_product, NEW.source_file)
       IS DISTINCT FROM
       ROW(OLD.sensor, OLD.satellite, OLD.acq_date, OLD.acq_ts, OLD.lat, OLD.lon, OLD.brightness,
           OLD.bright_t31, OLD.scan, OLD.track, OLD.frp, OLD.confidence_raw, OLD.confidence_class,
           OLD.daynight, OLD.version, OLD.source_product, OLD.source_file)
    THEN
        RAISE EXCEPTION 'detection e imutavel: valores originais nao podem ser alterados (RN05)';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Agregacao diaria por celula H3 em varias resolucoes (substitui a grade lat/lon de 0,1 grau).
CREATE TABLE daily_h3 (
    acq_date date             NOT NULL,
    sensor   text             NOT NULL,
    res      smallint         NOT NULL,
    cell     bigint           NOT NULL,
    n        integer          NOT NULL,
    frp_sum  double precision NOT NULL,
    frp_max  real,
    PRIMARY KEY (res, cell, acq_date, sensor)
);
CREATE INDEX idx_daily_h3_date ON daily_h3 (res, acq_date);

DROP TABLE daily_grid;
