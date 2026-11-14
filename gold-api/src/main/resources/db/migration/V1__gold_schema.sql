-- Esquema do app gold-api: le SOMENTE a camada ouro do data lake (gold/map_grid, Delta Lake) e serve os agregados.
-- Roda no esquema proprio "gold" (historico do Flyway separado do app principal): nao divide tabelas, versoes de
-- migracao nem classes com o app da main.

-- Catalogo de sensores e satelites (a resolucao nominal vem daqui).
CREATE TABLE IF NOT EXISTS sensor (
    code                 text PRIMARY KEY,
    name                 text    NOT NULL,
    nominal_resolution_m integer NOT NULL CHECK (nominal_resolution_m > 0),
    description          text    NOT NULL
);

CREATE TABLE IF NOT EXISTS satellite (
    code           text PRIMARY KEY,
    sensor         text NOT NULL REFERENCES sensor (code),
    platform_name  text NOT NULL,
    source_product text NOT NULL
);

INSERT INTO sensor (code, name, nominal_resolution_m, description) VALUES
    ('MODIS', 'Moderate Resolution Imaging Spectroradiometer', 1000,
     'Pixel nominal de ~1 km no nadir; produto FIRMS MODIS Collection 6.1 (Terra e Aqua).'),
    ('VIIRS', 'Visible Infrared Imaging Radiometer Suite', 375,
     'Banda de imagem de 375 m; produto FIRMS VIIRS (S-NPP, NOAA-20 e NOAA-21). Detecta focos menores que o MODIS.')
ON CONFLICT (code) DO NOTHING;

INSERT INTO satellite (code, sensor, platform_name, source_product) VALUES
    ('Terra', 'MODIS', 'Terra (EOS AM-1)', 'MODIS_C6.1'),
    ('Aqua',  'MODIS', 'Aqua (EOS PM-1)',  'MODIS_C6.1'),
    ('SNPP',  'VIIRS', 'Suomi NPP',        'VIIRS_S-NPP'),
    ('N20',   'VIIRS', 'NOAA-20 (JPSS-1)', 'VIIRS_NOAA-20'),
    ('N21',   'VIIRS', 'NOAA-21 (JPSS-2)', 'VIIRS_NOAA-21')
ON CONFLICT (code) DO NOTHING;

-- Grade H3 diaria por sensor (uma linha por celula, dia e sensor), como vem da gold.
CREATE TABLE IF NOT EXISTS daily_h3 (
    acq_date           date             NOT NULL,
    sensor             text             NOT NULL,
    res                smallint         NOT NULL,
    cell               bigint           NOT NULL,
    n                  bigint           NOT NULL,
    frp_sum            double precision,
    frp_max            real,
    frp_mean           double precision,
    platforms          integer,
    brightness_max     double precision,
    viirs_conf_low     bigint,
    viirs_conf_nominal bigint,
    viirs_conf_high    bigint,
    modis_conf_mean    double precision,
    source_key         text,
    PRIMARY KEY (res, cell, acq_date, sensor)
);
CREATE INDEX IF NOT EXISTS idx_daily_h3_date ON daily_h3 (res, acq_date);

COMMENT ON TABLE daily_h3 IS 'Grade H3 diaria por sensor (gold/map_grid do data lake). Fonte unica dos agregados da API.';
COMMENT ON COLUMN daily_h3.res IS 'Resolucao H3 da celula (0 a 6 na gold)';
COMMENT ON COLUMN daily_h3.cell IS 'Indice H3 da celula, como veio da fonte (a API nao o calcula)';
COMMENT ON COLUMN daily_h3.n IS 'Numero de deteccoes (focos de calor) do sensor na celula e dia';
COMMENT ON COLUMN daily_h3.frp_sum IS 'Soma do FRP em MW';
COMMENT ON COLUMN daily_h3.frp_max IS 'Maior FRP em MW';
COMMENT ON COLUMN daily_h3.frp_mean IS 'Media do FRP em MW (valor original da fonte)';
COMMENT ON COLUMN daily_h3.platforms IS 'Numero de plataformas (satelites) que contribuiram';
COMMENT ON COLUMN daily_h3.brightness_max IS 'Maior temperatura de brilho em kelvin';
COMMENT ON COLUMN daily_h3.viirs_conf_low IS 'Deteccoes VIIRS de confianca baixa (l)';
COMMENT ON COLUMN daily_h3.viirs_conf_nominal IS 'Deteccoes VIIRS de confianca nominal (n)';
COMMENT ON COLUMN daily_h3.viirs_conf_high IS 'Deteccoes VIIRS de confianca alta (h)';
COMMENT ON COLUMN daily_h3.modis_conf_mean IS 'Media da confianca MODIS (0 a 100)';
COMMENT ON COLUMN daily_h3.source_key IS 'Particao da gold de onde veio a linha (proveniencia): tabela/resolution=R/year=A@vVERSAO';

-- Metadados e falhas de cada importacao da gold.
CREATE TABLE IF NOT EXISTS ingestion_log (
    id            bigserial PRIMARY KEY,
    source_key    text        NOT NULL,
    product       text        NOT NULL,
    sensor        text        NOT NULL,
    bbox          text        NOT NULL,
    period_start  date        NOT NULL,
    period_end    date        NOT NULL,
    rows_scanned  bigint      NOT NULL,
    rows_loaded   bigint      NOT NULL,
    min_date      date,
    max_date      date,
    loaded_at     timestamptz NOT NULL DEFAULT now(),
    rows_rejected bigint      NOT NULL DEFAULT 0,
    rejects       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status        text        NOT NULL DEFAULT 'COMPLETED' CHECK (status IN ('COMPLETED', 'FAILED')),
    error         text,
    expected_rows bigint,
    source_sha256 text,
    verified      boolean     NOT NULL DEFAULT false
);
