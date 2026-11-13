CREATE EXTENSION IF NOT EXISTS postgis;

-- Deteccoes de focos de calor (MODIS e VIIRS) em formato unico, preservando a origem.
CREATE TABLE detection (
    sensor           text             NOT NULL CHECK (sensor IN ('MODIS', 'VIIRS')),
    satellite        text             NOT NULL,
    acq_date         date             NOT NULL,
    acq_ts           timestamptz      NOT NULL,          -- acq_date + acq_time (UTC)
    lat              double precision NOT NULL,
    lon              double precision NOT NULL,
    geom             geometry(Point, 4326) GENERATED ALWAYS AS
                         (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED,
    brightness       real,
    bright_t31       real,
    scan             real,
    track            real,
    frp              real,                               -- MW
    confidence_raw   text,                               -- valor original (0-100 no MODIS; l/n/h no VIIRS)
    confidence_class char(1),                            -- harmonizado: l / n / h
    daynight         char(1),
    version          text,
    source_product   text             NOT NULL,          -- ex.: MODIS_C6.1, VIIRS_S-NPP
    source_file      text             NOT NULL           -- chave do objeto no data lake (proveniencia)
) PARTITION BY RANGE (acq_date);

-- Particoes anuais (2000..2030) + default.
DO $$
DECLARE y int;
BEGIN
    FOR y IN 2000..2030 LOOP
        EXECUTE format(
            'CREATE TABLE detection_%s PARTITION OF detection FOR VALUES FROM (%L) TO (%L)',
            y, make_date(y, 1, 1), make_date(y + 1, 1, 1));
    END LOOP;
END $$;
CREATE TABLE detection_default PARTITION OF detection DEFAULT;

CREATE INDEX idx_detection_geom   ON detection USING gist (geom);
CREATE INDEX idx_detection_date   ON detection USING brin (acq_date);
CREATE INDEX idx_detection_sensor ON detection (sensor, acq_date);

-- Grade diaria pre-agregada (celulas de 0,1 grau) para calendario e zoom afastado.
CREATE TABLE daily_grid (
    acq_date date    NOT NULL,
    sensor   text    NOT NULL,
    cx       integer NOT NULL,   -- floor((lon + 180) * 10), 0..3599
    cy       integer NOT NULL,   -- floor((lat + 90)  * 10), 0..1799
    n        integer NOT NULL,
    frp_sum  double precision NOT NULL,
    frp_max  real,
    PRIMARY KEY (acq_date, sensor, cx, cy)
);
CREATE INDEX idx_daily_grid_cell ON daily_grid (sensor, cx, cy, acq_date);

-- Rastreabilidade das cargas.
CREATE TABLE ingestion_log (
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
    loaded_at     timestamptz NOT NULL DEFAULT now()
);

-- Perfil anonimo (cookie): regiao favorita sem login.
CREATE TABLE user_preference (
    anon_id    uuid PRIMARY KEY,
    prefs      jsonb       NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
