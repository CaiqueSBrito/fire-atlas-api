-- Cache compartilhado da grade anual. Os arquivos ativos do Delta sao unidades duraveis de recuperacao.
CREATE TABLE annual_grid_bootstrap (
    year integer PRIMARY KEY CHECK (year = 2026),
    state text NOT NULL DEFAULT 'idle' CHECK (state IN ('idle', 'loading', 'ready', 'failed')),
    message text NOT NULL DEFAULT 'Aguardando carga mundial de 2026',
    error text,
    checked_at timestamptz,
    grid_version bigint,
    places_version bigint
);
INSERT INTO annual_grid_bootstrap (year) VALUES (2026);

CREATE TABLE annual_grid_partition (
    source_key text PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('GRID', 'PLACES')),
    resolution smallint NOT NULL CHECK (resolution BETWEEN 0 AND 6),
    file_size bigint NOT NULL,
    expected_rows bigint,
    empty_partition boolean NOT NULL DEFAULT false,
    completed boolean NOT NULL DEFAULT false,
    rows_loaded bigint NOT NULL DEFAULT 0,
    completed_at timestamptz
);

CREATE TABLE annual_map_grid (
    resolution smallint NOT NULL CHECK (resolution BETWEEN 0 AND 6),
    year integer NOT NULL CHECK (year = 2026),
    day date NOT NULL CHECK (day >= DATE '2026-01-01' AND day < DATE '2027-01-01'),
    sensor text NOT NULL CHECK (sensor IN ('MODIS', 'VIIRS')),
    h3_cell bigint NOT NULL,
    detections bigint NOT NULL CHECK (detections >= 0),
    platforms integer,
    frp_sum_mw double precision,
    frp_mean_mw double precision,
    frp_max_mw double precision,
    brightness_max_k double precision,
    viirs_confidence_low bigint,
    viirs_confidence_nominal bigint,
    viirs_confidence_high bigint,
    modis_confidence_mean double precision,
    source_key text NOT NULL REFERENCES annual_grid_partition (source_key) ON DELETE CASCADE,
    PRIMARY KEY (resolution, day, sensor, h3_cell)
);
CREATE INDEX annual_map_grid_cell_date ON annual_map_grid (resolution, h3_cell, day);
CREATE INDEX annual_map_grid_source ON annual_map_grid (source_key);

CREATE TABLE annual_h3_places (
    resolution smallint NOT NULL CHECK (resolution BETWEEN 0 AND 6),
    h3_cell bigint NOT NULL,
    continent text,
    country_code text,
    country_iso_a2 text,
    country_name text,
    country_name_pt text,
    city_geonameid bigint,
    city_name text,
    city_country_code text,
    city_distance_km double precision,
    source_key text NOT NULL REFERENCES annual_grid_partition (source_key) ON DELETE CASCADE,
    PRIMARY KEY (resolution, h3_cell)
);
CREATE INDEX annual_h3_places_country ON annual_h3_places (resolution, country_iso_a2);
CREATE INDEX annual_h3_places_iso3 ON annual_h3_places (resolution, country_code);
CREATE INDEX annual_h3_places_continent ON annual_h3_places (resolution, continent);
CREATE INDEX annual_h3_places_city ON annual_h3_places (resolution, city_geonameid, city_distance_km);
CREATE INDEX annual_h3_places_source ON annual_h3_places (source_key);

-- Staging duravel: lotes nao sao visiveis na grade ate o commit que grava tambem o marcador.
CREATE TABLE annual_map_grid_stage (LIKE annual_map_grid INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE INDEX annual_map_grid_stage_source ON annual_map_grid_stage (source_key);
CREATE TABLE annual_h3_places_stage (LIKE annual_h3_places INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE INDEX annual_h3_places_stage_source ON annual_h3_places_stage (source_key);
