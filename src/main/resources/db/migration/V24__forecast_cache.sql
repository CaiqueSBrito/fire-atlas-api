-- A previsao conserva celulas nativas r6 sem ampliar a grade historica nem lugares compartilhados.
ALTER TABLE annual_athena_export DROP CONSTRAINT annual_athena_export_kind_check;
ALTER TABLE annual_athena_export ADD CONSTRAINT annual_athena_export_kind_check
    CHECK (kind IN ('GRID', 'PLACES', 'FORECAST'));

CREATE TABLE forecast_cache_bootstrap (
    id boolean PRIMARY KEY DEFAULT true CHECK (id),
    state text NOT NULL DEFAULT 'idle' CHECK (state IN ('idle', 'loading', 'ready', 'failed')),
    message text NOT NULL DEFAULT 'Aguardando carga mundial da previsao',
    error text,
    phase text NOT NULL DEFAULT 'preparing',
    checked_at timestamptz,
    loader_pid integer,
    staging_snapshot text,
    active_snapshot text,
    forecast_version bigint,
    places_version bigint,
    planned_units bigint NOT NULL DEFAULT 0 CHECK (planned_units >= 0),
    published_units bigint NOT NULL DEFAULT 0 CHECK (published_units >= 0),
    finalization_completed bigint NOT NULL DEFAULT 0 CHECK (finalization_completed >= 0),
    finalization_total bigint NOT NULL DEFAULT 0 CHECK (finalization_total >= 0)
);
INSERT INTO forecast_cache_bootstrap (id) VALUES (true);

CREATE TABLE forecast_cache_partition (
    id bigserial PRIMARY KEY,
    snapshot_id text NOT NULL,
    source_key text NOT NULL,
    kind text NOT NULL CHECK (kind = 'FORECAST'),
    resolution smallint NOT NULL CHECK (resolution = 6),
    file_size bigint NOT NULL CHECK (file_size >= 0),
    expected_rows bigint CHECK (expected_rows >= 0),
    empty_partition boolean NOT NULL DEFAULT false,
    completed boolean NOT NULL DEFAULT false,
    rows_loaded bigint NOT NULL DEFAULT 0 CHECK (rows_loaded >= 0),
    completed_at timestamptz,
    UNIQUE (snapshot_id, source_key),
    UNIQUE (id, snapshot_id)
);

-- Linhas confirmadas de uma atualizacao ficam isoladas ate a troca atomica de active_snapshot.
CREATE TABLE forecast_cache (
    snapshot_id text NOT NULL,
    issue_date date NOT NULL,
    h3_r6 bigint NOT NULL CHECK (((h3_r6 >> 52) & 15) = 6),
    h3_r1 bigint,
    prob_fire_7d double precision,
    det_24h double precision,
    det_3d double precision,
    det_7d double precision,
    frp_mean_7d double precision,
    modis_brightness_mean_7d double precision,
    viirs_brightness_mean_7d double precision,
    ring1_det_24h double precision,
    ring1_det_7d double precision,
    ring2_det_7d double precision,
    active boolean,
    country_name_pt text,
    city_name text,
    city_distance_km double precision,
    latitude double precision CHECK (latitude BETWEEN -90 AND 90),
    longitude double precision CHECK (longitude BETWEEN -180 AND 180),
    continent text,
    country_code text,
    country_iso_a2 text,
    city_geonameid bigint,
    partition_id bigint NOT NULL,
    geom geometry(Point, 4326) GENERATED ALWAYS AS
        (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED,
    PRIMARY KEY (snapshot_id, issue_date, h3_r6),
    FOREIGN KEY (partition_id, snapshot_id) REFERENCES forecast_cache_partition (id, snapshot_id) ON DELETE CASCADE
);
CREATE INDEX forecast_cache_partition_rows ON forecast_cache (partition_id);
CREATE INDEX forecast_cache_probability ON forecast_cache (snapshot_id, issue_date, prob_fire_7d DESC, h3_r6);
CREATE INDEX forecast_cache_country ON forecast_cache (snapshot_id, country_iso_a2);
CREATE INDEX forecast_cache_iso3 ON forecast_cache (snapshot_id, country_code);
CREATE INDEX forecast_cache_continent ON forecast_cache (snapshot_id, continent);
CREATE INDEX forecast_cache_city ON forecast_cache (snapshot_id, city_geonameid, city_distance_km);
CREATE INDEX forecast_cache_geom_gist ON forecast_cache USING gist (geom);
