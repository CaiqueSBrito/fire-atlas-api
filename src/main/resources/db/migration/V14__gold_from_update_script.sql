-- A camada ouro volta a ser servida pelo PostgreSQL: scripts/atualizar.sh baixa as tabelas Delta do data lake
-- (perfil ingest-gold) e as rotas /api/v2 leem somente estas tabelas. Nenhuma consulta vai ao Athena.
-- Mesmo esquema de V7 a V12, que V13 havia removido.

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

COMMENT ON TABLE daily_h3 IS 'Grade H3 diaria por sensor (gold/map_grid), carregada por scripts/atualizar.sh.';
COMMENT ON COLUMN daily_h3.source_key IS 'Particao da gold de onde veio a linha (proveniencia): tabela/resolution=R/year=A@vVERSAO';

CREATE TABLE IF NOT EXISTS h3_place (
    res               smallint         NOT NULL,
    cell              bigint           NOT NULL,
    latitude          double precision,
    longitude         double precision,
    continent         text,
    country_code      text,
    country_iso_a2    text,
    country_name      text,
    country_name_pt   text,
    country_share     double precision,
    country_method    text,
    detections        bigint,
    city_geonameid    bigint,
    city_name         text,
    city_country_code text,
    city_distance_km  double precision,
    source_key        text,
    PRIMARY KEY (res, cell)
);
CREATE INDEX IF NOT EXISTS idx_h3_place_country   ON h3_place (res, country_iso_a2);
CREATE INDEX IF NOT EXISTS idx_h3_place_continent ON h3_place (res, continent);
CREATE INDEX IF NOT EXISTS idx_h3_place_city      ON h3_place (res, city_geonameid) WHERE city_geonameid IS NOT NULL;

COMMENT ON TABLE h3_place IS 'Lugar de cada celula H3 (gold/h3_places): continente, pais predominante e cidade mais proxima.';

CREATE TABLE IF NOT EXISTS forecast (
    issue_date                date   NOT NULL,
    h3_r6                     bigint NOT NULL,
    h3_r1                     bigint,
    det_24h                   double precision,
    det_3d                    double precision,
    det_7d                    double precision,
    frp_mean_7d               double precision,
    modis_brightness_mean_7d  double precision,
    viirs_brightness_mean_7d  double precision,
    ring1_det_24h             double precision,
    ring1_det_7d              double precision,
    ring2_det_7d              double precision,
    active                    boolean,
    prob_fire_7d              real,
    source_key                text,
    PRIMARY KEY (issue_date, h3_r6)
);
CREATE INDEX IF NOT EXISTS idx_forecast_prob ON forecast (issue_date, prob_fire_7d DESC);
CREATE INDEX IF NOT EXISTS idx_forecast_r1   ON forecast (issue_date, h3_r1);

COMMENT ON TABLE forecast IS 'Previsao de fogo da gold (gold/forecast), gerada pelo pipeline de dados; a API nao recalcula.';

CREATE TABLE IF NOT EXISTS forecast_run (
    issue_date     date        PRIMARY KEY,
    source_key     text        NOT NULL,
    model_sha256   text,
    method_version text,
    silver_version text,
    imported_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS harmonized_cell (
    res               smallint         NOT NULL,
    cell              bigint           NOT NULL,
    day               date             NOT NULL,
    harmonized        double precision NOT NULL,
    contributing      smallint         NOT NULL,
    units_terra       integer,
    detections_terra  integer,
    aqua_eq_terra     double precision,
    units_aqua        integer,
    detections_aqua   integer,
    aqua_eq_aqua      double precision,
    units_snpp        integer,
    detections_snpp   integer,
    aqua_eq_snpp      double precision,
    units_noaa20      integer,
    detections_noaa20 integer,
    aqua_eq_noaa20    double precision,
    units_noaa21      integer,
    detections_noaa21 integer,
    aqua_eq_noaa21    double precision,
    source_key        text,
    t_lt320           double precision,
    t320              double precision,
    t330              double precision,
    t340              double precision,
    t350              double precision,
    t_ge360           double precision,
    t_none            double precision,
    basis             text,
    PRIMARY KEY (res, cell, day)
);
CREATE INDEX IF NOT EXISTS idx_harmonized_cell_day ON harmonized_cell (res, day);

COMMENT ON TABLE harmonized_cell IS 'Atividade de fogo harmonizada por celula H3 e dia local (gold/harmonized/cells). Unidade: celulas ativas equivalentes ao MODIS Aqua.';
COMMENT ON COLUMN harmonized_cell.contributing IS 'Quantos satelites contribuiram no dia (0 = SEM DADO, nao zero fogo)';

CREATE TABLE IF NOT EXISTS harmonized_availability (
    day           date             NOT NULL,
    platform      text             NOT NULL,
    units         integer,
    typical_units double precision,
    available     boolean,
    contributes   boolean,
    source_key    text,
    PRIMARY KEY (day, platform)
);

COMMENT ON TABLE harmonized_availability IS 'Disponibilidade dos satelites por dia (gold/harmonized/availability).';
