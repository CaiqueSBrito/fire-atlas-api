-- Lugares (continente, pais, cidade mais proxima) por celula H3 e previsao de fogo, ambos da camada ouro
-- (gold/h3_places e gold/forecast). Gerados pelo time de dados; a API so importa e consulta.

-- Uma linha por celula H3 e resolucao (0 a 6). A cidade so existe nas resolucoes finas (5 e 6).
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
COMMENT ON COLUMN h3_place.country_share IS 'Fracao da celula que pertence ao pais (0 a 1); a celula e atribuida ao pais predominante';
COMMENT ON COLUMN h3_place.country_method IS 'Regra usada pelo pipeline para escolher o pais da celula (ex.: predominant)';
COMMENT ON COLUMN h3_place.city_geonameid IS 'Cidade mais proxima do centro da celula (GeoNames cities1000), nao "dentro" da cidade';
COMMENT ON COLUMN h3_place.city_distance_km IS 'Distancia em km do centro da celula ate a cidade mais proxima';

-- Previsao de fogo por celula H3 de resolucao 6 e data de emissao (gold/forecast).
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
COMMENT ON COLUMN forecast.prob_fire_7d IS 'Probabilidade (0 a 1) de deteccao de fogo na celula nos proximos 7 dias, segundo o modelo do pipeline';
COMMENT ON COLUMN forecast.active IS 'Celula com fogo recente (marcada pelo pipeline)';
COMMENT ON COLUMN forecast.source_key IS 'Arquivo da gold de onde veio a linha: tabela/particao@vVERSAO';
