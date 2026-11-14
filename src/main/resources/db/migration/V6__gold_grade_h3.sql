-- A API passa a usar SOMENTE a camada ouro (gold/map_grid, Delta Lake): uma linha por celula H3, dia e sensor,
-- ja agregada. Nao ha mais focos individuais no banco; a tabela de deteccoes (bronze) deixa de existir.
-- daily_h3 e a tabela de agregados derivada; os dados antigos vinham das deteccoes e precisam ser reimportados
-- da gold (scripts/importar.sh).

DELETE FROM daily_h3;

DROP TABLE detection CASCADE;
DROP FUNCTION IF EXISTS detection_block_update();

ALTER TABLE daily_h3 ALTER COLUMN n TYPE bigint;
ALTER TABLE daily_h3 ALTER COLUMN frp_sum DROP NOT NULL;

ALTER TABLE daily_h3
    ADD COLUMN frp_mean         double precision,
    ADD COLUMN platforms        integer,
    ADD COLUMN brightness_max   double precision,
    ADD COLUMN viirs_conf_low     bigint,
    ADD COLUMN viirs_conf_nominal bigint,
    ADD COLUMN viirs_conf_high    bigint,
    ADD COLUMN modis_conf_mean  double precision,
    ADD COLUMN source_key       text;

COMMENT ON TABLE daily_h3 IS 'Grade H3 diaria por sensor (gold/map_grid do data lake). Fonte unica dos agregados da API.';
COMMENT ON COLUMN daily_h3.res IS 'Resolucao H3 da celula (0 a 6 na gold)';
COMMENT ON COLUMN daily_h3.cell IS 'Indice H3 da celula, como veio da fonte (a API nao o calcula)';
COMMENT ON COLUMN daily_h3.n IS 'Numero de deteccoes (focos de calor) do sensor na celula e dia';
COMMENT ON COLUMN daily_h3.frp_sum IS 'Soma do FRP em MW';
COMMENT ON COLUMN daily_h3.frp_mean IS 'Media do FRP em MW (valor original da fonte)';
COMMENT ON COLUMN daily_h3.frp_max IS 'Maior FRP em MW';
COMMENT ON COLUMN daily_h3.platforms IS 'Numero de plataformas (satelites) que contribuiram';
COMMENT ON COLUMN daily_h3.brightness_max IS 'Maior temperatura de brilho em kelvin';
COMMENT ON COLUMN daily_h3.viirs_conf_low IS 'Deteccoes VIIRS de confianca baixa (l)';
COMMENT ON COLUMN daily_h3.viirs_conf_nominal IS 'Deteccoes VIIRS de confianca nominal (n)';
COMMENT ON COLUMN daily_h3.viirs_conf_high IS 'Deteccoes VIIRS de confianca alta (h)';
COMMENT ON COLUMN daily_h3.modis_conf_mean IS 'Media da confianca MODIS (0 a 100)';
COMMENT ON COLUMN daily_h3.source_key IS 'Particao da gold de onde veio a linha (proveniencia): tabela/resolution=R/year=A@vVERSAO';

COMMENT ON TABLE harmonization_run IS 'Sem uso: a harmonizacao MODIS x VIIRS depende de focos individuais, que a gold nao tem.';
