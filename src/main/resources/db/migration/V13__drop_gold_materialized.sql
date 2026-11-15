-- A camada ouro deixou de ser materializada: a API consulta as tabelas Delta direto no S3 pelo Amazon Athena
-- (catalogo Glue fireatlas_gold), entao as copias no PostGIS nao existem mais e nao ha o que sincronizar.
--
-- Continuam no banco: detection (alimentada pela prata, que serve /api/v1), sensor e satellite (catalogo estatico
-- semeado em V7, que nao vem do data lake), lake_import_log (proveniencia das importacoes da prata) e
-- user_preference.

DROP TABLE IF EXISTS harmonized_availability CASCADE;
DROP TABLE IF EXISTS harmonized_cell CASCADE;
DROP TABLE IF EXISTS forecast_run CASCADE;
DROP TABLE IF EXISTS forecast CASCADE;
DROP TABLE IF EXISTS h3_place CASCADE;
DROP TABLE IF EXISTS daily_h3 CASCADE;
DROP TABLE IF EXISTS lake_sync_state CASCADE;
