-- Importacao por COPY direto na tabela final, um arquivo por transacao: o staging deixa de existir.
-- Cada linha passa a referenciar o arquivo por id inteiro, nao pelo URI S3 (~260 bytes por linha).
-- Corte limpo: o cache e republicado; exportacoes Athena concluidas continuam reutilizaveis.
DROP TABLE annual_map_grid_stage;
DROP TABLE annual_h3_places_stage;
TRUNCATE annual_map_grid, annual_h3_places, annual_grid_partition;

ALTER TABLE annual_grid_partition ADD COLUMN id integer GENERATED ALWAYS AS IDENTITY UNIQUE;

DROP INDEX annual_map_grid_source;
ALTER TABLE annual_map_grid DROP COLUMN source_key,
    ADD COLUMN partition_id integer NOT NULL REFERENCES annual_grid_partition (id) ON DELETE CASCADE;
CREATE INDEX annual_map_grid_partition ON annual_map_grid (partition_id);

DROP INDEX annual_h3_places_source;
ALTER TABLE annual_h3_places DROP COLUMN source_key,
    ADD COLUMN partition_id integer NOT NULL REFERENCES annual_grid_partition (id) ON DELETE CASCADE;
CREATE INDEX annual_h3_places_partition ON annual_h3_places (partition_id);

UPDATE annual_grid_bootstrap SET state='idle', checked_at=NULL, error=NULL, grid_version=NULL, places_version=NULL,
    message='Preparando importacao direta por COPY';
