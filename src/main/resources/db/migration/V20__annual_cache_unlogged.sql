-- O cache anual e reconstruivel a partir do S3: sem WAL, a carga deixa de disputar o log de escrita
-- (38,6% da espera das sessoes COPY na medicao com 12 trabalhadores).
-- Contrapartida: numa queda do Postgres (nao num desligamento limpo), tabelas UNLOGGED voltam vazias.
-- Marcadores e dados ficam juntos sem WAL: depois de uma queda nao sobra "arquivo concluido" sem linhas;
-- o status passa a "Cache anual incompleto" e o proximo POST recarrega.
-- Corte limpo: SET UNLOGGED reescreveria o cache inteiro; ele e republicado pela proxima carga.
TRUNCATE annual_map_grid, annual_h3_places, annual_grid_partition;

-- Tabela com WAL nao pode referenciar tabela sem WAL: primeiro as que referenciam, depois os marcadores.
ALTER TABLE annual_map_grid SET UNLOGGED;
ALTER TABLE annual_h3_places SET UNLOGGED;
ALTER TABLE annual_grid_partition SET UNLOGGED;

UPDATE annual_grid_bootstrap SET state='idle', checked_at=NULL, error=NULL, grid_version=NULL, places_version=NULL,
    message='Preparando cache sem WAL';
