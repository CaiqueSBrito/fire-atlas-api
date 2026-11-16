-- A grade passa a ser exportada com um UNLOAD por (resolucao, mes); lugares continuam um por resolucao (month = 0).
-- Exportacoes anuais da grade nao correspondem a nenhuma chave mensal: corte limpo, sem reuso.
DELETE FROM annual_athena_export WHERE kind = 'GRID';
ALTER TABLE annual_athena_export ADD COLUMN month smallint NOT NULL DEFAULT 0;
ALTER TABLE annual_athena_export ALTER COLUMN month DROP DEFAULT;
ALTER TABLE annual_athena_export ADD CHECK (CASE kind WHEN 'GRID' THEN month BETWEEN 1 AND 12 ELSE month = 0 END);
ALTER TABLE annual_athena_export DROP CONSTRAINT annual_athena_export_pkey;
ALTER TABLE annual_athena_export ADD PRIMARY KEY (snapshot_id, kind, resolution, month);
-- O proximo POST replaneja: particoes anuais saem do plano e os arquivos mensais sao importados.
UPDATE annual_grid_bootstrap SET state='idle', checked_at=NULL, error=NULL,
    message='Preparando exportacao mensal no Athena';
