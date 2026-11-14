-- Registros de importacao da bronze (CSV bruto) descrevem dados que nao existem mais: a API usa so a gold.
DELETE FROM ingestion_log WHERE product <> 'map_grid';
