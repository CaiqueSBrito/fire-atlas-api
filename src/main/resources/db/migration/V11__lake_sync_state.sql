-- Sincronizacao automatica da gold ao subir a API: ultima versao do Delta importada por inteiro em cada tabela do
-- data lake e ate que dia ela cobre. Sem linha = a tabela ainda nao teve a carga inicial.
CREATE TABLE IF NOT EXISTS lake_sync_state (
    table_path     text        PRIMARY KEY,
    version        bigint      NOT NULL,
    synced_through date,
    synced_at      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN lake_sync_state.table_path IS 'Prefixo da tabela Delta no bucket (ex.: gold/map_grid/)';
COMMENT ON COLUMN lake_sync_state.version IS 'Versao do _delta_log importada sem falhas';
COMMENT ON COLUMN lake_sync_state.synced_through IS 'Ultimo dia do periodo importado (nulo nas tabelas sem periodo)';
