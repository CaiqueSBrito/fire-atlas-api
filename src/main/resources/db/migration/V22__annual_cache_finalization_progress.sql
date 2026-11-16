-- Arquivos importados nao significam cache pronto: indices, chaves e ANALYZE tambem sao trabalho real.
ALTER TABLE annual_grid_bootstrap
    ADD COLUMN phase text NOT NULL DEFAULT 'preparing'
        CHECK (phase IN ('preparing', 'importing', 'indexes', 'validating', 'analyzing', 'ready')),
    ADD COLUMN finalization_completed bigint NOT NULL DEFAULT 0 CHECK (finalization_completed >= 0),
    ADD COLUMN finalization_total bigint NOT NULL DEFAULT 0 CHECK (finalization_total >= finalization_completed);

UPDATE annual_grid_bootstrap SET phase='ready' WHERE state='ready';
