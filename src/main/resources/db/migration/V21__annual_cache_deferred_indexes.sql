-- Carga do zero sem indices nem chaves: as definicoes ficam aqui (com WAL) enquanto o COPY grava, e voltam antes
-- do ready. Uma queda no meio deixa as linhas aqui; a proxima carga continua sem os indices e os recria no fim.
CREATE TABLE annual_cache_deferred_index (
    index_name text PRIMARY KEY,
    table_name text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('index', 'constraint_index', 'constraint')),
    create_ddl text NOT NULL,
    attach_ddl text,
    deferred_at timestamptz NOT NULL DEFAULT now()
);
