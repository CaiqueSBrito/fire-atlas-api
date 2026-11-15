-- Cada tentativa tem destino vazio exclusivo. Sucessos imutaveis sao reutilizados por snapshot/projecao.
CREATE TABLE annual_athena_export (
    snapshot_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('GRID', 'PLACES')),
    resolution smallint NOT NULL CHECK (resolution BETWEEN 0 AND 6),
    client_token text NOT NULL UNIQUE,
    output_prefix text NOT NULL,
    query_id text,
    state text NOT NULL CHECK (state IN ('pending', 'running', 'complete', 'failed')),
    manifest_location text,
    files jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_id, kind, resolution),
    CHECK (state <> 'complete' OR (manifest_location IS NOT NULL AND files IS NOT NULL))
);

-- Corte limpo: marcadores de Parquet original nao representam os arquivos projetados pelo Athena.
DELETE FROM annual_map_grid_stage;
DELETE FROM annual_h3_places_stage;
DELETE FROM annual_grid_partition;
ALTER TABLE annual_map_grid DROP COLUMN platforms, DROP COLUMN frp_mean_mw, DROP COLUMN brightness_max_k;
ALTER TABLE annual_map_grid_stage DROP COLUMN platforms, DROP COLUMN frp_mean_mw, DROP COLUMN brightness_max_k;
UPDATE annual_grid_bootstrap SET state='idle', checked_at=NULL, error=NULL,
    message='Preparando exportacao colunar no Athena';
