-- Cada ano conserva seu plano e publicacao; lugares compartilhados pertencem somente a 2026.
-- Nao reescreve nem descarta os arquivos e linhas ja confirmados no cache atual.
ALTER TABLE annual_grid_bootstrap
    DROP CONSTRAINT annual_grid_bootstrap_year_check,
    ADD CONSTRAINT annual_grid_bootstrap_year_check CHECK (year BETWEEN 1 AND 2026),
    ADD COLUMN loader_pid integer,
    ADD COLUMN planned_units bigint NOT NULL DEFAULT 0 CHECK (planned_units >= 0);

ALTER TABLE annual_grid_partition
    ADD COLUMN year integer NOT NULL DEFAULT 2026 REFERENCES annual_grid_bootstrap (year),
    DROP CONSTRAINT annual_grid_partition_pkey,
    ADD PRIMARY KEY (year, source_key),
    ADD CONSTRAINT annual_grid_partition_places_year_check CHECK (kind <> 'PLACES' OR year = 2026);
ALTER TABLE annual_grid_partition ALTER COLUMN year DROP DEFAULT;

ALTER TABLE annual_map_grid
    DROP CONSTRAINT annual_map_grid_year_check,
    DROP CONSTRAINT annual_map_grid_day_check,
    ADD CONSTRAINT annual_map_grid_year_check CHECK (year BETWEEN 1 AND 2026),
    ADD CONSTRAINT annual_map_grid_day_check CHECK (EXTRACT(YEAR FROM day) = year);

-- Um estado ready anterior so continua publicado se todos os marcadores sobreviveram.
UPDATE annual_grid_bootstrap b SET planned_units = (
    SELECT count(*) FROM annual_grid_partition p WHERE p.year = b.year
) WHERE b.state = 'ready';
