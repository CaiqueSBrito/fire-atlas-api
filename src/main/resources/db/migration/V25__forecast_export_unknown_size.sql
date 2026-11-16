-- UNLOAD publica caminhos, nao tamanhos. -1 e o mesmo sentinela do plano anual;
-- zero continua reservado a particoes explicitamente vazias.
ALTER TABLE forecast_cache_partition DROP CONSTRAINT forecast_cache_partition_file_size_check;
ALTER TABLE forecast_cache_partition ADD CONSTRAINT forecast_cache_partition_file_size_check
    CHECK (file_size >= -1);
