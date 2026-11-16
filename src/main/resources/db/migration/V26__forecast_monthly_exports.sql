-- A previsao tambem usa UNLOAD mensal; lugares continuam sem periodo.
ALTER TABLE annual_athena_export DROP CONSTRAINT annual_athena_export_check1;
ALTER TABLE annual_athena_export ADD CONSTRAINT annual_athena_export_month_check
    CHECK (CASE kind WHEN 'PLACES' THEN month = 0 ELSE month BETWEEN 1 AND 12 END);
ALTER TABLE annual_athena_export ADD CONSTRAINT annual_athena_export_forecast_shape_check
    CHECK (kind <> 'FORECAST' OR (resolution = 6 AND part = 0));
