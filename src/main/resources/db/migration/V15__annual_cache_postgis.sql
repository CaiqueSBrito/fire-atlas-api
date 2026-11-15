-- Coordenadas publicadas na gold: o backend nao recalcula H3 nem geometrias de fronteiras.
CREATE EXTENSION IF NOT EXISTS postgis;

ALTER TABLE annual_h3_places
    ADD COLUMN latitude double precision CHECK (latitude BETWEEN -90 AND 90),
    ADD COLUMN longitude double precision CHECK (longitude BETWEEN -180 AND 180),
    ADD COLUMN geom geometry(Point, 4326) GENERATED ALWAYS AS
        (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED;
CREATE INDEX annual_h3_places_geom_gist ON annual_h3_places USING gist (geom);

ALTER TABLE annual_h3_places_stage
    ADD COLUMN latitude double precision CHECK (latitude BETWEEN -90 AND 90),
    ADD COLUMN longitude double precision CHECK (longitude BETWEEN -180 AND 180);

-- Uma versao anterior do cache nao preservava latitude/longitude; requer republicar apenas lugares.
UPDATE annual_grid_partition SET completed=false, rows_loaded=0, completed_at=NULL WHERE kind='PLACES';
UPDATE annual_grid_bootstrap SET state='idle', checked_at=NULL, message='Preparando cache geoespacial PostGIS', error=NULL;
