-- NASA-018: identificador individual por deteccao em tabela particionada por acq_date,
-- indice geografico geodesico (antimeridiano e altas latitudes) e indice para ordenacao deterministica.

CREATE SEQUENCE IF NOT EXISTS detection_id_seq;

ALTER TABLE detection
    ADD COLUMN id bigint NOT NULL DEFAULT nextval('detection_id_seq');

ALTER SEQUENCE detection_id_seq OWNED BY detection.id;

ALTER TABLE detection
    ADD CONSTRAINT pk_detection PRIMARY KEY (acq_date, id);

CREATE INDEX idx_detection_geog ON detection USING gist ((geom::geography));

CREATE INDEX idx_detection_sensor_ts ON detection (sensor, acq_ts, id);
