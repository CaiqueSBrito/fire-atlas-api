-- Catalogo de sensores: deixa de ser so texto em detection e passa a ter entidade propria
-- (resolucao nominal deixa de ficar fixa no codigo).
CREATE TABLE sensor (
    code                 text    PRIMARY KEY,                       -- valor de detection.sensor
    name                 text    NOT NULL,
    nominal_resolution_m integer NOT NULL CHECK (nominal_resolution_m > 0),
    description          text    NOT NULL
);

INSERT INTO sensor (code, name, nominal_resolution_m, description) VALUES
    ('MODIS', 'Moderate Resolution Imaging Spectroradiometer', 1000,
     'Pixel nominal de ~1 km no nadir; produto FIRMS MODIS Collection 6.1 (Terra e Aqua).'),
    ('VIIRS', 'Visible Infrared Imaging Radiometer Suite', 375,
     'Banda de imagem de 375 m; produto FIRMS VIIRS (S-NPP, NOAA-20 e NOAA-21). Detecta focos menores que o MODIS.');

-- Plataformas (satelites) que carregam cada sensor; code e o valor da coluna satellite dos CSVs da FIRMS.
CREATE TABLE satellite (
    code           text PRIMARY KEY,
    sensor         text NOT NULL REFERENCES sensor (code),
    platform_name  text NOT NULL,
    source_product text NOT NULL                                     -- produto do data lake (detection.source_product)
);

INSERT INTO satellite (code, sensor, platform_name, source_product) VALUES
    ('Terra', 'MODIS', 'Terra (EOS AM-1)',     'MODIS_C6.1'),
    ('Aqua',  'MODIS', 'Aqua (EOS PM-1)',      'MODIS_C6.1'),
    ('SNPP',  'VIIRS', 'Suomi NPP',            'VIIRS_S-NPP'),
    ('N20',   'VIIRS', 'NOAA-20 (JPSS-1)',     'VIIRS_NOAA-20'),
    ('N21',   'VIIRS', 'NOAA-21 (JPSS-2)',     'VIIRS_NOAA-21');

-- Toda deteccao precisa apontar para um sensor do catalogo.
ALTER TABLE detection ADD CONSTRAINT detection_sensor_fk FOREIGN KEY (sensor) REFERENCES sensor (code);
