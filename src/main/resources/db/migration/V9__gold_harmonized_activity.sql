-- Atividade de fogo harmonizada (gold/harmonized/cells e gold/harmonized/availability), gerada pelo time de dados.
-- Unidade: "celulas ativas equivalentes ao MODIS Aqua" (NAO e numero de focos nem de incendios).

-- Uma linha por celula H3, dia LOCAL de fogo (06:00 a 06:00 no horario solar local, nao UTC) e nivel de zoom.
-- Os niveis 0 a 5 ja trazem a soma das celulas filhas: nao se soma r6 no backend.
CREATE TABLE IF NOT EXISTS harmonized_cell (
    res              smallint         NOT NULL,
    cell             bigint           NOT NULL,
    day              date             NOT NULL,
    harmonized       double precision NOT NULL,
    contributing     smallint         NOT NULL,
    units_terra      integer,
    detections_terra integer,
    aqua_eq_terra    double precision,
    units_aqua       integer,
    detections_aqua  integer,
    aqua_eq_aqua     double precision,
    units_snpp       integer,
    detections_snpp  integer,
    aqua_eq_snpp     double precision,
    units_noaa20      integer,
    detections_noaa20 integer,
    aqua_eq_noaa20    double precision,
    units_noaa21      integer,
    detections_noaa21 integer,
    aqua_eq_noaa21    double precision,
    source_key       text,
    PRIMARY KEY (res, cell, day)
);
CREATE INDEX IF NOT EXISTS idx_harmonized_cell_day ON harmonized_cell (res, day);

COMMENT ON TABLE harmonized_cell IS 'Atividade de fogo harmonizada por celula H3 e dia local (gold/harmonized/cells). Unidade: celulas ativas equivalentes ao MODIS Aqua.';
COMMENT ON COLUMN harmonized_cell.day IS 'Dia local de fogo, das 06:00 as 06:00 no horario solar local (nao e o acquisition_date UTC da prata)';
COMMENT ON COLUMN harmonized_cell.harmonized IS 'O numero do calendario, em celulas ativas equivalentes ao MODIS Aqua';
COMMENT ON COLUMN harmonized_cell.contributing IS 'Quantos satelites contribuiram no dia (0 = SEM DADO, nao zero fogo)';
COMMENT ON COLUMN harmonized_cell.source_key IS 'Particao da gold de onde veio a linha: tabela/resolution=R/year=A@vVERSAO';

-- Disponibilidade dos satelites: um satelite por dia.
CREATE TABLE IF NOT EXISTS harmonized_availability (
    day           date             NOT NULL,
    platform      text             NOT NULL,
    units         integer,
    typical_units double precision,
    available     boolean,
    contributes   boolean,
    source_key    text,
    PRIMARY KEY (day, platform)
);

COMMENT ON TABLE harmonized_availability IS 'Disponibilidade dos satelites por dia (gold/harmonized/availability).';
COMMENT ON COLUMN harmonized_availability.units IS 'Celulas ativas do satelite no dia';
COMMENT ON COLUMN harmonized_availability.typical_units IS 'Valor tipico de celulas ativas do satelite (referencia do pipeline)';
