-- Atividade harmonizada separada por faixa de temperatura de brilho (K) do pixel mais quente da unidade.
-- Colunas opcionais: arquivos antigos da gold nao as tem e carregam NULL. A soma das 7 faixas = harmonized.
ALTER TABLE harmonized_cell
    ADD COLUMN IF NOT EXISTS t_lt320 double precision,
    ADD COLUMN IF NOT EXISTS t320    double precision,
    ADD COLUMN IF NOT EXISTS t330    double precision,
    ADD COLUMN IF NOT EXISTS t340    double precision,
    ADD COLUMN IF NOT EXISTS t350    double precision,
    ADD COLUMN IF NOT EXISTS t_ge360 double precision,
    ADD COLUMN IF NOT EXISTS t_none  double precision,
    ADD COLUMN IF NOT EXISTS basis   text;

COMMENT ON COLUMN harmonized_cell.t_lt320 IS 'Parte do harmonized com brilho < 320 K';
COMMENT ON COLUMN harmonized_cell.t320 IS 'Parte do harmonized com brilho de 320 a 330 K';
COMMENT ON COLUMN harmonized_cell.t330 IS 'Parte do harmonized com brilho de 330 a 340 K';
COMMENT ON COLUMN harmonized_cell.t340 IS 'Parte do harmonized com brilho de 340 a 350 K';
COMMENT ON COLUMN harmonized_cell.t350 IS 'Parte do harmonized com brilho de 350 a 360 K';
COMMENT ON COLUMN harmonized_cell.t_ge360 IS 'Parte do harmonized com brilho >= 360 K';
COMMENT ON COLUMN harmonized_cell.t_none IS 'Parte do harmonized sem brilho medido';
COMMENT ON COLUMN harmonized_cell.basis IS 'Base da temperatura: viirs (VIIRS I-4) ou aqua/terra (MODIS canal 21/22); nulo = desconhecida';
