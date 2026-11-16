-- Lugares grandes passam a ser exportados em partes disjuntas por hash de h3_cell (part 1..8); 0 = inteiro.
-- O fingerprint do snapshot inclui a regra de divisao, entao exportacoes anteriores nunca sao confundidas com partes.
ALTER TABLE annual_athena_export ADD COLUMN part smallint NOT NULL DEFAULT 0;
ALTER TABLE annual_athena_export ALTER COLUMN part DROP DEFAULT;
ALTER TABLE annual_athena_export ADD CHECK (CASE kind WHEN 'GRID' THEN part = 0 ELSE part BETWEEN 0 AND 8 END);
ALTER TABLE annual_athena_export DROP CONSTRAINT annual_athena_export_pkey;
ALTER TABLE annual_athena_export ADD PRIMARY KEY (snapshot_id, kind, resolution, month, part);
