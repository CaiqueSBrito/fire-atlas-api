-- NASA-018 Gate B.4 (C3-F01): versionamento explicito de source_semantic_hash e
-- isolamento controlado de assinaturas historicas pre-V6 sem inventar hashes nem alterar valores cientificos.

ALTER TABLE detection
    ADD COLUMN legacy_source_semantic_hash text,
    ADD COLUMN source_semantic_hash_version text,
    ADD COLUMN source_semantic_hash_status text NOT NULL DEFAULT 'UNKNOWN';

-- Registros pre-V6 com assinatura semantica nao versionada sao isolados em legacy_source_semantic_hash
-- (source_semantic_hash passa a NULL), evitando exposicao como hash atual e conflitos artificiais no indice unico V5.
UPDATE detection
SET legacy_source_semantic_hash = source_semantic_hash,
    source_semantic_hash = NULL,
    source_semantic_hash_version = 'v1-legacy-unverified',
    source_semantic_hash_status = 'LEGACY_UNVERIFIED'
WHERE source_semantic_hash IS NOT NULL
  AND source_semantic_hash_version IS NULL;

ALTER TABLE detection
    ADD CONSTRAINT chk_detection_semantic_hash_consistency CHECK ((
        source_semantic_hash_status IN ('CURRENT', 'UNKNOWN', 'LEGACY_UNVERIFIED', 'UPDATED_FROM_LEGACY')
        AND (
            (source_semantic_hash_status = 'CURRENT'
             AND source_semantic_hash IS NOT NULL
             AND source_semantic_hash_version IS NOT NULL
             AND source_semantic_hash_version = 'v2-ieee754-direct'
             AND (legacy_source_semantic_hash IS NULL OR legacy_source_semantic_hash IS NOT DISTINCT FROM source_semantic_hash))
            OR
            (source_semantic_hash_status = 'UNKNOWN'
             AND source_semantic_hash IS NULL
             AND legacy_source_semantic_hash IS NULL
             AND source_semantic_hash_version IS NULL)
            OR
            (source_semantic_hash_status = 'LEGACY_UNVERIFIED'
             AND source_semantic_hash IS NULL
             AND legacy_source_semantic_hash IS NOT NULL
             AND source_semantic_hash_version IS NOT NULL
             AND source_semantic_hash_version = 'v1-legacy-unverified')
            OR
            (source_semantic_hash_status = 'UPDATED_FROM_LEGACY'
             AND source_semantic_hash IS NOT NULL
             AND source_semantic_hash_version IS NOT NULL
             AND source_semantic_hash_version = 'v2-ieee754-direct'
             AND legacy_source_semantic_hash IS NOT NULL
             AND legacy_source_semantic_hash IS DISTINCT FROM source_semantic_hash)
        )
    ) IS TRUE);

CREATE INDEX idx_detection_semantic_status
    ON detection (source_file, acq_date, source_semantic_hash_status)
    WHERE source_semantic_hash_status IN ('LEGACY_UNVERIFIED', 'UNKNOWN');
