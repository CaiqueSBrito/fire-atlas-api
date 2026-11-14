package com.fireatlas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fireatlas.api.AnalysisController;
import com.fireatlas.api.ApiExceptionHandler;
import com.fireatlas.api.Dto;
import com.fireatlas.domain.BBox;
import com.fireatlas.ingestion.DetectionCsvLoader;
import com.fireatlas.ingestion.DetectionRowParser;
import com.fireatlas.ingestion.IngestionRunner;
import com.fireatlas.ingestion.IngestionTestBridge;
import com.fireatlas.oracle.FirmsIndependentOracle;
import com.fireatlas.query.DetectionRepository;
import com.fireatlas.query.HarmonizationService;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.PGConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Testes integrados contra container PostgreSQL/PostGIS descartavel isolado (porta 55432, banco fireatlas_test).
 * NUNCA executar contra o banco persistente principal (porta 5433 / banco fireatlas).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HarmonizationPostgisIT {

    private static final String DEFAULT_TEST_DB_URL = "jdbc:postgresql://localhost:55432/fireatlas_test";

    @DynamicPropertySource
    static void isolatedTestDbProperties(DynamicPropertyRegistry registry) {
        String url = System.getProperty("test.db.url",
                System.getenv().getOrDefault("TEST_DB_URL", DEFAULT_TEST_DB_URL));
        if (url.contains(":5433") || url.endsWith("/fireatlas")) {
            throw new IllegalStateException("Recusando executar testes integrados no banco principal: " + url);
        }
        registry.add("spring.datasource.url", () -> url);
        registry.add("spring.datasource.username", () -> System.getProperty("test.db.user", "fireatlas"));
        registry.add("spring.datasource.password", () -> System.getProperty("test.db.password", "fireatlas"));
        registry.add("fireatlas.limits.harmonize-timeout-ms", () -> "10000");
    }

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private DetectionRepository repo;

    @Autowired
    private PlatformTransactionManager txManager;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeAll
    void verifyIsolatedDatabaseAndMigrations() {
        String dbName = jdbc.queryForObject("SELECT current_database()", String.class);
        Integer port = jdbc.queryForObject("SELECT inet_server_port()", Integer.class);
        assertNotEquals("fireatlas", dbName, "Banco de teste deve ser isolado do banco principal 'fireatlas'");
        assertEquals("fireatlas_test", dbName);
        assertEquals(5432, port); // porta interna do container descartavel (mapeada em 55432 no host)

        List<String> versions = jdbc.queryForList(
                "SELECT version FROM flyway_schema_history WHERE success = true ORDER BY installed_rank",
                String.class);
        assertTrue(versions.containsAll(List.of("1", "2", "3", "4", "5", "6")),
                "Flyway V1, V2, V3, V4, V5 e V6 devem estar aplicadas: " + versions);

        Integer pkCols = jdbc.queryForObject(
                "SELECT count(*) FROM information_schema.table_constraints WHERE table_name = 'detection' AND constraint_name = 'pk_detection'",
                Integer.class);
        assertEquals(1, pkCols);

        Integer legacyRawUniqueIdx = jdbc.queryForObject(
                "SELECT count(*) FROM pg_indexes WHERE tablename = 'detection' AND indexname = 'uq_detection_source_line_hash'",
                Integer.class);
        assertEquals(0, legacyRawUniqueIdx, "V5 deve remover unicidade antiga sobre source_row_hash");

        Integer legacySemIdx = jdbc.queryForObject(
                "SELECT count(*) FROM pg_indexes WHERE tablename = 'detection' AND indexname = 'idx_detection_source_semantic'",
                Integer.class);
        assertEquals(0, legacySemIdx, "V5 deve substituir idx_detection_source_semantic por indice unico");

        Integer semUniqueIdx = jdbc.queryForObject(
                "SELECT count(*) FROM pg_indexes WHERE tablename = 'detection' AND indexname = 'uq_detection_source_line_semantic'",
                Integer.class);
        assertEquals(1, semUniqueIdx);

        Integer rawSupportIdx = jdbc.queryForObject(
                "SELECT count(*) FROM pg_indexes WHERE tablename = 'detection' AND indexname = 'idx_detection_source_raw_hash'",
                Integer.class);
        assertEquals(1, rawSupportIdx);

        Integer semanticStatusIdx = jdbc.queryForObject(
                "SELECT count(*) FROM pg_indexes WHERE tablename = 'detection' AND indexname = 'idx_detection_semantic_status'",
                Integer.class);
        assertEquals(1, semanticStatusIdx);

        Integer consistencyConstraint = jdbc.queryForObject(
                "SELECT count(*) FROM pg_constraint WHERE conrelid = 'detection'::regclass AND conname = 'chk_detection_semantic_hash_consistency' AND convalidated = true",
                Integer.class);
        assertEquals(1, consistencyConstraint);

        Integer totalPartitions = jdbc.queryForObject(
                "SELECT count(*) FROM pg_inherits WHERE inhparent = 'detection'::regclass",
                Integer.class);
        assertEquals(32, totalPartitions, "Deve haver 31 particoes anuais (2000..2030) + detection_default");

        Integer validatedPartitions = jdbc.queryForObject("""
                SELECT count(*)
                FROM pg_inherits i
                JOIN pg_constraint c ON c.conrelid = i.inhrelid
                WHERE i.inhparent = 'detection'::regclass
                  AND c.conname = 'chk_detection_semantic_hash_consistency'
                  AND c.convalidated = true
                """, Integer.class);
        assertEquals(totalPartitions, validatedPartitions,
                "Todas as particoes de detection devem possuir chk_detection_semantic_hash_consistency validada");
    }

    @BeforeEach
    void cleanTables() {
        String dbName = jdbc.queryForObject("SELECT current_database()", String.class);
        assertEquals("fireatlas_test", dbName);
        jdbc.execute("TRUNCATE detection, daily_grid, ingestion_log, user_preference RESTART IDENTITY");
    }

    private void insertDetection(
            String sensor, String satellite, String date, String ts,
            double lat, double lon, Float frp, String confRaw, String confClass,
            String daynight, String version, String product, String sourceFile) {
        jdbc.update("""
                INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon, frp,
                                       confidence_raw, confidence_class, daynight, version, source_product, source_file)
                VALUES (?, ?, ?::date, ?::timestamptz, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                sensor, satellite, date, ts, lat, lon, frp, confRaw, confClass, daynight, version, product, sourceFile);
    }

    private void insertDetectionWithExplicitId(
            long id,
            String sensor, String satellite, String date, String ts,
            double lat, double lon, Float frp, String confRaw, String confClass,
            String daynight, String version, String product, String sourceFile) {
        jdbc.update("""
                INSERT INTO detection (id, sensor, satellite, acq_date, acq_ts, lat, lon, frp,
                                       confidence_raw, confidence_class, daynight, version, source_product, source_file)
                VALUES (?, ?, ?, ?::date, ?::timestamptz, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                id, sensor, satellite, date, ts, lat, lon, frp, confRaw, confClass, daynight, version, product, sourceFile);
    }

    private IngestionRunner.IngestResult ingestCsvText(
            String csvText, String product, String sourceKey, BBox bbox, LocalDate start, LocalDate end) throws Exception {
        try (InputStream in = new ByteArrayInputStream(csvText.getBytes(StandardCharsets.UTF_8))) {
            return IngestionRunner.ingestStream(dataSource, in, product, sourceKey,
                    new DetectionCsvLoader.Filter(bbox, start, end));
        }
    }

    private ResponseEntity<Dto.HarmonizationResponse> getHarmonized(String query) {
        return rest.getForEntity("/api/v1/harmonized?" + query, Dto.HarmonizationResponse.class);
    }

    @Test
    void pairing1To1PreservesIdsProvenanceAndRejectsScientificClaims() {
        insertDetection("MODIS", "A", "2026-11-12", "2026-11-12T07:09:00Z", -3.4134, -53.52631,
                8.75f, "68", "n", "N", "6.1NRT", "MODIS_C6.1", "bronze/raw/MODIS_C6.1/2026/m.csv");
        insertDetection("VIIRS", "N20", "2026-11-12", "2026-11-12T04:46:00Z", -3.41376, -53.52631,
                6.78f, "nominal", "n", "N", "2.0NRT", "VIIRS_NOAA-20", "bronze/raw/VIIRS_NOAA-20/2026/v.csv");

        // Padrao sem includeAssociations e opt-in false, omitindo a lista enquanto preserva summary
        var defaultSummaryOnly = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12").getBody();
        assertNotNull(defaultSummaryOnly);
        assertFalse(defaultSummaryOnly.params().includeAssociations());
        assertNull(defaultSummaryOnly.associations());
        assertEquals(1, defaultSummaryOnly.summary().candidateAssociationsTotal());
        assertEquals(0, defaultSummaryOnly.summary().associationsReturned());

        var resp = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&includeAssociations=true");
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        var body = resp.getBody();
        assertNotNull(body);
        assertEquals("exploratory-spatial-temporal-v0.2", body.exploratoryMethodVersion());
        assertEquals("EXPLORATORY_ASSOCIATION", body.scientificState().correspondenceMode());
        assertEquals("METHOD_NOT_APPROVED", body.scientificState().harmonizationStatus());
        assertFalse(body.scientificState().scientificallyApproved());
        assertEquals("NOT_EVALUATED", body.scientificState().derivedClassificationStatus());
        assertEquals("PRODUCT_NATIVE_NOT_EQUIVALENT", body.scientificState().confidenceSemantics());

        assertEquals(1, body.result().modisEvaluated());
        assertEquals(1, body.result().modisWithViirsMatch());
        assertEquals(1.0, body.result().matchRate(), 1e-9);
        assertNull(body.result().meanModisFrp());
        assertNull(body.result().meanViirsFrpSum());
        assertNull(body.result().viirsToModisFrpRatio());
        assertNull(body.result().frpCorrelation());
        assertNull(body.result().medianViirsPixelsPerModis());
        assertTrue(body.unavailableIndicators().containsKey("viirsToModisFrpRatio"));
        assertEquals("METHOD_NOT_APPROVED", body.unavailableIndicators().get("viirsToModisFrpRatio").status());

        assertEquals(1, body.associations().size());
        var assoc = body.associations().get(0);
        assertEquals("EXPLORATORY_CANDIDATE", assoc.associationType());
        assertTrue(assoc.distanceMeters() > 39.0 && assoc.distanceMeters() < 41.0);
        assertEquals(-8580L, assoc.timeOffsetSeconds());
        assertEquals(143.0, assoc.absTimeDiffMinutes(), 1e-9);
        assertTrue(assoc.modis().id() > 0);
        assertEquals(LocalDate.of(2026, 11, 12), assoc.modis().acquiredDate());
        assertEquals("2026-11-12:" + assoc.modis().id(), assoc.modis().observationKey());
        assertNull(assoc.modis().sourceLineNumber());
        assertNull(assoc.modis().sourceRecordKey());
        assertTrue(assoc.viirs().id() > 0);
        assertEquals("2026-11-12:" + assoc.viirs().id(), assoc.viirs().observationKey());
        assertEquals("MODIS_C6.1", assoc.modis().sourceProduct());
        assertEquals("6.1NRT", assoc.modis().version());
        assertEquals("68", assoc.modis().confidenceRaw());
        assertEquals("MODIS_NUMERIC_0_100_NATIVE", assoc.modis().confidenceSemantics());
        assertEquals(8.75f, assoc.modis().frp());
        assertEquals("MW", assoc.modis().frpUnit());
        assertEquals("VIIRS_NOAA-20", assoc.viirs().sourceProduct());
        assertEquals("nominal", assoc.viirs().confidenceRaw());
        assertEquals("VIIRS_CATEGORICAL_L_N_H_NATIVE", assoc.viirs().confidenceSemantics());
        assertEquals(6.78f, assoc.viirs().frp());
    }

    @Test
    void pairing1ToNAndNMKeepAllCandidateEdgesWithoutDoubleCountingViirsOrFrp() {
        // 2 MODIS proximas + 2 VIIRS (V1 compartilhada por M1 e M2; V2 apenas com M2 -> total 3 pares N:M)
        insertDetection("MODIS", "A", "2026-11-12", "2026-11-12T07:09:00Z", -3.23000, -53.91400,
                11.0f, "74", "n", "N", "6.1NRT", "MODIS_C6.1", "m.csv");
        insertDetection("MODIS", "A", "2026-11-12", "2026-11-12T07:09:00Z", -3.23200, -53.91400,
                14.0f, "84", "h", "N", "6.1NRT", "MODIS_C6.1", "m.csv");
        // V1 entre M1 e M2 (~111m de ambos)
        insertDetection("VIIRS", "N20", "2026-11-12", "2026-11-12T05:29:00Z", -3.23100, -53.91400,
                5.0f, "nominal", "n", "N", "2.0NRT", "VIIRS_NOAA-20", "v20.csv");
        // V2 perto apenas de M2 (~650m sul de M2 = ~870m de M1)
        insertDetection("VIIRS", "N21", "2026-11-12", "2026-11-12T05:29:00Z", -3.23780, -53.91400,
                3.0f, "high", "h", "N", "2.0NRT", "VIIRS_NOAA-21", "v21.csv");

        var resp = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&includeAssociations=true");
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        var body = resp.getBody();
        assertNotNull(body);
        assertEquals(2, body.summary().modisEvaluated());
        assertEquals(2, body.summary().modisWithCandidateMatch());
        assertEquals(2, body.summary().distinctViirsMatched()); // V1 e V2 distintos (nao 3!)
        assertEquals(3, body.summary().candidateAssociationsTotal()); // (M1,V1), (M2,V1), (M2,V2)
        assertFalse(body.summary().exclusiveAssignmentApplied());
        assertEquals(3, body.associations().size());
        assertNull(body.result().meanViirsFrpSum());
        assertNull(body.result().viirsToModisFrpRatio());
        assertNull(body.result().frpCorrelation());
    }

    @Test
    void noCandidateMatchWithBothSensorsDistinguishedFromMissingData() {
        insertDetection("MODIS", "T", "2026-11-12", "2026-11-12T12:00:00Z", -3.5, -53.5,
                10.0f, "60", "n", "D", "6.1NRT", "MODIS_C6.1", "m.csv");
        // Mesmo local, porem 300 min depois (> 180 min)
        insertDetection("VIIRS", "N20", "2026-11-12", "2026-11-12T17:00:00Z", -3.5, -53.5,
                4.0f, "nominal", "n", "D", "2.0NRT", "VIIRS_NOAA-20", "v.csv");

        var resp = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&includeAssociations=true");
        var body = resp.getBody();
        assertNotNull(body);
        assertEquals("EXPLORATORY_ASSOCIATION", body.scientificState().correspondenceMode());
        assertEquals("BOTH_SENSORS_NO_CANDIDATE_PAIRS", body.scientificState().dataAvailabilityStatus());
        assertEquals(0.0, body.result().matchRate(), 1e-9);
        assertEquals(0, body.summary().candidateAssociationsTotal());
        assertEquals(0, body.associations().size());
        assertNotNull(body.message());
        assertTrue(body.message().contains("ausencia de pares nao indica ausencia de incendio"));
    }

    @Test
    void modisOnlyViirsOnlyAndNoDetectionsAreNotComputable() {
        // Sem dados
        var emptyResp = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12").getBody();
        assertNotNull(emptyResp);
        assertEquals("NOT_COMPUTABLE", emptyResp.scientificState().correspondenceMode());
        assertEquals("NO_DETECTIONS", emptyResp.scientificState().dataAvailabilityStatus());
        assertNull(emptyResp.result().matchRate());
        assertEquals("NOT_COMPUTABLE", emptyResp.unavailableIndicators().get("matchRate").status());

        // Apenas MODIS
        insertDetection("MODIS", "A", "2026-11-12", "2026-11-12T07:09:00Z", -3.5, -53.5,
                null, "50", "n", "N", "6.1NRT", "MODIS_C6.1", "m.csv");
        var modisOnly = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12").getBody();
        assertNotNull(modisOnly);
        assertEquals("NOT_COMPUTABLE", modisOnly.scientificState().correspondenceMode());
        assertEquals("MODIS_ONLY", modisOnly.scientificState().dataAvailabilityStatus());
        assertNull(modisOnly.result().matchRate());
        assertEquals(1, modisOnly.summary().modisEvaluated());

        // Apenas VIIRS (em outro bbox)
        insertDetection("VIIRS", "N20", "2026-11-12", "2026-11-12T07:09:00Z", -10.5, -60.5,
                2.0f, "high", "h", "N", "2.0NRT", "VIIRS_NOAA-20", "v.csv");
        var viirsOnly = getHarmonized("bbox=-61,-11,-60,-10&start=2026-11-12&end=2026-11-12").getBody();
        assertNotNull(viirsOnly);
        assertEquals("NOT_COMPUTABLE", viirsOnly.scientificState().correspondenceMode());
        assertEquals("VIIRS_ONLY", viirsOnly.scientificState().dataAvailabilityStatus());
        assertNull(viirsOnly.result().matchRate());
    }

    @Test
    void exactDistanceAndTimeBoundariesUtcDayYearAndGeodesicLimitDivergence() {
        // 1. Caso reproduzido de divergencia entre ST_Distance <= 750 e ST_DWithin(..., 750) (azimute 0 deg)
        Map<String, Object> boundaryNorth = jdbc.queryForMap("""
                WITH raw AS (
                    SELECT ST_Project(ST_SetSRID(ST_MakePoint(-53.5, -3.5), 4326)::geography, 750.0, radians(0)) AS p
                ),
                pt AS (
                    SELECT ST_SetSRID(ST_MakePoint(ST_X(p::geometry), ST_Y(p::geometry)), 4326)::geography AS g,
                           ST_Y(p::geometry) AS lat,
                           ST_X(p::geometry) AS lon
                    FROM raw
                )
                SELECT lat, lon,
                       ST_Distance(ST_SetSRID(ST_MakePoint(-53.5, -3.5), 4326)::geography, g) AS st_dist,
                       ST_DWithin(ST_SetSRID(ST_MakePoint(-53.5, -3.5), 4326)::geography, g, 750.0) AS raw_dwithin
                FROM pt
                """);
        double bLat = ((Number) boundaryNorth.get("lat")).doubleValue();
        double bLon = ((Number) boundaryNorth.get("lon")).doubleValue();
        double exactPostgisDist = ((Number) boundaryNorth.get("st_dist")).doubleValue();
        boolean rawDWithin = (Boolean) boundaryNorth.get("raw_dwithin");
        assertTrue(exactPostgisDist <= 750.0, "Ponto deve estar em <= 750.0 m: " + exactPostgisDist);
        assertFalse(rawDWithin, "Pre-condicao PostGIS 3.4: ST_DWithin(..., 750.0) retorna false no limite norte exato");
        assertEquals(750.0, FirmsIndependentOracle.vincentyDistanceMeters(-3.5, -53.5, bLat, bLon), 0.05);

        // 2. Pontos imediatamente abaixo e acima via oraculo Vincenty independente (sem usar ST_Project)
        double[] belowPt = FirmsIndependentOracle.vincentyDirectPoint(-3.5, -53.5, 90.0, 749.5);
        double[] overPt = FirmsIndependentOracle.vincentyDirectPoint(-3.5, -53.5, 90.0, 750.5);
        assertTrue(FirmsIndependentOracle.vincentyDistanceMeters(-3.5, -53.5, belowPt[0], belowPt[1]) < 750.0);
        assertTrue(FirmsIndependentOracle.vincentyDistanceMeters(-3.5, -53.5, overPt[0], overPt[1]) > 750.0);

        // Virada de ano UTC: MODIS 2024-12-31T23:30Z
        insertDetection("MODIS", "A", "2024-12-31", "2024-12-31T23:30:00Z", -3.5, -53.5,
                12.0f, "70", "n", "N", "6.1NRT", "MODIS_C6.1", "m2024.csv");
        // V_exact_boundary: +180 min exato e distancia exata no limite onde ST_DWithin bruto falhava -> INCLUIDO
        insertDetection("VIIRS", "N20", "2025-01-01", "2025-01-01T02:30:00Z", bLat, bLon,
                2.5f, "nominal", "n", "N", "2.0NRT", "VIIRS_NOAA-20", "v_exact.csv");
        // V_below: +60 min, 749.5 m -> INCLUIDO
        insertDetection("VIIRS", "N20", "2025-01-01", "2025-01-01T00:30:00Z", belowPt[0], belowPt[1],
                2.0f, "nominal", "n", "N", "2.0NRT", "VIIRS_NOAA-20", "v_below.csv");
        // V_in_minus: -180 min exato (2024-12-31T20:30Z), 0 m -> INCLUIDO
        insertDetection("VIIRS", "N21", "2024-12-31", "2024-12-31T20:30:00Z", -3.5, -53.5,
                3.0f, "high", "h", "D", "2.0NRT", "VIIRS_NOAA-21", "v2024.csv");
        // V_over_dist: +180 min, 750.5 m -> EXCLUIDO
        insertDetection("VIIRS", "N20", "2025-01-01", "2025-01-01T02:30:00Z", overPt[0], overPt[1],
                2.0f, "low", "l", "N", "2.0NRT", "VIIRS_NOAA-20", "v_over.csv");
        // V_out_time: +181 min (2025-01-01T02:31Z), 0 m -> EXCLUIDO
        insertDetection("VIIRS", "N20", "2025-01-01", "2025-01-01T02:31:00Z", -3.5, -53.5,
                2.0f, "low", "l", "N", "2.0NRT", "VIIRS_NOAA-20", "v2025.csv");

        var resp = getHarmonized("bbox=-54,-4,-53,-3&start=2024-12-31&end=2024-12-31&maxDistanceMeters=750&maxMinutes=180&includeAssociations=true");
        var body = resp.getBody();
        assertNotNull(body);
        assertEquals(3, body.summary().candidateAssociationsTotal());
        assertEquals(3, body.associations().size());
        assertTrue(body.associations().stream().anyMatch(a -> "v_exact.csv".equals(a.viirs().sourceFile())));
        assertTrue(body.associations().stream().anyMatch(a -> "v_below.csv".equals(a.viirs().sourceFile())));
        assertFalse(body.associations().stream().anyMatch(a -> "v_over.csv".equals(a.viirs().sourceFile())));
        assertTrue(body.associations().stream().anyMatch(a -> "D".equals(a.viirs().dayNight()) && "N".equals(a.modis().dayNight())));
        assertTrue(body.associations().stream().anyMatch(a -> a.timeOffsetSeconds() == 10800L));
        assertTrue(body.associations().stream().anyMatch(a -> a.timeOffsetSeconds() == -10800L));
        assertEquals("NOT_EVALUATED", body.scientificState().derivedClassificationStatus());

        // Fronteira em alta latitude (85N) e antimeridiano (180/-180) com calculo Vincenty independente
        double[] hiIn = FirmsIndependentOracle.vincentyDirectPoint(85.0, 10.0, 45.0, 749.5);
        double[] hiOut = FirmsIndependentOracle.vincentyDirectPoint(85.0, 10.0, 45.0, 750.5);
        insertDetection("MODIS", "T", "2024-12-31", "2024-12-31T12:00:00Z", 85.0, 10.0,
                5.0f, "60", "n", "D", "6.1NRT", "MODIS_C6.1", "hi_m.csv");
        insertDetection("VIIRS", "N20", "2024-12-31", "2024-12-31T12:00:00Z", hiIn[0], hiIn[1],
                1.0f, "high", "h", "D", "2.0NRT", "VIIRS_NOAA-20", "hi_in.csv");
        insertDetection("VIIRS", "N20", "2024-12-31", "2024-12-31T12:00:00Z", hiOut[0], hiOut[1],
                1.0f, "low", "l", "D", "2.0NRT", "VIIRS_NOAA-20", "hi_out.csv");

        var hiResp = getHarmonized("bbox=9.0,84.0,11.0,86.0&start=2024-12-31&end=2024-12-31&maxDistanceMeters=750&includeAssociations=true").getBody();
        assertNotNull(hiResp);
        assertEquals(1, hiResp.summary().candidateAssociationsTotal());
        assertEquals("hi_in.csv", hiResp.associations().get(0).viirs().sourceFile());
    }

    @Test
    void nullFrpAndIncompleteProvenancePreservedWithoutFabrication() {
        // Ambos FRP NULL + metadados incompletos (version NULL, confidence_raw NULL, daynight NULL, satellite vazio)
        insertDetection("MODIS", "", "2026-11-12", "2026-11-12T07:00:00Z", -3.5, -53.5,
                null, null, null, null, null, "MODIS_C6.1", "unknown");
        insertDetection("VIIRS", "N20", "2026-11-12", "2026-11-12T07:10:00Z", -3.5, -53.5,
                null, null, null, null, null, "VIIRS_NOAA-20", "v.csv");

        // Padrao sem includeAssociations e opt-in false, omitindo a lista enquanto preserva summary
        var defaultSummaryOnly = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12").getBody();
        assertNotNull(defaultSummaryOnly);
        assertFalse(defaultSummaryOnly.params().includeAssociations());
        assertNull(defaultSummaryOnly.associations());
        assertEquals(1, defaultSummaryOnly.summary().candidateAssociationsTotal());
        assertEquals(0, defaultSummaryOnly.summary().associationsReturned());

        var resp = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&includeAssociations=true");
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        var body = resp.getBody();
        assertNotNull(body);
        assertEquals(1, body.summary().modisEvaluated());
        assertEquals(0, body.summary().modisEvaluatedWithFrp());
        assertEquals(1, body.summary().modisEvaluatedWithoutFrp());
        assertEquals(0, body.summary().distinctViirsMatchedWithFrp());
        assertEquals(1, body.summary().distinctViirsMatchedWithoutFrp());

        var pair = body.associations().get(0);
        assertNull(pair.modis().frp());
        assertNull(pair.viirs().frp());
        assertNull(pair.modis().version());
        assertNull(pair.modis().confidenceRaw());
        assertNull(pair.modis().dayNight());
        assertTrue(body.scientificState().reasons().stream().anyMatch(r -> r.contains("proveniencia") || r.contains("ausente")));

        // CA03: even when native metadata fields exist, missing V3/V4 provenance fields trigger incomplete-provenance signal
        jdbc.execute("TRUNCATE detection, daily_grid, ingestion_log, user_preference RESTART IDENTITY");
        insertDetection("MODIS", "A", "2026-11-12", "2026-11-12T07:00:00Z", -3.5, -53.5,
                10.0f, "70", "n", "N", "6.1NRT", "MODIS_C6.1", "legacy_no_v3.csv");
        insertDetection("VIIRS", "N20", "2026-11-12", "2026-11-12T07:10:00Z", -3.5, -53.5,
                5.0f, "nominal", "n", "N", "2.0NRT", "VIIRS_NOAA-20", "legacy_v.csv");
        var v3Missing = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12").getBody();
        assertNotNull(v3Missing);
        assertTrue(v3Missing.scientificState().reasons().stream().anyMatch(r -> r.contains("V3/V4")));
    }

    @Test
    void deterministicSamplingAndTruncationAndIncludeAssociationsFalse() {
        // Insere 105 deteccoes MODIS com empates de acq_ts + 2 VIIRS proximas a algumas delas
        for (int i = 0; i < 105; i++) {
            String ts = i < 50 ? "2026-11-12T07:00:00Z" : "2026-11-12T08:00:00Z";
            insertDetection("MODIS", "A", "2026-11-12", ts, -3.5, -53.5 + (i * 0.00001),
                    5.0f, "70", "n", "N", "6.1NRT", "MODIS_C6.1", "m.csv");
        }
        insertDetection("VIIRS", "N20", "2026-11-12", "2026-11-12T07:10:00Z", -3.5, -53.5,
                2.0f, "nominal", "n", "N", "2.0NRT", "VIIRS_NOAA-20", "v.csv");

        // Caso exact cap (1000 >= 105): sampled DEVE ser false
        var unsampled = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&modisSampleCap=1000").getBody();
        assertNotNull(unsampled);
        assertFalse(unsampled.result().sampled());
        assertFalse(unsampled.summary().modisSampled());
        assertEquals(105, unsampled.summary().modisTotalInWindow());
        assertEquals(105, unsampled.summary().modisEvaluated());

        // Caso amostrado (cap=100 < 105) e truncado (associationsLimit=5): deterministico em chamadas repetidas
        var r1 = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&modisSampleCap=100&includeAssociations=true&associationsLimit=5").getBody();
        var r2 = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&modisSampleCap=100&includeAssociations=true&associationsLimit=5").getBody();
        assertNotNull(r1);
        assertNotNull(r2);
        assertTrue(r1.result().sampled());
        assertEquals(100, r1.summary().modisEvaluated());
        assertEquals(105, r1.summary().modisTotalInWindow());
        assertTrue(r1.summary().associationsTruncated());
        assertEquals(5, r1.summary().associationsReturned());
        assertEquals(100, r1.summary().candidateAssociationsTotal());
        assertEquals(r1.associations(), r2.associations());

        // Padrao (sem includeAssociations) omite lista mantendo contagens
        var summaryOnlyDefault = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&modisSampleCap=100").getBody();
        assertNotNull(summaryOnlyDefault);
        assertFalse(summaryOnlyDefault.params().includeAssociations());
        assertNull(summaryOnlyDefault.associations());
        assertEquals(0, summaryOnlyDefault.summary().associationsReturned());
        assertFalse(summaryOnlyDefault.summary().associationsTruncated());
        assertEquals(100, summaryOnlyDefault.summary().candidateAssociationsTotal());

        // Explicitamente includeAssociations=false omite lista mantendo contagens
        var summaryOnly = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&modisSampleCap=100&includeAssociations=false").getBody();
        assertNotNull(summaryOnly);
        assertNull(summaryOnly.associations());
        assertEquals(0, summaryOnly.summary().associationsReturned());
        assertFalse(summaryOnly.summary().associationsTruncated());
        assertEquals(100, summaryOnly.summary().candidateAssociationsTotal());
    }

    @Test
    void antimeridianAndHighLatitudeGeodesicMatching() {
        // Antimeridiano: MODIS em +179.999, VIIRS em -179.999 (~219.3m na lat 10)
        insertDetection("MODIS", "A", "2026-11-12", "2026-11-12T10:00:00Z", 10.0, 179.999,
                10.0f, "80", "h", "D", "6.1NRT", "MODIS_C6.1", "anti_m.csv");
        insertDetection("VIIRS", "N20", "2026-11-12", "2026-11-12T10:30:00Z", 10.0, -179.999,
                2.0f, "high", "h", "D", "2.0NRT", "VIIRS_NOAA-20", "anti_v.csv");

        var anti = getHarmonized("bbox=179.0,9.0,180.0,11.0&start=2026-11-12&end=2026-11-12&includeAssociations=true").getBody();
        assertNotNull(anti);
        assertEquals(1, anti.summary().candidateAssociationsTotal());
        double expectedAntiDist = FirmsIndependentOracle.vincentyDistanceMeters(10.0, 179.999, 10.0, -179.999);
        assertEquals(expectedAntiDist, anti.associations().get(0).distanceMeters(), 0.25);

        // Alta latitude (85N): 0.05 deg lon ~486.7m
        insertDetection("MODIS", "T", "2026-11-12", "2026-11-12T11:00:00Z", 85.0, 10.0,
                5.0f, "50", "n", "D", "6.1NRT", "MODIS_C6.1", "hi_m.csv");
        insertDetection("VIIRS", "N21", "2026-11-12", "2026-11-12T11:15:00Z", 85.0, 10.05,
                1.0f, "nominal", "n", "D", "2.0NRT", "VIIRS_NOAA-21", "hi_v.csv");

        var highLat = getHarmonized("bbox=9.0,84.0,11.0,86.0&start=2026-11-12&end=2026-11-12&includeAssociations=true").getBody();
        assertNotNull(highLat);
        assertEquals(1, highLat.summary().candidateAssociationsTotal());
        double expectedHiDist = FirmsIndependentOracle.vincentyDistanceMeters(85.0, 10.0, 85.0, 10.05);
        assertEquals(expectedHiDist, highLat.associations().get(0).distanceMeters(), 0.25);
    }

    @Test
    void statementTimeoutAbortsWithoutPartialResultsAndMapsTo504ProblemDetail() {
        var tx = new TransactionTemplate(txManager);
        tx.setReadOnly(true);
        tx.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);

        DataAccessException ex = assertThrows(DataAccessException.class, () -> tx.execute(status -> {
            repo.setTransactionStatementTimeoutMs(5);
            jdbc.execute("SELECT pg_sleep(0.05)");
            return null;
        }));
        assertTrue(ex instanceof QueryTimeoutException
                || (ex.getMessage() != null && ex.getMessage().contains("statement timeout")),
                "Esperado QueryTimeoutException/57014, obtido: " + ex);

        // Valida tambem que o orcamento compartilhado no HarmonizationService aborta antes do segundo statement
        var slowRepo = new DetectionRepository(new org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate(jdbc)) {
            @Override
            public HarmonizationWindowTotals harmonizationWindowTotals(BBox b, LocalDate s, LocalDate e) {
                jdbc.execute("SELECT pg_sleep(0.06)");
                return new HarmonizationWindowTotals(1, 1);
            }

            @Override
            public HarmonizationQueryResult findExploratoryAssociations(
                    HarmonizationWindowTotals totals, BBox b, LocalDate s, LocalDate e,
                    int maxDistM, int maxMinutes, int modisCap, boolean includeAssociations, int associationsLimit) {
                fail("Segunda query nao deveria ser iniciada apos estouro do orcamento da transacao");
                return null;
            }
        };
        var shortBudgetService = new HarmonizationService(slowRepo, txManager, 30);
        assertThrows(QueryTimeoutException.class, () -> shortBudgetService.harmonize(
                new BBox(-54, -4, -53, -3), LocalDate.of(2026, 11, 12), LocalDate.of(2026, 11, 12),
                750, 180, 20000, true, 500));

        ProblemDetail pd = new ApiExceptionHandler().queryTimeout(new QueryTimeoutException("timeout", ex));
        assertEquals(504, pd.getStatus());
        assertTrue(pd.getDetail().contains("nenhum resultado parcial"));
    }

    @Test
    void compositeIdentityAcrossPartitionsPreventsCrossDateIdCollisionInCountsAndDtos() {
        // C1: Duas MODIS em datas distintas com o mesmo id=777 e duas VIIRS em datas distintas com o mesmo id=888
        insertDetectionWithExplicitId(777L, "MODIS", "A", "2026-11-12", "2026-11-12T23:30:00Z", -3.5, -53.5,
                10.0f, "70", "n", "N", "6.1NRT", "MODIS_C6.1", "c1_m.csv");
        insertDetectionWithExplicitId(777L, "MODIS", "A", "2026-11-13", "2026-11-13T00:10:00Z", -3.501, -53.5,
                12.0f, "80", "h", "N", "6.1NRT", "MODIS_C6.1", "c1_m.csv");
        insertDetectionWithExplicitId(888L, "VIIRS", "N20", "2026-11-12", "2026-11-12T23:40:00Z", -3.5005, -53.5,
                3.0f, "nominal", "n", "N", "2.0NRT", "VIIRS_NOAA-20", "c1_v.csv");
        insertDetectionWithExplicitId(888L, "VIIRS", "N21", "2026-11-13", "2026-11-13T00:20:00Z", -3.5006, -53.5,
                4.0f, "high", "h", "N", "2.0NRT", "VIIRS_NOAA-21", "c1_v.csv");

        var resp = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-13&maxDistanceMeters=750&maxMinutes=180&includeAssociations=true");
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        var body = resp.getBody();
        assertNotNull(body);
        assertEquals(2, body.summary().modisEvaluated());
        assertEquals(2, body.summary().modisWithCandidateMatch());
        assertEquals(2, body.summary().distinctViirsMatched());
        assertEquals(4, body.summary().candidateAssociationsTotal());
        assertEquals(1.0, body.result().matchRate(), 1e-9);
        assertEquals(4, body.associations().size());

        Set<String> modisKeys = new HashSet<>();
        Set<String> viirsKeys = new HashSet<>();
        for (var a : body.associations()) {
            modisKeys.add(a.modis().observationKey());
            viirsKeys.add(a.viirs().observationKey());
        }
        assertEquals(Set.of("2026-11-12:777", "2026-11-13:777"), modisKeys);
        assertEquals(Set.of("2026-11-12:888", "2026-11-13:888"), viirsKeys);
    }

    @Test
    void reingestionPreservesIdsIncludingLegitimateDuplicatesAndAuditLog() throws Exception {
        // C2: Retransmissao identica preserva IDs [antes == depois], inclusive com registros duplicados legitimos
        String header = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
        String row1 = "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,10.5,N\n";
        String row2DupCoords = "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,10.5,N\n";
        String csv = header + row1 + row2DupCoords;
        BBox bbox = new BBox(-54, -4, -53, -3);
        LocalDate d = LocalDate.of(2026, 11, 12);

        var r1 = ingestCsvText(csv, "MODIS_C6.1", "local:dup_reingest.csv", bbox, d, d);
        assertEquals(2, r1.rowsInserted());
        assertEquals(0, r1.rowsUnchanged());
        assertEquals(0, r1.rowsDeleted());
        assertNotNull(r1.sourceSha256());

        List<Map<String, Object>> beforeRows = jdbc.queryForList(
                "SELECT id, source_line_number, source_row_hash FROM detection WHERE source_file = 'local:dup_reingest.csv' ORDER BY id");
        assertEquals(2, beforeRows.size());
        assertEquals(2L, ((Number) beforeRows.get(0).get("source_line_number")).longValue());
        assertEquals(3L, ((Number) beforeRows.get(1).get("source_line_number")).longValue());
        List<Long> idsBefore = beforeRows.stream().map(m -> ((Number) m.get("id")).longValue()).toList();

        // Retransmissao identica
        var r2 = ingestCsvText(csv, "MODIS_C6.1", "local:dup_reingest.csv", bbox, d, d);
        assertEquals(0, r2.rowsInserted());
        assertEquals(2, r2.rowsUnchanged());
        assertEquals(0, r2.rowsDeleted());

        List<Long> idsAfter = jdbc.queryForList(
                "SELECT id FROM detection WHERE source_file = 'local:dup_reingest.csv' ORDER BY id", Long.class);
        assertEquals(idsBefore, idsAfter, "IDs nao podem mudar em retransmissao identica");
        assertEquals(2, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid WHERE sensor = 'MODIS'", Integer.class));
    }

    @Test
    void partialOverlappingDisjointModifiedAndRollbackReloadsPreserveExternalRegions() throws Exception {
        // C3 + C2: Regiao A (-53.8, -3.5), Overlap (-53.5, -3.5), Regiao B (-53.2, -3.5)
        String header = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
        String regA = "-3.5,-53.8,310.0,1.0,1.0,2026-11-12,0709,A,70,6.1NRT,290.0,11.0,N\n";
        String regOv = "-3.5,-53.5,311.0,1.0,1.0,2026-11-12,0709,A,75,6.1NRT,290.0,12.0,N\n";
        String regB = "-3.5,-53.2,312.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,13.0,N\n";
        String csvV1 = header + regA + regOv + regB;
        LocalDate d = LocalDate.of(2026, 11, 12);

        // 1. Bboxes disjuntas: carga inicial apenas em A [-54,-4,-53.7,-3], depois B [-53.3,-4,-53.0,-3]
        BBox bboxA = new BBox(-54.0, -4.0, -53.7, -3.0);
        BBox bboxB = new BBox(-53.3, -4.0, -53.0, -3.0);
        BBox bboxFull = new BBox(-54.0, -4.0, -53.0, -3.0);

        ingestCsvText(csvV1, "MODIS_C6.1", "local:regions.csv", bboxA, d, d);
        ingestCsvText(csvV1, "MODIS_C6.1", "local:regions.csv", bboxB, d, d);
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));

        Long idADisjoint = jdbc.queryForObject("SELECT id FROM detection WHERE lon < -53.7", Long.class);
        Long idBDisjoint = jdbc.queryForObject("SELECT id FROM detection WHERE lon > -53.3", Long.class);

        // 2. Recarga com bbox ampla traz regOv mantendo idA e idB
        ingestCsvText(csvV1, "MODIS_C6.1", "local:regions.csv", bboxFull, d, d);
        assertEquals(3, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(idADisjoint, jdbc.queryForObject("SELECT id FROM detection WHERE lon < -53.7", Long.class));
        assertEquals(idBDisjoint, jdbc.queryForObject("SELECT id FROM detection WHERE lon > -53.3", Long.class));
        Long idOv = jdbc.queryForObject("SELECT id FROM detection WHERE lon BETWEEN -53.6 AND -53.4", Long.class);

        // 3. Recarga parcial identica restrita a bboxA preserva integralmente regOv e regB!
        ingestCsvText(csvV1, "MODIS_C6.1", "local:regions.csv", bboxA, d, d);
        assertEquals(3, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(idBDisjoint, jdbc.queryForObject("SELECT id FROM detection WHERE lon > -53.3", Long.class));
        assertEquals(idOv, jdbc.queryForObject("SELECT id FROM detection WHERE lon BETWEEN -53.6 AND -53.4", Long.class));

        // 4. Recarga sobreposta (bboxOvB = [-53.6,-4,-53.0,-3]) com conteudo alterado em regOv (frp=99.0 e linha deslocada)
        BBox bboxOvB = new BBox(-53.6, -4.0, -53.0, -3.0);
        String regOvChanged = "-3.5,-53.5,311.0,1.0,1.0,2026-11-12,0709,A,75,6.1NRT,290.0,99.0,N\n";
        String csvV2 = header + regB + regOvChanged; // linha nova sem regA e com regB/regOv trocados de linha
        ingestCsvText(csvV2, "MODIS_C6.1", "local:regions.csv", bboxOvB, d, d);

        // RegA fora de bboxOvB DEVE estar intacta com o mesmo ID!
        assertEquals(idADisjoint, jdbc.queryForObject("SELECT id FROM detection WHERE lon < -53.7", Long.class));
        // RegOv dentro de bboxOvB alterada recebe novo id (nao reaproveita identidade como mesmo registro) e novo FRP=99.0
        Map<String, Object> ovRow = jdbc.queryForMap("SELECT id, frp FROM detection WHERE lon BETWEEN -53.6 AND -53.4");
        assertNotEquals(idOv, ((Number) ovRow.get("id")).longValue());
        assertEquals(99.0f, ((Number) ovRow.get("frp")).floatValue());

        // 5. Falha no meio do COPY/transacao provoca rollback completo sem perda ou duplicacao
        int countBeforeFail = jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class);
        InputStream failingStream = new InputStream() {
            private final byte[] ok = (header + regOv).getBytes(StandardCharsets.UTF_8);
            private int pos = 0;

            @Override
            public int read() throws IOException {
                if (pos >= ok.length) throw new IOException("Falha simulada no meio do stream S3");
                return ok[pos++] & 0xFF;
            }
        };
        assertThrows(IOException.class, () -> IngestionRunner.ingestStream(
                dataSource, failingStream, "MODIS_C6.1", "local:regions.csv",
                new DetectionCsvLoader.Filter(bboxOvB, d, d)));
        assertEquals(countBeforeFail, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(99.0f, jdbc.queryForObject("SELECT frp FROM detection WHERE lon BETWEEN -53.6 AND -53.4", Float.class));
    }

    @Test
    void dailyGridConsistencyOnEmptyLoadDeletionZeroOutRollbackAndConcurrency() throws Exception {
        // C4: Carga vazia inicial -> 0
        BBox full = new BBox(-54, -4, -53, -3);
        LocalDate d1 = LocalDate.of(2026, 11, 12);
        LocalDate d2 = LocalDate.of(2026, 11, 13);
        String header = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";

        var emptyInit = ingestCsvText(header, "MODIS_C6.1", "local:empty.csv", full, d1, d2);
        assertEquals(0, emptyInit.stats().kept());
        assertEquals(0, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid", Integer.class));

        // Arquivo 1 carrega duas datas (d1 em celula cx=1264, d2 em celula cx=1268); Arquivo 2 carrega outra regiao (d1 em cx=1261)
        String file1 = header
                + "-3.55,-53.55,310.0,1.0,1.0,2026-11-12,0709,A,70,6.1NRT,290.0,10.0,N\n"
                + "-3.15,-53.15,310.0,1.0,1.0,2026-11-13,0709,A,70,6.1NRT,290.0,20.0,N\n";
        String file2 = header
                + "-3.85,-53.85,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,30.0,N\n";

        ingestCsvText(file1, "MODIS_C6.1", "local:g1.csv", full, d1, d2);
        ingestCsvText(file2, "MODIS_C6.1", "local:g2.csv", full, d1, d2);
        assertEquals(3, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(3, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid", Integer.class));

        // Remove todos os registros de local:g1.csv recarregando apenas o cabecalho (kept = 0) -> d2 deve zerar e d1 de g2 permanecer!
        var emptyReload = ingestCsvText(header, "MODIS_C6.1", "local:g1.csv", full, d1, d2);
        assertEquals(0, emptyReload.stats().kept());
        assertEquals(2, emptyReload.rowsDeleted());
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM daily_grid WHERE acq_date = '2026-11-13'", Integer.class));

        // Concorrencia: duas cargas simultaneas de arquivos distintos na mesma celula (acq_date, sensor, cx, cy)
        String concA = header + "-3.85,-53.85,310.0,1.0,1.0,2026-11-12,0710,A,80,6.1NRT,290.0,5.0,N\n";
        String concB = header + "-3.85,-53.85,310.0,1.0,1.0,2026-11-12,0711,A,80,6.1NRT,290.0,7.0,N\n";
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        Callable<IngestionRunner.IngestResult> taskA = () -> {
            ready.countDown();
            start.await(5, TimeUnit.SECONDS);
            return ingestCsvText(concA, "MODIS_C6.1", "local:concA.csv", full, d1, d1);
        };
        Callable<IngestionRunner.IngestResult> taskB = () -> {
            ready.countDown();
            start.await(5, TimeUnit.SECONDS);
            return ingestCsvText(concB, "MODIS_C6.1", "local:concB.csv", full, d1, d1);
        };
        try {
            Future<IngestionRunner.IngestResult> fa = pool.submit(taskA);
            Future<IngestionRunner.IngestResult> fb = pool.submit(taskB);
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            fa.get(10, TimeUnit.SECONDS);
            fb.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        assertEquals(3, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(3, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid", Integer.class));
    }

    @Test
    void realNasaFirmsFixturesMatchAll58PairsInIndependentOracleAndSupportRegressionEndpoints() throws Exception {
        loadFixture("fixtures/firms_brazil_2026_11_12_13/MODIS_C6_1_South_America_24h.csv",
                "MODIS_C6.1", "local:MODIS_C6_1_South_America_24h.csv");
        loadFixture("fixtures/firms_brazil_2026_11_12_13/J1_VIIRS_C2_South_America_24h.csv",
                "VIIRS_NOAA-20", "local:J1_VIIRS_C2_South_America_24h.csv");
        loadFixture("fixtures/firms_brazil_2026_11_12_13/J2_VIIRS_C2_South_America_24h.csv",
                "VIIRS_NOAA-21", "local:J2_VIIRS_C2_South_America_24h.csv");

        var oracle = FirmsIndependentOracle.verifyAndCompute(
                FirmsIndependentOracle.FIXTURE_DIR,
                FirmsIndependentOracle.DEFAULT_MAX_DISTANCE_METERS,
                FirmsIndependentOracle.DEFAULT_MAX_SECONDS
        );

        // Padrao sem includeAssociations: resumo completo, associations omitido
        var defaultSummary = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-13&maxDistanceMeters=750&maxMinutes=180").getBody();
        assertNotNull(defaultSummary);
        assertFalse(defaultSummary.params().includeAssociations());
        assertNull(defaultSummary.associations());
        assertEquals(58, defaultSummary.summary().candidateAssociationsTotal());

        var resp = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-13&maxDistanceMeters=750&maxMinutes=180&includeAssociations=true");
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        var body = resp.getBody();
        assertNotNull(body);
        assertEquals(oracle.modisTotal(), body.summary().modisTotalInWindow());
        assertEquals(oracle.viirsTotal(), body.summary().viirsTotalInWindow());
        assertEquals(oracle.modisTotal(), body.summary().modisEvaluated());
        assertEquals(oracle.modisMatched(), body.summary().modisWithCandidateMatch());
        assertEquals(oracle.distinctViirsMatched(), body.summary().distinctViirsMatched());
        assertEquals(oracle.pairs().size(), body.summary().candidateAssociationsTotal());
        assertEquals(58, body.associations().size());
        assertFalse(body.summary().associationsTruncated());
        assertFalse(body.scientificState().scientificallyApproved());
        assertEquals("METHOD_NOT_APPROVED", body.scientificState().harmonizationStatus());
        assertEquals("NOT_EVALUATED", body.scientificState().derivedClassificationStatus());

        // C7: Conferencia integral 1-a-1 dos 58 pares candidatos contra o oraculo independente
        List<String> oraclePairSigs = oracle.pairs().stream()
                .map(p -> sig(p.modis().acquiredAt(), p.modis().lat(), p.modis().lon(),
                        p.viirs().satellite(), p.viirs().acquiredAt(), p.viirs().lat(), p.viirs().lon(),
                        p.timeOffsetSeconds()))
                .sorted()
                .toList();
        List<String> apiPairSigs = body.associations().stream()
                .map(a -> sig(a.modis().acquiredAt(), a.modis().lat(), a.modis().lon(),
                        a.viirs().satellite(), a.viirs().acquiredAt(), a.viirs().lat(), a.viirs().lon(),
                        a.timeOffsetSeconds()))
                .sorted()
                .toList();
        assertEquals(oraclePairSigs, apiPairSigs);

        for (var apiPair : body.associations()) {
            String key = sig(apiPair.modis().acquiredAt(), apiPair.modis().lat(), apiPair.modis().lon(),
                    apiPair.viirs().satellite(), apiPair.viirs().acquiredAt(), apiPair.viirs().lat(), apiPair.viirs().lon(),
                    apiPair.timeOffsetSeconds());
            var expected = oracle.pairs().stream()
                    .filter(p -> sig(p.modis().acquiredAt(), p.modis().lat(), p.modis().lon(),
                            p.viirs().satellite(), p.viirs().acquiredAt(), p.viirs().lat(), p.viirs().lon(),
                            p.timeOffsetSeconds()).equals(key))
                    .findFirst()
                    .orElseThrow();
            assertEquals(expected.vincentyDistanceMeters(), apiPair.distanceMeters(),
                    FirmsIndependentOracle.POSTGIS_COMPARISON_TOLERANCE_METERS);
            assertNotNull(apiPair.modis().observationKey());
            assertNotNull(apiPair.modis().sourceLineNumber());
            assertNotNull(apiPair.modis().sourceRecordKey());
            assertNotNull(apiPair.modis().sourceRawLocator());
            assertEquals(64, apiPair.modis().sourceRowHash().length());
            assertEquals(64, apiPair.modis().sourceSemanticHash().length());
            assertEquals(64, apiPair.modis().sourceFileSha256().length());
            assertNotNull(apiPair.viirs().observationKey());
            assertNotNull(apiPair.viirs().sourceLineNumber());
            assertNotNull(apiPair.viirs().sourceRecordKey());
            assertNotNull(apiPair.viirs().sourceRawLocator());
            assertEquals(64, apiPair.viirs().sourceRowHash().length());
            assertEquals(64, apiPair.viirs().sourceSemanticHash().length());
            assertEquals(64, apiPair.viirs().sourceFileSha256().length());
        }

        // Verifica plano SQL com poda de particoes e indice geografico
        List<String> plan = jdbc.queryForList("""
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT m.id, v.id, ST_Distance(v.geom::geography, m.geom::geography)
                FROM detection m
                JOIN detection v ON v.sensor = 'VIIRS'
                  AND v.acq_date BETWEEN '2026-11-11'::date AND '2026-11-14'::date
                  AND ST_DWithin(v.geom::geography, m.geom::geography, 751.0)
                  AND ST_Distance(v.geom::geography, m.geom::geography) <= 750.0
                WHERE m.sensor = 'MODIS'
                  AND m.acq_date BETWEEN '2026-11-12'::date AND '2026-11-13'::date
                """, String.class);
        String joinedPlan = String.join("\n", plan);
        assertTrue(joinedPlan.contains("detection_2026"), "Plano deve usar particao detection_2026: " + joinedPlan);

        // Verifica regressao dos demais endpoints /api/v1 contra PostGIS real
        var detPoints = rest.getForEntity("/api/v1/detections?bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-13&zoom=10", Dto.DetectionsResponse.class);
        assertEquals(HttpStatus.OK, detPoints.getStatusCode());
        assertEquals(196, detPoints.getBody().count());

        var detGrid = rest.getForEntity("/api/v1/detections?bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-13&zoom=6", Dto.DetectionsResponse.class);
        assertEquals(HttpStatus.OK, detGrid.getStatusCode());
        assertEquals("grid", detGrid.getBody().mode());
        assertEquals(196, detGrid.getBody().count());

        var cal = rest.getForEntity("/api/v1/calendar?bbox=-54,-4,-53,-3&month=2026-11", Dto.CalendarResponse.class);
        assertEquals(HttpStatus.OK, cal.getStatusCode());
        assertTrue(cal.getBody().daysWithFire() >= 1);

        var cmp = rest.getForEntity("/api/v1/compare?bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-13", Dto.CompareResponse.class);
        assertEquals(HttpStatus.OK, cmp.getStatusCode());
        assertEquals(2, cmp.getBody().sensors().size());

        var ts = rest.getForEntity("/api/v1/timeseries?bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-13", Dto.TimeseriesResponse.class);
        assertEquals(HttpStatus.OK, ts.getStatusCode());

        var cov = rest.getForEntity("/api/v1/meta/coverage", Dto.Coverage[].class);
        assertEquals(HttpStatus.OK, cov.getStatusCode());
        assertEquals(2, cov.getBody().length);

        var src = rest.getForEntity("/api/v1/meta/sources", Dto.SourceInfo[].class);
        assertEquals(HttpStatus.OK, src.getStatusCode());
        assertEquals(3, src.getBody().length);

        var prefPut = rest.exchange("/api/v1/preferences", HttpMethod.PUT,
                new HttpEntity<>(new Dto.Preferences("Para", List.of(-54.0, -4.0, -53.0, -3.0), "BOTH", 8, List.of(-53.5, -3.5))),
                Dto.Preferences.class);
        assertEquals(HttpStatus.OK, prefPut.getStatusCode());
    }

    @Test
    void flywayV1ToV6MigratesLegacyRowsAndV4ToV6PreservesDataAcrossRevisions() throws Exception {
        String baseUrl = System.getProperty("test.db.url",
                System.getenv().getOrDefault("TEST_DB_URL", DEFAULT_TEST_DB_URL));
        String adminUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/')) + "/postgres";
        String upgradeDb = "fireatlas_v1_upgrade_test";
        String upgradeUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/')) + "/" + upgradeDb;
        String v4UpgradeDb = "fireatlas_v4_upgrade_test";
        String v4UpgradeUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/')) + "/" + v4UpgradeDb;
        String user = System.getProperty("test.db.user", "fireatlas");
        String pass = System.getProperty("test.db.password", "fireatlas");

        try (Connection admin = java.sql.DriverManager.getConnection(adminUrl, user, pass);
             var st = admin.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS " + upgradeDb);
            st.execute("CREATE DATABASE " + upgradeDb);
            st.execute("DROP DATABASE IF EXISTS " + v4UpgradeDb);
            st.execute("CREATE DATABASE " + v4UpgradeDb);
        }
        try {
            // 1. Banco V1 com registro legado -> upgrade V1 -> V6
            Flyway.configure().dataSource(upgradeUrl, user, pass).locations("classpath:db/migration").target("1").load().migrate();

            try (Connection c = java.sql.DriverManager.getConnection(upgradeUrl, user, pass);
                 var ps = c.prepareStatement("""
                         INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon,
                                                frp, confidence_raw, confidence_class, daynight, version,
                                                source_product, source_file)
                         VALUES ('MODIS', 'A', '2024-12-31', '2024-12-31T23:30:00Z', -3.5, -53.5,
                                 NULL, NULL, NULL, NULL, NULL, 'MODIS_C6.1', 'legacy_v1.csv')
                         """)) {
                ps.executeUpdate();
            }

            var v1ToLatest = Flyway.configure().dataSource(upgradeUrl, user, pass).locations("classpath:db/migration").load().migrate();
            assertEquals(5, v1ToLatest.migrationsExecuted);
            assertEquals("6", v1ToLatest.targetSchemaVersion);

            try (Connection c = java.sql.DriverManager.getConnection(upgradeUrl, user, pass);
                 var st = c.createStatement();
                 var rs = st.executeQuery("""
                         SELECT id, frp, confidence_raw, daynight, version, source_file,
                                source_line_number, source_row_hash, source_semantic_hash, source_file_sha256,
                                legacy_source_semantic_hash, source_semantic_hash_version, source_semantic_hash_status
                         FROM detection
                         """)) {
                assertTrue(rs.next());
                assertTrue(rs.getLong("id") > 0);
                assertNull(rs.getObject("frp"));
                assertNull(rs.getString("confidence_raw"));
                assertNull(rs.getString("daynight"));
                assertNull(rs.getString("version"));
                assertEquals("legacy_v1.csv", rs.getString("source_file"));
                assertNull(rs.getObject("source_line_number"));
                assertNull(rs.getString("source_row_hash"));
                assertNull(rs.getString("source_semantic_hash"));
                assertNull(rs.getString("source_file_sha256"));
                assertNull(rs.getString("legacy_source_semantic_hash"));
                assertNull(rs.getString("source_semantic_hash_version"));
                assertEquals("UNKNOWN", rs.getString("source_semantic_hash_status"));
                assertFalse(rs.next());
            }

            String header = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
            String row = "-3.6,-53.6,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,12.5,N\n";
            try (Connection c = java.sql.DriverManager.getConnection(upgradeUrl, user, pass)) {
                c.setAutoCommit(false);
                var ing = IngestionRunner.ingestOnConnection(c,
                        new ByteArrayInputStream((header + row).getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:post_v6.csv",
                        new DetectionCsvLoader.Filter(new BBox(-54, -4, -53, -3),
                                LocalDate.of(2026, 11, 12), LocalDate.of(2026, 11, 12)));
                c.commit();
                assertEquals(1, ing.rowsInserted());
                try (var st = c.createStatement();
                     var rs = st.executeQuery("""
                             SELECT source_line_number, source_row_hash, source_semantic_hash, source_file_sha256,
                                    legacy_source_semantic_hash, source_semantic_hash_version, source_semantic_hash_status
                             FROM detection WHERE source_file = 'local:post_v6.csv'
                             """)) {
                    assertTrue(rs.next());
                    assertEquals(2L, rs.getLong("source_line_number"));
                    assertEquals(64, rs.getString("source_row_hash").length());
                    assertEquals(64, rs.getString("source_semantic_hash").length());
                    assertEquals(64, rs.getString("source_file_sha256").length());
                    assertNull(rs.getString("legacy_source_semantic_hash"));
                    assertEquals(DetectionRowParser.SEMANTIC_HASH_VERSION, rs.getString("source_semantic_hash_version"));
                    assertEquals("CURRENT", rs.getString("source_semantic_hash_status"));
                }
            }

            // 2. Upgrade V4 -> V6 com dados preexistentes isolando hash legado e permitindo revisao semantica em nova bbox
            Flyway.configure().dataSource(v4UpgradeUrl, user, pass).locations("classpath:db/migration").target("4").load().migrate();
            String rawCoordLine = "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,12.5,N";
            String rawCoordRow = rawCoordLine + "\n";
            String v4Csv = header + rawCoordRow;
            String v4LegacyHash = legacyV4BuggySemanticHash(
                    "MODIS", "A", "2026-11-12", "2026-11-12T07:09:00Z",
                    -3.500, -53.500, "310.0", "290.0", "1.0", "1.0", "12.5",
                    "80", "h", "N", "6.1NRT", "MODIS_C6.1", "local:v4_to_v6.csv");
            try (Connection c = java.sql.DriverManager.getConnection(v4UpgradeUrl, user, pass);
                 var ps = c.prepareStatement("""
                         INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon,
                                                brightness, bright_t31, scan, track, frp,
                                                confidence_raw, confidence_class, daynight, version,
                                                source_product, source_file, source_line_number,
                                                source_row_hash, source_semantic_hash, source_file_sha256)
                         VALUES ('MODIS', 'A', '2026-11-12'::date, '2026-11-12T07:09:00Z'::timestamptz, -3.500, -53.500,
                                 310.0, 290.0, 1.0, 1.0, 12.5,
                                 '80', 'h', 'N', '6.1NRT', 'MODIS_C6.1', 'local:v4_to_v6.csv', 2,
                                 ?, ?, ?)
                         """)) {
                ps.setString(1, sha256HexIndependent(rawCoordLine));
                ps.setString(2, v4LegacyHash);
                ps.setString(3, sha256HexIndependent(v4Csv));
                ps.executeUpdate();
            }

            var v4ToV6 = Flyway.configure().dataSource(v4UpgradeUrl, user, pass).locations("classpath:db/migration").load().migrate();
            assertEquals(2, v4ToV6.migrationsExecuted);
            assertEquals("6", v4ToV6.targetSchemaVersion);

            try (Connection c = java.sql.DriverManager.getConnection(v4UpgradeUrl, user, pass);
                 var st = c.createStatement();
                 var rs = st.executeQuery("""
                         SELECT source_semantic_hash, legacy_source_semantic_hash,
                                source_semantic_hash_version, source_semantic_hash_status
                         FROM detection WHERE source_file = 'local:v4_to_v6.csv'
                         """)) {
                assertTrue(rs.next());
                assertNull(rs.getString("source_semantic_hash"));
                assertEquals(v4LegacyHash, rs.getString("legacy_source_semantic_hash"));
                assertEquals("v1-legacy-unverified", rs.getString("source_semantic_hash_version"));
                assertEquals("LEGACY_UNVERIFIED", rs.getString("source_semantic_hash_status"));
            }

            String headerSwappedCoords = "longitude,latitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
            try (Connection c = java.sql.DriverManager.getConnection(v4UpgradeUrl, user, pass)) {
                c.setAutoCommit(false);
                var ing = IngestionRunner.ingestOnConnection(c,
                        new ByteArrayInputStream((headerSwappedCoords + rawCoordRow).getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:v4_to_v6.csv",
                        new DetectionCsvLoader.Filter(new BBox(-4, -54, -3, -53),
                                LocalDate.of(2026, 11, 12), LocalDate.of(2026, 11, 12)));
                c.commit();
                assertEquals(1, ing.rowsInserted());
                assertEquals(0, ing.rowsDeleted());
                try (var st = c.createStatement();
                     var rs = st.executeQuery("""
                             SELECT count(*) AS total,
                                    count(*) FILTER (WHERE source_semantic_hash_status = 'LEGACY_UNVERIFIED') AS legacy_cnt,
                                    count(*) FILTER (WHERE source_semantic_hash_status = 'CURRENT') AS current_cnt
                             FROM detection WHERE source_file = 'local:v4_to_v6.csv'
                             """)) {
                    assertTrue(rs.next());
                    assertEquals(2L, rs.getLong("total"));
                    assertEquals(1L, rs.getLong("legacy_cnt"));
                    assertEquals(1L, rs.getLong("current_cnt"));
                }
            }
        } finally {
            try (Connection admin = java.sql.DriverManager.getConnection(adminUrl, user, pass);
                 var st = admin.createStatement()) {
                st.execute("DROP DATABASE IF EXISTS " + upgradeDb);
                st.execute("DROP DATABASE IF EXISTS " + v4UpgradeDb);
            }
        }
    }

    @Test
    void n1IncompleteAnd600kPostPeriodCsvDoesNotDeleteFinalDetection() throws Exception {
        String header = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
        String postPeriodLine = "-3.500,-53.500,310.0,1.0,1.0,2026-11-20,0709,A,80,6.1NRT,290.0,10.0,N\n";
        String validFinalLine = "-3.500,-53.500,315.0,1.0,1.0,2026-11-12,0709,A,85,6.1NRT,291.0,42.5,N\n";
        BBox bbox = new BBox(-54, -4, -53, -3);
        LocalDate d = LocalDate.of(2026, 11, 12);
        var filter = new DetectionCsvLoader.Filter(bbox, d, d);

        // 1. Arquivo com 600.005 linhas posteriores ao periodo e 1 deteccao valida no fim: leitura integral carrega sem cortar
        var r1 = IngestionRunner.ingestStream(dataSource,
                largePostPeriodCsvStream(header, postPeriodLine, 600_005, validFinalLine),
                "MODIS_C6.1", "local:n1_unsorted.csv", filter);
        assertFalse(r1.stats().stoppedEarly());
        assertEquals(600_006, r1.stats().scanned());
        assertEquals(1, r1.stats().kept());
        assertEquals(1, r1.rowsInserted());
        assertNotNull(r1.sourceSha256());

        Long existingId = jdbc.queryForObject(
                "SELECT id FROM detection WHERE source_file = 'local:n1_unsorted.csv'", Long.class);
        assertNotNull(existingId);

        // 2. Reingestao da mesma origem preserva o registro final e seu ID
        var r2 = IngestionRunner.ingestStream(dataSource,
                largePostPeriodCsvStream(header, postPeriodLine, 600_005, validFinalLine),
                "MODIS_C6.1", "local:n1_unsorted.csv", filter);
        assertFalse(r2.stats().stoppedEarly());
        assertEquals(0, r2.rowsDeleted());
        assertEquals(1, r2.rowsUnchanged());
        assertEquals(existingId, jdbc.queryForObject(
                "SELECT id FROM detection WHERE source_file = 'local:n1_unsorted.csv'", Long.class));

        // 3. Caminho fail-safe: leitura interrompida (stoppedEarly=true) aborta com rollback e NAO apaga deteccao existente nem grava log
        int logsBefore = jdbc.queryForObject("SELECT count(*) FROM ingestion_log", Integer.class);
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            assertThrows(IngestionRunner.IncompleteSourceReadException.class, () ->
                    IngestionRunner.ingestOnConnection(con,
                            largePostPeriodCsvStream(header, postPeriodLine, 10, validFinalLine),
                            "MODIS_C6.1", "local:n1_unsorted.csv", filter, 5L));
            con.rollback();
        }
        assertEquals(existingId, jdbc.queryForObject(
                "SELECT id FROM detection WHERE source_file = 'local:n1_unsorted.csv'", Long.class));
        assertEquals(logsBefore, jdbc.queryForObject("SELECT count(*) FROM ingestion_log", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid WHERE sensor = 'MODIS'", Integer.class));

        // 4. Interrupcao de stream (IOException) faz rollback completo sem perder dados nem gravar log de sucesso
        InputStream brokenStream = new InputStream() {
            private final byte[] prefix = (header + postPeriodLine).getBytes(StandardCharsets.UTF_8);
            private int pos = 0;

            @Override
            public int read() throws IOException {
                if (pos >= prefix.length) throw new IOException("Conexao S3 interrompida antes do EOF");
                return prefix[pos++] & 0xFF;
            }
        };
        assertThrows(IOException.class, () ->
                IngestionRunner.ingestStream(dataSource, brokenStream, "MODIS_C6.1", "local:n1_unsorted.csv", filter));
        assertEquals(existingId, jdbc.queryForObject(
                "SELECT id FROM detection WHERE source_file = 'local:n1_unsorted.csv'", Long.class));
        assertEquals(logsBefore, jdbc.queryForObject("SELECT count(*) FROM ingestion_log", Integer.class));

        // 5. Diferenciacao: arquivo integralmente lido sem registros elegiveis (somente cabecalho) reconcilia legitimamente e zera a grade
        var emptyComplete = ingestCsvText(header, "MODIS_C6.1", "local:n1_unsorted.csv", bbox, d, d);
        assertFalse(emptyComplete.stats().stoppedEarly());
        assertNotNull(emptyComplete.sourceSha256());
        assertEquals(0, emptyComplete.stats().kept());
        assertEquals(1, emptyComplete.rowsDeleted());
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid", Integer.class));
    }

    @Test
    void n2HeaderReorderingUpdatesScientificValuesWithoutFalselyReportingUnchanged() throws Exception {
        BBox bbox = new BBox(-54, -4, -53, -3);
        LocalDate d = LocalDate.of(2026, 11, 12);
        String headerOriginal = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,frp,confidence,version,bright_t31,daynight\n";
        String rowSameRawText = "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,10,80,6.1NRT,290.0,N\n";

        // 1. Carga inicial
        var r1 = ingestCsvText(headerOriginal + rowSameRawText, "MODIS_C6.1", "local:n2_header.csv", bbox, d, d);
        assertEquals(1, r1.rowsInserted());
        Long idOriginal = jdbc.queryForObject("SELECT id FROM detection WHERE source_file = 'local:n2_header.csv'", Long.class);

        // 2. Mesmo cabecalho + mesma linha -> ID preservado, rowsUnchanged = 1
        var rSame = ingestCsvText(headerOriginal + rowSameRawText, "MODIS_C6.1", "local:n2_header.csv", bbox, d, d);
        assertEquals(0, rSame.rowsDeleted());
        assertEquals(0, rSame.rowsInserted());
        assertEquals(1, rSame.rowsUnchanged());
        assertEquals(idOriginal, jdbc.queryForObject("SELECT id FROM detection WHERE source_file = 'local:n2_header.csv'", Long.class));

        // 3. Cabecalho reordenado + mesma linha textual (frp <-> confidence invertidos) -> DEVE atualizar valores e nao reportar unchanged
        String headerSwapped = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,frp,version,bright_t31,daynight\n";
        var rFlipped = ingestCsvText(headerSwapped + rowSameRawText, "MODIS_C6.1", "local:n2_header.csv", bbox, d, d);
        assertEquals(0, rFlipped.rowsUnchanged(), "Nao pode reportar rowsUnchanged quando valores interpretados mudaram");
        assertEquals(1, rFlipped.rowsDeleted());
        assertEquals(1, rFlipped.rowsInserted());
        Map<String, Object> flippedRow = jdbc.queryForMap(
                "SELECT id, frp, confidence_raw, confidence_class, source_row_hash, source_semantic_hash FROM detection WHERE source_file = 'local:n2_header.csv'");
        Long idFlipped = ((Number) flippedRow.get("id")).longValue();
        assertNotEquals(idOriginal, idFlipped);
        assertEquals(80.0f, ((Number) flippedRow.get("frp")).floatValue());
        assertEquals("10", flippedRow.get("confidence_raw"));
        assertEquals("l", ((String) flippedRow.get("confidence_class")).trim());
        assertEquals(80.0, jdbc.queryForObject(
                "SELECT frp_sum FROM daily_grid WHERE sensor = 'MODIS' AND acq_date = '2026-11-12'", Double.class), 1e-6);

        // 4. Cabecalho reordenado + valores efetivos equivalentes (confidence,frp trocados de volta junto com os valores + formato equivalente + coluna extra) -> PRESERVA ID!
        String headerExtra = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,frp,confidence,version,bright_t31,daynight,instrument\n";
        String equivRow = "-3.5,-53.5,310.00,1.0,1.0,2026-11-12,709,A,80.00,10,6.1NRT,290.0,N,TERRA_MODIS\n";
        var rEquiv = ingestCsvText(headerExtra + equivRow, "MODIS_C6.1", "local:n2_header.csv", bbox, d, d);
        assertEquals(0, rEquiv.rowsDeleted());
        assertEquals(0, rEquiv.rowsInserted());
        assertEquals(1, rEquiv.rowsUnchanged());
        Map<String, Object> equivPersisted = jdbc.queryForMap(
                "SELECT id, frp, confidence_raw, source_row_hash, source_semantic_hash FROM detection WHERE source_file = 'local:n2_header.csv'");
        assertEquals(idFlipped, ((Number) equivPersisted.get("id")).longValue(),
                "Valores efetivos equivalentes na mesma posicao devem preservar o ID");
        assertNotEquals(flippedRow.get("source_row_hash"), equivPersisted.get("source_row_hash"),
                "Hash bruto da linha deve refletir os novos bytes recebidos");
        assertEquals(flippedRow.get("source_semantic_hash"), equivPersisted.get("source_semantic_hash"));

        // 5. Conteudo alterado mantendo origem e numero da linha -> atualiza valor e nao reporta unchanged
        String changedContent = "-3.5,-53.5,310.00,1.0,1.0,2026-11-12,709,A,95.5,10,6.1NRT,290.0,N,TERRA_MODIS\n";
        var rChanged = ingestCsvText(headerExtra + changedContent, "MODIS_C6.1", "local:n2_header.csv", bbox, d, d);
        assertEquals(0, rChanged.rowsUnchanged());
        assertEquals(1, rChanged.rowsDeleted());
        assertEquals(1, rChanged.rowsInserted());
        assertEquals(95.5f, jdbc.queryForObject(
                "SELECT frp FROM detection WHERE source_file = 'local:n2_header.csv'", Float.class));

        // 6. Linhas reordenadas e linhas textualmente identicas em posicoes distintas
        String lineA = "-3.51,-53.51,310.0,1.0,1.0,2026-11-12,0710,A,70,6.1NRT,290.0,15.0,N\n";
        String lineB = "-3.52,-53.52,311.0,1.0,1.0,2026-11-12,0711,A,80,6.1NRT,290.0,25.0,N\n";
        ingestCsvText(headerOriginal.replace("frp,confidence", "confidence,frp") + lineA + lineB,
                "MODIS_C6.1", "local:n2_multi.csv", bbox, d, d);
        String hStd = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
        ingestCsvText(hStd + lineA + lineB, "MODIS_C6.1", "local:n2_multi.csv", bbox, d, d);
        ingestCsvText(hStd + lineB + lineA, "MODIS_C6.1", "local:n2_multi.csv", bbox, d, d);
        assertEquals(2, jdbc.queryForObject(
                "SELECT count(*) FROM detection WHERE source_file = 'local:n2_multi.csv'", Integer.class));

        String dupCsv = hStd + lineA + lineA;
        var rDup1 = ingestCsvText(dupCsv, "MODIS_C6.1", "local:n2_dups.csv", bbox, d, d);
        assertEquals(2, rDup1.rowsInserted());
        List<Long> dupIdsBefore = jdbc.queryForList(
                "SELECT id FROM detection WHERE source_file = 'local:n2_dups.csv' ORDER BY source_line_number", Long.class);
        String dupCsvReorderedHeader = "satellite,acq_date,acq_time,latitude,longitude,brightness,scan,track,confidence,version,bright_t31,frp,daynight\n"
                + "A,2026-11-12,0710,-3.51,-53.51,310.0,1.0,1.0,70,6.1NRT,290.0,15.0,N\n"
                + "A,2026-11-12,0710,-3.51,-53.51,310.0,1.0,1.0,70,6.1NRT,290.0,15.0,N\n";
        var rDup2 = ingestCsvText(dupCsvReorderedHeader, "MODIS_C6.1", "local:n2_dups.csv", bbox, d, d);
        assertEquals(2, rDup2.rowsUnchanged());
        assertEquals(dupIdsBefore, jdbc.queryForList(
                "SELECT id FROM detection WHERE source_file = 'local:n2_dups.csv' ORDER BY source_line_number", Long.class));

        // 7. Falha durante reconciliacao exige rollback integral
        int totalBeforeFail = jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class);
        InputStream failMid = new InputStream() {
            private final byte[] bytes = (hStd + lineA).getBytes(StandardCharsets.UTF_8);
            private int p = 0;

            @Override
            public int read() throws IOException {
                if (p >= bytes.length) throw new IOException("Falha simulada N2");
                return bytes[p++] & 0xFF;
            }
        };
        assertThrows(IOException.class, () ->
                IngestionRunner.ingestStream(dataSource, failMid, "MODIS_C6.1", "local:n2_dups.csv",
                        new DetectionCsvLoader.Filter(bbox, d, d)));
        assertEquals(totalBeforeFail, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(dupIdsBefore, jdbc.queryForList(
                "SELECT id FROM detection WHERE source_file = 'local:n2_dups.csv' ORDER BY source_line_number", Long.class));
    }

    @Test
    void n3StagingCleanupPreservesPermanentHomonymousTables() throws Exception {
        jdbc.execute("DROP TABLE IF EXISTS public.tmp_detection_stage");
        jdbc.execute("DROP TABLE IF EXISTS public.tmp_affected_grid");
        jdbc.execute("CREATE TABLE public.tmp_detection_stage (marker text NOT NULL)");
        jdbc.execute("CREATE TABLE public.tmp_affected_grid (marker text NOT NULL)");
        jdbc.update("INSERT INTO public.tmp_detection_stage VALUES ('perm_stage_marker')");
        jdbc.update("INSERT INTO public.tmp_affected_grid VALUES ('perm_grid_marker')");
        try {
            String csv1 = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n"
                    + "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,12.0,N\n";
            BBox bbox = new BBox(-54, -4, -53, -3);
            LocalDate d = LocalDate.of(2026, 11, 12);
            var res = ingestCsvText(csv1, "MODIS_C6.1", "local:n3_perm.csv", bbox, d, d);
            assertEquals(1, res.rowsInserted());

            // Executa tambem uma segunda ingestao (com remocao + atualizacao de grade) para confirmar pg_temp antes/depois
            String csv2 = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n"
                    + "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,24.0,N\n";
            var res2 = ingestCsvText(csv2, "MODIS_C6.1", "local:n3_perm.csv", bbox, d, d);
            assertEquals(1, res2.rowsDeleted());
            assertEquals(1, res2.rowsInserted());

            assertEquals("perm_stage_marker", jdbc.queryForObject("SELECT marker FROM public.tmp_detection_stage", String.class));
            assertEquals("perm_grid_marker", jdbc.queryForObject("SELECT marker FROM public.tmp_affected_grid", String.class));
            assertEquals(1, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid WHERE sensor = 'MODIS'", Integer.class));
            assertEquals(24.0, jdbc.queryForObject("SELECT coalesce(sum(frp_sum), 0) FROM daily_grid WHERE sensor = 'MODIS'", Double.class), 1e-6);
        } finally {
            jdbc.execute("DROP TABLE IF EXISTS public.tmp_detection_stage");
            jdbc.execute("DROP TABLE IF EXISTS public.tmp_affected_grid");
        }
    }

    @Test
    void n4MultiOriginInSameTransactionIsRejectedWithoutPartialWritesAndConcurrentFlowsDoNotDeadlock() throws Exception {
        String header = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
        String rowA = "-3.55,-53.55,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,10.0,N\n"
                + "-3.65,-53.65,311.0,1.0,1.0,2026-11-12,0710,A,80,6.1NRT,290.0,20.0,N\n";
        String rowB = "-3.65,-53.65,312.0,1.0,1.0,2026-11-12,0711,A,80,6.1NRT,290.0,30.0,N\n"
                + "-3.55,-53.55,313.0,1.0,1.0,2026-11-12,0712,A,80,6.1NRT,290.0,40.0,N\n";
        BBox bbox = new BBox(-54, -4, -53, -3);
        LocalDate d = LocalDate.of(2026, 11, 12);
        var filter = new DetectionCsvLoader.Filter(bbox, d, d);

        // 1. autoCommit=true rejeitado
        try (Connection autoCon = dataSource.getConnection()) {
            assertTrue(autoCon.getAutoCommit());
            assertThrows(IllegalStateException.class, () -> IngestionRunner.ingestOnConnection(
                    autoCon, new ByteArrayInputStream((header + rowA).getBytes(StandardCharsets.UTF_8)),
                    "MODIS_C6.1", "local:auto.csv", filter));
        }

        // 2. Tentativa de multiplas origens na mesma transacao e segunda chamada mesma origem -> rejeitadas e rollback completo
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            try {
                IngestionRunner.ingestOnConnection(con,
                        new ByteArrayInputStream((header + rowA).getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:src_a.csv", filter);
                assertThrows(IllegalStateException.class, () -> IngestionRunner.ingestOnConnection(con,
                        new ByteArrayInputStream((header + rowB).getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:src_b.csv", filter));
                assertThrows(IllegalStateException.class, () -> IngestionRunner.ingestOnConnection(con,
                        new ByteArrayInputStream((header + rowA).getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:src_a.csv", filter));
                con.rollback();
            } catch (Exception e) {
                con.rollback();
                throw e;
            }
        }
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM daily_grid", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM ingestion_log", Integer.class));

        // 3. Concorrencia nos fluxos suportados (duas origens distintas compartilhando multiplas celulas + mesma origem concorrente)
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            for (int round = 0; round < 3; round++) {
                CountDownLatch ready = new CountDownLatch(4);
                CountDownLatch start = new CountDownLatch(1);
                List<Callable<IngestionRunner.IngestResult>> jobs = List.of(
                        () -> { ready.countDown(); start.await(5, TimeUnit.SECONDS); return ingestCsvText(header + rowA, "MODIS_C6.1", "local:n4_a.csv", bbox, d, d); },
                        () -> { ready.countDown(); start.await(5, TimeUnit.SECONDS); return ingestCsvText(header + rowB, "MODIS_C6.1", "local:n4_b.csv", bbox, d, d); },
                        () -> { ready.countDown(); start.await(5, TimeUnit.SECONDS); return ingestCsvText(header + rowA, "MODIS_C6.1", "local:n4_a.csv", bbox, d, d); },
                        () -> { ready.countDown(); start.await(5, TimeUnit.SECONDS); return ingestCsvText(header + rowB, "MODIS_C6.1", "local:n4_b.csv", bbox, d, d); }
                );
                List<Future<IngestionRunner.IngestResult>> futures = jobs.stream().map(pool::submit).toList();
                assertTrue(ready.await(5, TimeUnit.SECONDS));
                start.countDown();
                for (Future<IngestionRunner.IngestResult> f : futures) {
                    f.get(15, TimeUnit.SECONDS);
                }
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(4, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(4, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid WHERE sensor = 'MODIS'", Integer.class));
        assertEquals(100.0, jdbc.queryForObject("SELECT coalesce(sum(frp_sum), 0) FROM daily_grid WHERE sensor = 'MODIS'", Double.class), 1e-6);
    }

    @Test
    void c2f01SemanticRevisionAcrossBboxDoesNotConflictOnRawRowHash() throws Exception {
        BBox bboxLatFirst = new BBox(-54.0, -4.0, -53.0, -3.0);
        BBox bboxLonFirst = new BBox(-4.0, -54.0, -3.0, -53.0);
        LocalDate d = LocalDate.of(2026, 11, 12);

        String sameRawRow = "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,12.5,N\n";
        String headerLatLon = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
        String headerLonLat = "longitude,latitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";

        // 1. Carga inicial em bboxLatFirst
        var r1 = ingestCsvText(headerLatLon + sameRawRow, "MODIS_C6.1", "local:c2f01_coords.csv", bboxLatFirst, d, d);
        assertEquals(1, r1.rowsInserted());
        Long idOriginal = jdbc.queryForObject("SELECT id FROM detection WHERE source_file = 'local:c2f01_coords.csv'", Long.class);

        // 2. Retransmissao identica (idempotencia)
        var rIdem = ingestCsvText(headerLatLon + sameRawRow, "MODIS_C6.1", "local:c2f01_coords.csv", bboxLatFirst, d, d);
        assertEquals(0, rIdem.rowsInserted());
        assertEquals(1, rIdem.rowsUnchanged());
        assertEquals(0, rIdem.rowsDeleted());
        assertEquals(idOriginal, jdbc.queryForObject("SELECT id FROM detection WHERE source_file = 'local:c2f01_coords.csv'", Long.class));

        // 3. Mesmo texto de linha, cabecalho alterado (longitude,latitude) e recarga restrita aa nova bbox -> sem conflito em hash bruto, preserva antiga fora do recorte
        var r2 = ingestCsvText(headerLonLat + sameRawRow, "MODIS_C6.1", "local:c2f01_coords.csv", bboxLonFirst, d, d);
        assertEquals(1, r2.rowsInserted());
        assertEquals(0, r2.rowsUnchanged());
        assertEquals(0, r2.rowsDeleted());
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM detection WHERE source_file = 'local:c2f01_coords.csv'", Integer.class));
        assertEquals(idOriginal, jdbc.queryForObject(
                "SELECT id FROM detection WHERE source_file = 'local:c2f01_coords.csv' AND lon < -50", Long.class));
        Long idNewBbox = jdbc.queryForObject(
                "SELECT id FROM detection WHERE source_file = 'local:c2f01_coords.csv' AND lon > -10", Long.class);

        // 4. Alteracao de cabecalho sem mudanca semantica (colunas/valores reordenados mantendo significados) -> preserva id local
        String headerReorderedEquiv = "longitude,latitude,brightness,scan,track,acq_date,acq_time,satellite,frp,confidence,version,bright_t31,daynight\n";
        String rowReorderedEquiv = "-3.50,-53.50,310.00,1.0,1.0,2026-11-12,709,A,12.50,80,6.1NRT,290.0,N\n";
        var rEquiv = ingestCsvText(headerReorderedEquiv + rowReorderedEquiv, "MODIS_C6.1", "local:c2f01_coords.csv", bboxLonFirst, d, d);
        assertEquals(0, rEquiv.rowsInserted());
        assertEquals(1, rEquiv.rowsUnchanged());
        assertEquals(0, rEquiv.rowsDeleted());
        assertEquals(idNewBbox, jdbc.queryForObject(
                "SELECT id FROM detection WHERE source_file = 'local:c2f01_coords.csv' AND lon > -10", Long.class));

        // 5. Alteracao de coordenadas e FRP dentro da nova bbox -> atualiza sem afetar a antiga fora da bbox
        String rowEdited = "-3.51,-53.51,310.0,1.0,1.0,2026-11-12,0709,A,99.0,80,6.1NRT,290.0,N\n";
        var rEdited = ingestCsvText(headerReorderedEquiv + rowEdited, "MODIS_C6.1", "local:c2f01_coords.csv", bboxLonFirst, d, d);
        assertEquals(1, rEdited.rowsDeleted());
        assertEquals(1, rEdited.rowsInserted());
        assertEquals(0, rEdited.rowsUnchanged());
        assertEquals(idOriginal, jdbc.queryForObject(
                "SELECT id FROM detection WHERE source_file = 'local:c2f01_coords.csv' AND lon < -50", Long.class));
        assertEquals(99.0f, jdbc.queryForObject(
                "SELECT frp FROM detection WHERE source_file = 'local:c2f01_coords.csv' AND lon > -10", Float.class));
        assertEquals(2, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid WHERE sensor = 'MODIS'", Integer.class));
        assertEquals(111.5, jdbc.queryForObject("SELECT coalesce(sum(frp_sum), 0) FROM daily_grid WHERE sensor = 'MODIS'", Double.class), 1e-6);

        // 6. Duas linhas identicas em posicoes distintas + 7. reingestao concorrente + 8. repeticao apos rollback
        String dupLine = "-3.52,-53.52,310.0,1.0,1.0,2026-11-12,0710,A,80,6.1NRT,290.0,15.0,N\n";
        String dupCsv = headerLatLon + dupLine + dupLine;
        var rDup = ingestCsvText(dupCsv, "MODIS_C6.1", "local:c2f01_dups.csv", bboxLatFirst, d, d);
        assertEquals(2, rDup.rowsInserted());

        try (Connection rollbackCon = dataSource.getConnection()) {
            rollbackCon.setAutoCommit(false);
            IngestionRunner.ingestOnConnection(rollbackCon,
                    new ByteArrayInputStream((headerLatLon + dupLine).getBytes(StandardCharsets.UTF_8)),
                    "MODIS_C6.1", "local:c2f01_dups.csv",
                    new DetectionCsvLoader.Filter(bboxLatFirst, d, d));
            rollbackCon.rollback();
        }
        var rAfterRb = ingestCsvText(dupCsv, "MODIS_C6.1", "local:c2f01_dups.csv", bboxLatFirst, d, d);
        assertEquals(2, rAfterRb.rowsUnchanged());
        assertEquals(0, rAfterRb.rowsInserted());
        assertEquals(0, rAfterRb.rowsDeleted());

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);
            Callable<IngestionRunner.IngestResult> c1 = () -> {
                ready.countDown();
                start.await(5, TimeUnit.SECONDS);
                return ingestCsvText(dupCsv, "MODIS_C6.1", "local:c2f01_dups.csv", bboxLatFirst, d, d);
            };
            Callable<IngestionRunner.IngestResult> c2 = () -> {
                ready.countDown();
                start.await(5, TimeUnit.SECONDS);
                return ingestCsvText(dupCsv, "MODIS_C6.1", "local:c2f01_dups.csv", bboxLatFirst, d, d);
            };
            Future<IngestionRunner.IngestResult> f1 = pool.submit(c1);
            Future<IngestionRunner.IngestResult> f2 = pool.submit(c2);
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            f1.get(15, TimeUnit.SECONDS);
            f2.get(15, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM detection WHERE source_file = 'local:c2f01_dups.csv'", Integer.class));
    }

    @Test
    void c2f02CaughtExceptionFollowedByCommitDoesNotPersistPartialIngestion() throws Exception {
        BBox bboxBoth = new BBox(-55.0, -55.0, -3.0, -3.0);
        BBox bboxSecond = new BBox(-4.0, -54.0, -3.0, -53.0);
        LocalDate d = LocalDate.of(2026, 11, 12);

        String header = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
        String row1 = "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,10.0,N\n";
        String row2 = "-53.500,-3.500,310.0,1.0,1.0,2026-11-12,0710,A,80,6.1NRT,290.0,20.0,N\n";
        ingestCsvText(header + row1 + row2, "MODIS_C6.1", "local:c2f02.csv", bboxBoth, d, d);
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(2, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid WHERE sensor = 'MODIS'", Integer.class));
        assertEquals(30.0, jdbc.queryForObject("SELECT coalesce(sum(frp_sum), 0) FROM daily_grid WHERE sensor = 'MODIS'", Double.class), 1e-6);
        int logsBefore = jdbc.queryForObject("SELECT count(*) FROM ingestion_log", Integer.class);

        String row2Replacement = "-53.500,-3.500,310.0,1.0,1.0,2026-11-12,0710,A,80,6.1NRT,290.0,99.0,N\n";

        // 1. Falha Java (IllegalStateException) APOS Step A deletar row2 e Step B inserir substituicao, mas ANTES de concluir daily_grid:
        //    chamador captura excecao e executa commit() -> zero alteracoes parciais confirmadas, mesmo com estado previo valido inserido na mesma TX.
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            try (var ps = con.prepareStatement("""
                    INSERT INTO user_preference (anon_id, prefs) VALUES ('00000000-0000-0000-0000-00000000c202', '{"keep":true}'::jsonb)
                    ON CONFLICT (anon_id) DO UPDATE SET prefs = EXCLUDED.prefs
                    """)) {
                ps.executeUpdate();
            }
            try {
                IngestionTestBridge.ingestOnConnectionWithHook(con,
                        new ByteArrayInputStream((header + row2Replacement).getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:c2f02.csv",
                        new DetectionCsvLoader.Filter(bboxSecond, d, d),
                        () -> {
                            throw new IllegalStateException("Falha Java forcada entre deteccoes e daily_grid");
                        });
                fail("Esperava IllegalStateException");
            } catch (IllegalStateException expected) {
                // Chamador captura e confirma transacao
            }
            // Tentativa de reutilizar a mesma TX apos falha de uma origem tambem continua bloqueada pelo contrato
            assertThrows(IllegalStateException.class, () -> IngestionRunner.ingestOnConnection(
                    con, new ByteArrayInputStream((header + row2Replacement).getBytes(StandardCharsets.UTF_8)),
                    "MODIS_C6.1", "local:c2f02_other.csv",
                    new DetectionCsvLoader.Filter(bboxSecond, d, d)));
            con.commit();
        }
        assertEquals(1, jdbc.queryForObject(
                "SELECT count(*) FROM user_preference WHERE anon_id = '00000000-0000-0000-0000-00000000c202'", Integer.class));
        jdbc.update("DELETE FROM user_preference WHERE anon_id = '00000000-0000-0000-0000-00000000c202'");
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(20.0f, jdbc.queryForObject("SELECT frp FROM detection WHERE lat < -50", Float.class));
        assertEquals(2, jdbc.queryForObject("SELECT coalesce(sum(n), 0) FROM daily_grid WHERE sensor = 'MODIS'", Integer.class));
        assertEquals(30.0, jdbc.queryForObject("SELECT coalesce(sum(frp_sum), 0) FROM daily_grid WHERE sensor = 'MODIS'", Double.class), 1e-6);
        assertEquals(logsBefore, jdbc.queryForObject("SELECT count(*) FROM ingestion_log", Integer.class));

        // 2. SQLException durante atualizacao de daily_grid -> captura + commit() preserva atomicidade
        jdbc.execute("""
                CREATE OR REPLACE FUNCTION fail_daily_grid_write() RETURNS trigger AS $$
                BEGIN
                    RAISE EXCEPTION 'Erro SQL simulado ao atualizar daily_grid';
                END;
                $$ LANGUAGE plpgsql;
                CREATE TRIGGER trg_fail_daily_grid BEFORE INSERT ON daily_grid
                FOR EACH STATEMENT EXECUTE FUNCTION fail_daily_grid_write();
                """);
        try {
            try (Connection con = dataSource.getConnection()) {
                con.setAutoCommit(false);
                try {
                    IngestionRunner.ingestOnConnection(con,
                            new ByteArrayInputStream((header + row2Replacement).getBytes(StandardCharsets.UTF_8)),
                            "MODIS_C6.1", "local:c2f02.csv",
                            new DetectionCsvLoader.Filter(bboxSecond, d, d));
                    fail("Esperava SQLException na grade");
                } catch (SQLException expected) {
                    // Chamador captura e tenta commit()
                }
                con.commit();
            }
        } finally {
            jdbc.execute("DROP TRIGGER IF EXISTS trg_fail_daily_grid ON daily_grid; DROP FUNCTION IF EXISTS fail_daily_grid_write();");
        }
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(20.0f, jdbc.queryForObject("SELECT frp FROM detection WHERE lat < -50", Float.class));
        assertEquals(30.0, jdbc.queryForObject("SELECT coalesce(sum(frp_sum), 0) FROM daily_grid WHERE sensor = 'MODIS'", Double.class), 1e-6);
        assertEquals(logsBefore, jdbc.queryForObject("SELECT count(*) FROM ingestion_log", Integer.class));

        // 3. Conflito de unicidade (23505) no Step B (sem ON CONFLICT DO NOTHING) -> captura + commit() preserva estado e desfaz Step A
        var rowConflictParser = new DetectionRowParser(header, "MODIS", "MODIS_C6.1", "local:c2f02_conflict.csv");
        var conflictParsed = rowConflictParser.parse(row2.trim());
        jdbc.update("""
                INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon, brightness, bright_t31, scan, track,
                                       frp, confidence_raw, confidence_class, daynight, version, source_product, source_file,
                                       source_line_number, source_row_hash, source_semantic_hash, source_file_sha256,
                                       source_semantic_hash_version, source_semantic_hash_status)
                VALUES ('MODIS', 'A', '2026-11-12', '2026-11-12T07:10:00Z', -53.500, -3.500, 310.0, 290.0, 1.0, 1.0,
                        20.0, '80', 'h', 'N', '6.1NRT', 'MODIS_C6.1', 'local:c2f02_conflict.csv',
                        99, 'dummy_raw_step_a', 'dummy_sem_step_a', 'dummy_file', 'v2-ieee754-direct', 'CURRENT'),
                       ('MODIS', 'A', '2026-11-12', '2026-11-12T07:10:00Z', -3.500, -53.500, 310.0, 290.0, 1.0, 1.0,
                        20.0, '80', 'h', 'N', '6.1NRT', 'LEGACY_PRODUCT', 'local:c2f02_conflict.csv',
                        2, 'dummy_raw_conflict', ?, 'dummy_file', 'v2-ieee754-direct', 'CURRENT')
                """, conflictParsed.sourceSemanticHash());
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            try {
                IngestionRunner.ingestOnConnection(con,
                        new ByteArrayInputStream((header + row2).getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:c2f02_conflict.csv",
                        new DetectionCsvLoader.Filter(bboxSecond, d, d));
                fail("Esperava SQLException 23505 no Step B sem mascarar via ON CONFLICT DO NOTHING");
            } catch (SQLException expected) {
                assertEquals("23505", expected.getSQLState());
            }
            con.commit();
        }
        // Step A havia removido source_line_number = 99 na mesma bbox; o rollback do Savepoint deve preserva-la apos commit()
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM detection WHERE source_file = 'local:c2f02_conflict.csv'", Integer.class));
        jdbc.update("DELETE FROM detection WHERE source_file = 'local:c2f02_conflict.csv'");

        // 4. IOException durante COPY e IncompleteSourceReadException -> captura + commit() mantem banco intacto
        InputStream ioFail = new InputStream() {
            private final byte[] bytes = (header + row2).getBytes(StandardCharsets.UTF_8);
            private int pos = 0;

            @Override
            public int read() throws IOException {
                if (pos >= bytes.length) throw new IOException("Falha IO S3 mid-copy");
                return bytes[pos++] & 0xFF;
            }
        };
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            try {
                IngestionRunner.ingestOnConnection(con, ioFail, "MODIS_C6.1", "local:c2f02.csv",
                        new DetectionCsvLoader.Filter(bboxBoth, d, d));
                fail("Esperava IOException");
            } catch (IOException expected) {
                // catch + commit
            }
            con.commit();
        }
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            String postRow = "-53.500,-3.500,310.0,1.0,1.0,2026-11-20,0710,A,80,6.1NRT,290.0,20.0,N\n";
            try {
                IngestionRunner.ingestOnConnection(con,
                        largePostPeriodCsvStream(header, postRow, 10, row2Replacement),
                        "MODIS_C6.1", "local:c2f02.csv",
                        new DetectionCsvLoader.Filter(bboxSecond, d, d), 5L);
                fail("Esperava IncompleteSourceReadException");
            } catch (IngestionRunner.IncompleteSourceReadException expected) {
                // catch + commit
            }
            con.commit();
        }
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(30.0, jdbc.queryForObject("SELECT coalesce(sum(frp_sum), 0) FROM daily_grid WHERE sensor = 'MODIS'", Double.class), 1e-6);
        assertEquals(logsBefore, jdbc.queryForObject("SELECT count(*) FROM ingestion_log", Integer.class));
    }

    @Test
    void c2f03PostgreSqlRealBitsMatchSemanticHashCanonicalization() throws Exception {
        BBox bbox = new BBox(-54, -4, -53, -3);
        LocalDate d = LocalDate.of(2026, 11, 12);
        String header = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";

        List<String> frpInputs = List.of(
                "1.0",
                "1.000000059604644775390625",
                "1.0000000596046447753906251",
                "1.000000178813934326171875",
                "1e-40",
                "0.0",
                ""
        );

        StringBuilder csv = new StringBuilder(header);
        for (int i = 0; i < frpInputs.size(); i++) {
            csv.append("-3.500,-53.500,310.0,1.0,1.0,2026-11-12,070").append(i).append(",A,80,6.1NRT,290.0,")
                    .append(frpInputs.get(i)).append(",N\n");
        }

        var res = ingestCsvText(csv.toString(), "MODIS_C6.1", "local:c2f03_pg_bits.csv", bbox, d, d);
        assertEquals(frpInputs.size(), res.rowsInserted());

        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT source_line_number,
                       encode(float4send(frp), 'hex') AS pg_hex,
                       source_semantic_hash
                FROM detection
                WHERE source_file = 'local:c2f03_pg_bits.csv'
                ORDER BY source_line_number
                """);
        assertEquals(frpInputs.size(), rows.size());

        // Compara bits float32 persistidos no PostgreSQL contra Float.parseFloat direto (sem double intermediario)
        for (int i = 0; i < frpInputs.size() - 1; i++) {
            float expectedFloat = Float.parseFloat(frpInputs.get(i));
            String expectedHex = String.format("%08x", Float.floatToRawIntBits(expectedFloat));
            assertEquals(expectedHex, rows.get(i).get("pg_hex"), "Bits PG divergem para " + frpInputs.get(i));
        }
        assertEquals("3f800000", rows.get(0).get("pg_hex"));
        assertEquals("3f800000", rows.get(1).get("pg_hex"));
        assertEquals("3f800001", rows.get(2).get("pg_hex"));
        assertEquals("3f800002", rows.get(3).get("pg_hex"));
        assertEquals("000116c2", rows.get(4).get("pg_hex"));
        assertEquals("00000000", rows.get(5).get("pg_hex"));
        assertNull(rows.get(6).get("pg_hex"));

        // Agora altera somente acq_time para igual entre 1.0 e 1.0000000596046447753906251 na mesma linha -> hashes semanticos DEVEM diferir
        String oneRow = header + "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.0,N\n";
        String advRow = header + "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.0000000596046447753906251,N\n";
        ingestCsvText(oneRow, "MODIS_C6.1", "local:c2f03_one.csv", bbox, d, d);
        ingestCsvText(advRow, "MODIS_C6.1", "local:c2f03_adv.csv", bbox, d, d);
        String semOne = jdbc.queryForObject("SELECT source_semantic_hash FROM detection WHERE source_file = 'local:c2f03_one.csv'", String.class);
        String semAdv = jdbc.queryForObject("SELECT source_semantic_hash FROM detection WHERE source_file = 'local:c2f03_adv.csv'", String.class);
        assertNotEquals(semOne, semAdv);
    }

    @Test
    void c2f04SourceTraceabilityDistinguishesSemanticRevisionsAndLegacyRows() throws Exception {
        BBox bbox = new BBox(-54, -4, -53, -3);
        LocalDate d = LocalDate.of(2026, 11, 12);
        String sameRawRow = "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,10,80,6.1NRT,290.0,N\n";
        String h1 = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,frp,confidence,version,bright_t31,daynight\n";
        String h2 = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,frp,version,bright_t31,daynight\n";

        // 1. Mesmo texto bruto, mesma origem/linha, interpretacao diferente (frp=10/conf=80 vs frp=80/conf=10)
        ingestCsvText(h1 + sameRawRow, "MODIS_C6.1", "local:c2f04_m.csv", bbox, d, d);
        insertDetection("VIIRS", "N20", "2026-11-12", "2026-11-12T07:10:00Z", -3.5005, -53.5005,
                5.0f, "nominal", "n", "N", "2.0NRT", "VIIRS_NOAA-20", "local:c2f04_v_legacy.csv");

        var resp1 = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&includeAssociations=true");
        var modis1 = resp1.getBody().associations().get(0).modis();
        var viirsLegacy = resp1.getBody().associations().get(0).viirs();

        // Legado pre-V3 preserva tudo null sem fabricacao
        assertNull(viirsLegacy.sourceLineNumber());
        assertNull(viirsLegacy.sourceRecordKey());
        assertNull(viirsLegacy.sourceRawLocator());
        assertNull(viirsLegacy.sourceRowHash());
        assertNull(viirsLegacy.sourceSemanticHash());
        assertNull(viirsLegacy.sourceFileSha256());

        ingestCsvText(h2 + sameRawRow, "MODIS_C6.1", "local:c2f04_m.csv", bbox, d, d);
        var resp2 = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&includeAssociations=true");
        var modis2 = resp2.getBody().associations().get(0).modis();

        assertEquals(modis1.sourceRawLocator(), modis2.sourceRawLocator());
        assertEquals(modis1.sourceRowHash(), modis2.sourceRowHash());
        assertNotEquals(modis1.observationKey(), modis2.observationKey());
        assertNotEquals(modis1.sourceSemanticHash(), modis2.sourceSemanticHash());
        assertNotEquals(modis1.sourceFileSha256(), modis2.sourceFileSha256());
        assertNotEquals(modis1.sourceRecordKey(), modis2.sourceRecordKey(),
                "C2-F04: sourceRecordKey deve distinguir interpretacoes semanticas diferentes mesmo quando hash bruto e identico");

        // 2. Interpretacao equivalente com cabecalho reordenado -> mantem observationKey e sourceSemanticHash; atualiza sourceRowHash/sourceFileSha256/sourceRecordKey
        String hEquiv = "longitude,latitude,brightness,scan,track,acq_date,acq_time,satellite,frp,confidence,version,bright_t31,daynight\n";
        String rowEquiv = "-53.5,-3.5,310.00,1.0,1.0,2026-11-12,709,A,80.0,10,6.1NRT,290.0,N\n";
        ingestCsvText(hEquiv + rowEquiv, "MODIS_C6.1", "local:c2f04_m.csv", bbox, d, d);
        var respEquiv = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&includeAssociations=true");
        var modisEquiv = respEquiv.getBody().associations().get(0).modis();
        assertEquals(modis2.observationKey(), modisEquiv.observationKey());
        assertEquals(modis2.sourceSemanticHash(), modisEquiv.sourceSemanticHash());
        assertNotEquals(modis2.sourceRowHash(), modisEquiv.sourceRowHash());
        assertNotEquals(modis2.sourceFileSha256(), modisEquiv.sourceFileSha256());
        assertNotEquals(modis2.sourceRecordKey(), modisEquiv.sourceRecordKey());

        // 3. Registro V3 parcial (apenas raw hash, sem semantic hash) nao inventa hash semantico
        jdbc.update("""
                INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon, frp,
                                       confidence_raw, confidence_class, daynight, version, source_product, source_file,
                                       source_line_number, source_row_hash, source_semantic_hash, source_file_sha256)
                VALUES ('VIIRS', 'N21', '2026-11-12', '2026-11-12T07:11:00Z', -3.5008, -53.5008, 7.5,
                        'high', 'h', 'N', '2.0NRT', 'VIIRS_NOAA-21', 'local:c2f04_v3_only.csv',
                        9, 'abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789', NULL, NULL)
                """);
        var respV3 = getHarmonized("bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&includeAssociations=true");
        var v3Obs = respV3.getBody().associations().stream()
                .map(Dto.HarmonizationResponse.CandidateAssociation::viirs)
                .filter(v -> "local:c2f04_v3_only.csv".equals(v.sourceFile()))
                .findFirst()
                .orElseThrow();
        assertEquals("local:c2f04_v3_only.csv#L9", v3Obs.sourceRawLocator());
        assertEquals("local:c2f04_v3_only.csv#L9:raw=abcdef012345", v3Obs.sourceRecordKey());
        assertNull(v3Obs.sourceSemanticHash());
        assertNull(v3Obs.sourceFileSha256());
        assertEquals("UNKNOWN", v3Obs.sourceSemanticHashStatus());
        assertNull(v3Obs.sourceSemanticHashVersion());
        assertNull(v3Obs.legacySourceSemanticHash());
    }

    private static InputStream largePostPeriodCsvStream(String header, String postLine, int postCount, String finalLine) {
        byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
        byte[] postBytes = postLine.getBytes(StandardCharsets.UTF_8);
        byte[] finalBytes = finalLine.getBytes(StandardCharsets.UTF_8);
        return new InputStream() {
            private int phase = 0; // 0=header, 1=postLines, 2=finalLine, 3=eof
            private int offset = 0;
            private int remainingPost = postCount;

            @Override
            public int read(byte[] b, int off, int len) {
                if (len == 0) return 0;
                int total = 0;
                while (total < len && phase < 3) {
                    byte[] cur = phase == 0 ? headerBytes : (phase == 1 ? postBytes : finalBytes);
                    int avail = cur.length - offset;
                    if (avail <= 0) {
                        offset = 0;
                        if (phase == 0) {
                            phase = remainingPost > 0 ? 1 : 2;
                        } else if (phase == 1) {
                            if (--remainingPost <= 0) phase = 2;
                        } else {
                            phase = 3;
                        }
                        continue;
                    }
                    int copy = Math.min(avail, len - total);
                    System.arraycopy(cur, offset, b, off + total, copy);
                    offset += copy;
                    total += copy;
                }
                return total == 0 ? -1 : total;
            }

            @Override
            public int read() {
                byte[] one = new byte[1];
                int n = read(one, 0, 1);
                return n == -1 ? -1 : (one[0] & 0xFF);
            }
        };
    }

    private void loadFixture(String classpathPath, String product, String sourceKey) throws Exception {
        var resource = new ClassPathResource(classpathPath);
        try (InputStream in = resource.getInputStream()) {
            var res = IngestionRunner.ingestStream(dataSource, in, product, sourceKey,
                    new DetectionCsvLoader.Filter(new BBox(-54, -4, -53, -3),
                            LocalDate.of(2026, 11, 12), LocalDate.of(2026, 11, 13)));
            assertEquals(0, res.stats().malformed());
            assertTrue(res.stats().kept() > 0);
            assertNotNull(res.sourceSha256());
        }
    }

    @Test
    void c3f01HistoricalV4Float32LegacyHashUpgradeAndSafeReconciliation() throws Exception {
        String baseUrl = System.getProperty("test.db.url",
                System.getenv().getOrDefault("TEST_DB_URL", DEFAULT_TEST_DB_URL));
        String adminUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/')) + "/postgres";
        String legacyDb = "fireatlas_c3f01_upgrade_test";
        String legacyUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/')) + "/" + legacyDb;
        String user = System.getProperty("test.db.user", "fireatlas");
        String pass = System.getProperty("test.db.password", "fireatlas");

        try (Connection admin = java.sql.DriverManager.getConnection(adminUrl, user, pass);
             var st = admin.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS " + legacyDb);
            st.execute("CREATE DATABASE " + legacyDb);
        }
        try {
            // 1. Setup realista em V4 antes da V5/V6
            Flyway.configure().dataSource(legacyUrl, user, pass).locations("classpath:db/migration").target("4").load().migrate();

            String modisHeader = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
            String lineFrpEdge = "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.0000000596046447753906251,N";
            String lineNormal = "-3.510,-53.510,312.0,1.0,1.0,2026-11-12,0710,A,82,6.1NRT,291.0,12.5,N";
            String lineOutBbox = "-13.500,-63.500,315.0,1.0,1.0,2026-11-12,0711,A,85,6.1NRT,292.0,18.0,N";
            String mainCsv = modisHeader + lineFrpEdge + "\n" + lineNormal + "\n" + lineOutBbox + "\n";
            String mainFileSha = sha256HexIndependent(mainCsv);

            String legacyHashEdge = legacyV4BuggySemanticHash(
                    "MODIS", "A", "2026-11-12", "2026-11-12T07:09:00Z",
                    -3.500, -53.500, "310.0", "290.0", "1.0", "1.0", "1.0000000596046447753906251",
                    "80", "h", "N", "6.1NRT", "MODIS_C6.1", "local:c3f01_main.csv");
            String legacyHashNormal = legacyV4BuggySemanticHash(
                    "MODIS", "A", "2026-11-12", "2026-11-12T07:10:00Z",
                    -3.510, -53.510, "312.0", "291.0", "1.0", "1.0", "12.5",
                    "82", "h", "N", "6.1NRT", "MODIS_C6.1", "local:c3f01_main.csv");
            String legacyHashOutBbox = legacyV4BuggySemanticHash(
                    "MODIS", "A", "2026-11-12", "2026-11-12T07:11:00Z",
                    -13.500, -63.500, "315.0", "292.0", "1.0", "1.0", "18.0",
                    "85", "h", "N", "6.1NRT", "MODIS_C6.1", "local:c3f01_main.csv");

            // Verifica previamente que frp = 1.0000000596046447753906251 gera hash v2-ieee754-direct distinto do algoritmo V4 double->float
            var parserMain = new DetectionRowParser(modisHeader, "MODIS", "MODIS_C6.1", "local:c3f01_main.csv");
            String currentV2HashEdge = parserMain.parse(lineFrpEdge).sourceSemanticHash();
            String currentV2HashNormal = parserMain.parse(lineNormal).sourceSemanticHash();
            assertNotEquals(legacyHashEdge, currentV2HashEdge);
            assertEquals(legacyHashNormal, currentV2HashNormal);

            // Arquivo com 2 linhas identicas em texto bruto em linhas diferentes (L2 e L3) com assinatura V4 antiga
            String dupLine = "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0712,A,80,6.1NRT,290.0,1.0000000596046447753906251,N";
            String dupsCsv = modisHeader + dupLine + "\n" + dupLine + "\n";
            String dupsFileSha = sha256HexIndependent(dupsCsv);
            String legacyHashDup = legacyV4BuggySemanticHash(
                    "MODIS", "A", "2026-11-12", "2026-11-12T07:12:00Z",
                    -3.500, -53.500, "310.0", "290.0", "1.0", "1.0", "1.0000000596046447753906251",
                    "80", "h", "N", "6.1NRT", "MODIS_C6.1", "local:c3f01_dups.csv");

            // Arquivo V4 cujo header tera frp/bright_t31 alterados ou arquivo modificado posteriormente
            String modLine = "-3.520,-53.520,310.0,1.0,1.0,2026-11-12,0715,A,80,6.1NRT,290.0,15.0,N";
            String fileModOriginalCsv = modisHeader + modLine + "\n";
            String legacyHashFileMod = legacyV4BuggySemanticHash(
                    "MODIS", "A", "2026-11-12", "2026-11-12T07:15:00Z",
                    -3.520, -53.520, "310.0", "290.0", "1.0", "1.0", "15.0",
                    "80", "h", "N", "6.1NRT", "MODIS_C6.1", "local:c3f01_filemod.csv");

            // VIIRS companheiro no mesmo ponto para habilitar /api/v1/harmonized
            String viirsHeader = "latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,confidence,version,bright_ti5,frp,daynight\n";
            String viirsLine = "-3.5002,-53.5002,328.0,0.4,0.4,2026-11-12,0714,N20,high,2.0NRT,294.0,9.5,N";
            String viirsCsv = viirsHeader + viirsLine + "\n";
            String legacyHashViirs = legacyV4BuggySemanticHash(
                    "VIIRS", "N20", "2026-11-12", "2026-11-12T07:14:00Z",
                    -3.5002, -53.5002, "328.0", "294.0", "0.4", "0.4", "9.5",
                    "high", "h", "N", "2.0NRT", "VIIRS_NOAA-20", "local:c3f01_viirs.csv");

            long origIdEdge;
            long origIdNormal;
            long origIdOutBbox;
            long origIdDup1;
            long origIdDup2;
            long origIdFileMod;
            long origIdViirs;
            long origIdPreV3;

            try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass)) {
                origIdEdge = insertV4Detection(c, "MODIS", "A", "2026-11-12", "2026-11-12T07:09:00Z",
                        -3.500, -53.500, 310.0, 290.0, 1.0, 1.0, 1.0000000596046447753906251,
                        "80", "h", "N", "6.1NRT", "MODIS_C6.1", "local:c3f01_main.csv", 2,
                        sha256HexIndependent(lineFrpEdge), legacyHashEdge, mainFileSha);
                origIdNormal = insertV4Detection(c, "MODIS", "A", "2026-11-12", "2026-11-12T07:10:00Z",
                        -3.510, -53.510, 312.0, 291.0, 1.0, 1.0, 12.5,
                        "82", "h", "N", "6.1NRT", "MODIS_C6.1", "local:c3f01_main.csv", 3,
                        sha256HexIndependent(lineNormal), legacyHashNormal, mainFileSha);
                origIdOutBbox = insertV4Detection(c, "MODIS", "A", "2026-11-12", "2026-11-12T07:11:00Z",
                        -13.500, -63.500, 315.0, 292.0, 1.0, 1.0, 18.0,
                        "85", "h", "N", "6.1NRT", "MODIS_C6.1", "local:c3f01_main.csv", 4,
                        sha256HexIndependent(lineOutBbox), legacyHashOutBbox, mainFileSha);
                origIdDup1 = insertV4Detection(c, "MODIS", "A", "2026-11-12", "2026-11-12T07:12:00Z",
                        -3.500, -53.500, 310.0, 290.0, 1.0, 1.0, 1.0000000596046447753906251,
                        "80", "h", "N", "6.1NRT", "MODIS_C6.1", "local:c3f01_dups.csv", 2,
                        sha256HexIndependent(dupLine), legacyHashDup, dupsFileSha);
                origIdDup2 = insertV4Detection(c, "MODIS", "A", "2026-11-12", "2026-11-12T07:12:00Z",
                        -3.500, -53.500, 310.0, 290.0, 1.0, 1.0, 1.0000000596046447753906251,
                        "80", "h", "N", "6.1NRT", "MODIS_C6.1", "local:c3f01_dups.csv", 3,
                        sha256HexIndependent(dupLine), legacyHashDup, dupsFileSha);
                origIdFileMod = insertV4Detection(c, "MODIS", "A", "2026-11-12", "2026-11-12T07:15:00Z",
                        -3.520, -53.520, 310.0, 290.0, 1.0, 1.0, 15.0,
                        "80", "h", "N", "6.1NRT", "MODIS_C6.1", "local:c3f01_filemod.csv", 2,
                        sha256HexIndependent(modLine), legacyHashFileMod, sha256HexIndependent(fileModOriginalCsv));
                origIdViirs = insertV4Detection(c, "VIIRS", "N20", "2026-11-12", "2026-11-12T07:14:00Z",
                        -3.5002, -53.5002, 328.0, 294.0, 0.4, 0.4, 9.5,
                        "high", "h", "N", "2.0NRT", "VIIRS_NOAA-20", "local:c3f01_viirs.csv", 2,
                        sha256HexIndependent(viirsLine), legacyHashViirs, sha256HexIndependent(viirsCsv));
                // Pre-V3 sem provenance
                try (var ps = c.prepareStatement("""
                        INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon,
                                               frp, confidence_raw, confidence_class, daynight, version,
                                               source_product, source_file)
                        VALUES ('MODIS', 'T', '2026-11-12'::date, '2026-11-12T07:05:00Z'::timestamptz, -3.550, -53.550,
                                5.0, '70', 'n', 'N', '6.1NRT', 'MODIS_C6.1', 'local:c3f01_pre_v3.csv')
                        RETURNING id
                        """)) {
                    var rs = ps.executeQuery();
                    assertTrue(rs.next());
                    origIdPreV3 = rs.getLong(1);
                }

                // Popula daily_grid inicial coerente com V4
                try (var st = c.createStatement()) {
                    st.execute("""
                            INSERT INTO daily_grid (acq_date, sensor, cx, cy, n, frp_sum, frp_max)
                            SELECT acq_date,
                                   sensor,
                                   GREATEST(0, LEAST(floor((lon + 180) * 10)::int, 3599)),
                                   GREATEST(0, LEAST(floor((lat + 90) * 10)::int, 1799)),
                                   count(*)::int,
                                   coalesce(sum(frp), 0),
                                   max(frp)
                            FROM detection
                            GROUP BY 1, 2, 3, 4
                            """);
                }
            }

            // 2. Snapshot antes do upgrade V4 -> V5 -> V6
            int frpBitsBeforeEdge;
            double gridSumBeforeModis;
            int gridCountBefore;
            try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass);
                 var st = c.createStatement()) {
                var rsBits = st.executeQuery("SELECT frp FROM detection WHERE id = " + origIdEdge);
                assertTrue(rsBits.next());
                frpBitsBeforeEdge = Float.floatToRawIntBits(rsBits.getFloat(1));
                assertEquals(0x3f800001, frpBitsBeforeEdge, "PostgreSQL real deve persistir bits exatos 3f800001");

                var rsGrid = st.executeQuery("SELECT count(*), sum(frp_sum) FROM daily_grid WHERE sensor = 'MODIS'");
                assertTrue(rsGrid.next());
                gridCountBefore = rsGrid.getInt(1);
                gridSumBeforeModis = rsGrid.getDouble(2);
            }

            // 3. Upgrade escalonado V4 -> V5 -> V6 (sem colisao nem alteracao de valores cientificos)
            var toV5 = Flyway.configure().dataSource(legacyUrl, user, pass).locations("classpath:db/migration").target("5").load().migrate();
            assertEquals(1, toV5.migrationsExecuted);
            assertEquals("5", toV5.targetSchemaVersion);

            var toV6 = Flyway.configure().dataSource(legacyUrl, user, pass).locations("classpath:db/migration").load().migrate();
            assertEquals(1, toV6.migrationsExecuted);
            assertEquals("6", toV6.targetSchemaVersion);

            // 4. Verificacao pos-upgrade V6 antes de qualquer retransmissao
            try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass);
                 var st = c.createStatement()) {
                var rs = st.executeQuery("""
                        SELECT id, source_semantic_hash, legacy_source_semantic_hash,
                               source_semantic_hash_version, source_semantic_hash_status, frp
                        FROM detection
                        WHERE id = %d
                        """.formatted(origIdEdge));
                assertTrue(rs.next());
                assertNull(rs.getString("source_semantic_hash"));
                assertEquals(legacyHashEdge, rs.getString("legacy_source_semantic_hash"));
                assertEquals("v1-legacy-unverified", rs.getString("source_semantic_hash_version"));
                assertEquals("LEGACY_UNVERIFIED", rs.getString("source_semantic_hash_status"));
                assertEquals(frpBitsBeforeEdge, Float.floatToRawIntBits(rs.getFloat("frp")));

                var rsUnknown = st.executeQuery("""
                        SELECT source_semantic_hash, legacy_source_semantic_hash,
                               source_semantic_hash_version, source_semantic_hash_status
                        FROM detection WHERE id = %d
                        """.formatted(origIdPreV3));
                assertTrue(rsUnknown.next());
                assertNull(rsUnknown.getString("source_semantic_hash"));
                assertNull(rsUnknown.getString("legacy_source_semantic_hash"));
                assertNull(rsUnknown.getString("source_semantic_hash_version"));
                assertEquals("UNKNOWN", rsUnknown.getString("source_semantic_hash_status"));
            }

            // 5. Verifica resposta HTTP JSON /api/v1/harmonized ANTES da retransmissao (LEGACY_UNVERIFIED nao exposto em sourceSemanticHash)
            MockMvc standaloneMvc = createStandaloneHarmonizationMockMvc(legacyUrl, user, pass);
            String beforeJsonStr = standaloneMvc.perform(
                            get("/api/v1/harmonized")
                                    .param("bbox", "-54,-4,-53,-3")
                                    .param("start", "2026-11-12")
                                    .param("end", "2026-11-12")
                                    .param("includeAssociations", "true"))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse()
                    .getContentAsString(StandardCharsets.UTF_8);
            JsonNode beforeJson = objectMapper.readTree(beforeJsonStr);
            assertTrue(beforeJson.path("associations").size() >= 1);
            JsonNode edgeAssocBefore = findAssociationByModisId(beforeJson, origIdEdge);
            JsonNode modisBefore = edgeAssocBefore.path("modis");
            assertEquals("LEGACY_UNVERIFIED", modisBefore.path("sourceSemanticHashStatus").asText());
            assertEquals("v1-legacy-unverified", modisBefore.path("sourceSemanticHashVersion").asText());
            assertTrue(modisBefore.has("sourceSemanticHash") && modisBefore.get("sourceSemanticHash").isNull(),
                    "Hash legado nao verificado JAMAIS deve ser exposto em sourceSemanticHash");
            assertEquals(legacyHashEdge, modisBefore.path("legacySourceSemanticHash").asText());
            String lineFrpEdgeShort = sha256HexIndependent(lineFrpEdge).substring(0, 12);
            assertEquals("local:c3f01_main.csv#L2:raw=" + lineFrpEdgeShort + ":legacySem=" + legacyHashEdge.substring(0, 12),
                    modisBefore.path("sourceRecordKey").asText());
            assertTrue(reasonsContain(beforeJson, "LEGACY_UNVERIFIED"));

            // 6. Retransmissao byte a byte identica para local:c3f01_main.csv -> reconcilia sem delete/insert nem troca de ID
            try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass)) {
                c.setAutoCommit(false);
                var res = IngestionRunner.ingestOnConnection(c,
                        new ByteArrayInputStream(mainCsv.getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:c3f01_main.csv",
                        new DetectionCsvLoader.Filter(new BBox(-54, -4, -53, -3),
                                LocalDate.of(2026, 11, 12), LocalDate.of(2026, 11, 12)));
                c.commit();
                assertEquals(0L, res.rowsInserted());
                assertEquals(0L, res.rowsDeleted());
                assertEquals(2L, res.rowsUnchanged());
            }

            // Verifica linhas duplicadas (dups.csv) em retransmissao identica
            try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass)) {
                c.setAutoCommit(false);
                var resDups = IngestionRunner.ingestOnConnection(c,
                        new ByteArrayInputStream(dupsCsv.getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:c3f01_dups.csv",
                        new DetectionCsvLoader.Filter(new BBox(-54, -4, -53, -3),
                                LocalDate.of(2026, 11, 12), LocalDate.of(2026, 11, 12)));
                c.commit();
                assertEquals(0L, resDups.rowsInserted());
                assertEquals(0L, resDups.rowsDeleted());
                assertEquals(2L, resDups.rowsUnchanged());
            }

            // Confirma IDs originais preservados, status UPDATED_FROM_LEGACY vs CURRENT, frp float32 bits, out-of-bbox intacto e grid intacto
            try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass);
                 var st = c.createStatement()) {
                var rsEdge = st.executeQuery("""
                        SELECT id, frp, source_semantic_hash, legacy_source_semantic_hash,
                               source_semantic_hash_version, source_semantic_hash_status
                        FROM detection WHERE source_file = 'local:c3f01_main.csv' AND source_line_number = 2
                        """);
                assertTrue(rsEdge.next());
                assertEquals(origIdEdge, rsEdge.getLong("id"));
                assertEquals(frpBitsBeforeEdge, Float.floatToRawIntBits(rsEdge.getFloat("frp")));
                assertEquals(currentV2HashEdge, rsEdge.getString("source_semantic_hash"));
                assertEquals(legacyHashEdge, rsEdge.getString("legacy_source_semantic_hash"));
                assertEquals(DetectionRowParser.SEMANTIC_HASH_VERSION, rsEdge.getString("source_semantic_hash_version"));
                assertEquals("UPDATED_FROM_LEGACY", rsEdge.getString("source_semantic_hash_status"));
                assertFalse(rsEdge.next());

                var rsNorm = st.executeQuery("""
                        SELECT id, source_semantic_hash, legacy_source_semantic_hash,
                               source_semantic_hash_version, source_semantic_hash_status
                        FROM detection WHERE source_file = 'local:c3f01_main.csv' AND source_line_number = 3
                        """);
                assertTrue(rsNorm.next());
                assertEquals(origIdNormal, rsNorm.getLong("id"));
                assertEquals(currentV2HashNormal, rsNorm.getString("source_semantic_hash"));
                assertEquals(legacyHashNormal, rsNorm.getString("legacy_source_semantic_hash"));
                assertEquals(DetectionRowParser.SEMANTIC_HASH_VERSION, rsNorm.getString("source_semantic_hash_version"));
                assertEquals("CURRENT", rsNorm.getString("source_semantic_hash_status"));
                assertFalse(rsNorm.next());

                var rsOut = st.executeQuery("""
                        SELECT id, source_semantic_hash, legacy_source_semantic_hash,
                               source_semantic_hash_version, source_semantic_hash_status
                        FROM detection WHERE source_file = 'local:c3f01_main.csv' AND source_line_number = 4
                        """);
                assertTrue(rsOut.next());
                assertEquals(origIdOutBbox, rsOut.getLong("id"));
                assertNull(rsOut.getString("source_semantic_hash"));
                assertEquals(legacyHashOutBbox, rsOut.getString("legacy_source_semantic_hash"));
                assertEquals("v1-legacy-unverified", rsOut.getString("source_semantic_hash_version"));
                assertEquals("LEGACY_UNVERIFIED", rsOut.getString("source_semantic_hash_status"));

                var rsDups = st.executeQuery(
                        "SELECT id, source_line_number, source_semantic_hash_status FROM detection WHERE source_file = 'local:c3f01_dups.csv' ORDER BY source_line_number");
                assertTrue(rsDups.next());
                assertEquals(origIdDup1, rsDups.getLong("id"));
                assertEquals("UPDATED_FROM_LEGACY", rsDups.getString("source_semantic_hash_status"));
                assertTrue(rsDups.next());
                assertEquals(origIdDup2, rsDups.getLong("id"));
                assertEquals("UPDATED_FROM_LEGACY", rsDups.getString("source_semantic_hash_status"));
                assertFalse(rsDups.next());

                var rsGridAfter = st.executeQuery("SELECT count(*), sum(frp_sum) FROM daily_grid WHERE sensor = 'MODIS'");
                assertTrue(rsGridAfter.next());
                assertEquals(gridCountBefore, rsGridAfter.getInt(1));
                assertEquals(gridSumBeforeModis, rsGridAfter.getDouble(2), 1e-6);

                var rsLogs = st.executeQuery(
                        "SELECT rows_inserted, rows_unchanged, rows_deleted FROM ingestion_log WHERE source_key = 'local:c3f01_main.csv'");
                assertTrue(rsLogs.next());
                assertEquals(0L, rsLogs.getLong("rows_inserted"));
                assertEquals(2L, rsLogs.getLong("rows_unchanged"));
                assertEquals(0L, rsLogs.getLong("rows_deleted"));
            }

            // 7. Verifica resposta HTTP JSON /api/v1/harmonized APOS retransmissao (UPDATED_FROM_LEGACY + legacySourceSemanticHash preservado)
            String afterJsonStr = standaloneMvc.perform(
                            get("/api/v1/harmonized")
                                    .param("bbox", "-54,-4,-53,-3")
                                    .param("start", "2026-11-12")
                                    .param("end", "2026-11-12")
                                    .param("includeAssociations", "true"))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse()
                    .getContentAsString(StandardCharsets.UTF_8);
            JsonNode afterJson = objectMapper.readTree(afterJsonStr);
            JsonNode modisAfter = findAssociationByModisId(afterJson, origIdEdge).path("modis");
            assertEquals("UPDATED_FROM_LEGACY", modisAfter.path("sourceSemanticHashStatus").asText());
            assertEquals(DetectionRowParser.SEMANTIC_HASH_VERSION, modisAfter.path("sourceSemanticHashVersion").asText());
            assertEquals(currentV2HashEdge, modisAfter.path("sourceSemanticHash").asText());
            assertEquals(legacyHashEdge, modisAfter.path("legacySourceSemanticHash").asText());
            assertEquals("local:c3f01_main.csv#L2:raw=" + lineFrpEdgeShort + ":sem=" + currentV2HashEdge.substring(0, 12),
                    modisAfter.path("sourceRecordKey").asText());
            assertTrue(reasonsContain(afterJson, "UPDATED_FROM_LEGACY"));
            assertTrue(reasonsContain(afterJson, "LEGACY_UNVERIFIED"), "VIIRS companion permanece LEGACY_UNVERIFIED");

            // 8. Segunda retransmissao identica + rollback + concorrencia em 2 threads
            try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass)) {
                c.setAutoCommit(false);
                var secondRes = IngestionRunner.ingestOnConnection(c,
                        new ByteArrayInputStream(mainCsv.getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:c3f01_main.csv",
                        new DetectionCsvLoader.Filter(new BBox(-54, -4, -53, -3),
                                LocalDate.of(2026, 11, 12), LocalDate.of(2026, 11, 12)));
                c.rollback(); // testa rollback completo
                assertEquals(0L, secondRes.rowsInserted());
                assertEquals(0L, secondRes.rowsDeleted());
                assertEquals(2L, secondRes.rowsUnchanged());
            }

            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch go = new CountDownLatch(1);
            try {
                Callable<IngestionRunner.IngestResult> worker = () -> {
                    ready.countDown();
                    assertTrue(go.await(10, TimeUnit.SECONDS));
                    try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass)) {
                        c.setAutoCommit(false);
                        var r = IngestionRunner.ingestOnConnection(c,
                                new ByteArrayInputStream(mainCsv.getBytes(StandardCharsets.UTF_8)),
                                "MODIS_C6.1", "local:c3f01_main.csv",
                                new DetectionCsvLoader.Filter(new BBox(-54, -4, -53, -3),
                                        LocalDate.of(2026, 11, 12), LocalDate.of(2026, 11, 12)));
                        c.commit();
                        return r;
                    }
                };
                Future<IngestionRunner.IngestResult> f1 = pool.submit(worker);
                Future<IngestionRunner.IngestResult> f2 = pool.submit(worker);
                assertTrue(ready.await(10, TimeUnit.SECONDS));
                go.countDown();
                var r1 = f1.get(30, TimeUnit.SECONDS);
                var r2 = f2.get(30, TimeUnit.SECONDS);
                assertEquals(2L, r1.rowsUnchanged());
                assertEquals(2L, r2.rowsUnchanged());
                assertEquals(0L, r1.rowsInserted() + r2.rowsInserted());
            } finally {
                pool.shutdownNow();
            }

            // 9. Revisao semantica com colunas permutadas (longitude,latitude e frp=1.0000001) em outra bbox na mesma linha 2 sem colisao V5
            String swappedHeader = "longitude,latitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
            String swappedLine = "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.0000001,N";
            try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass)) {
                c.setAutoCommit(false);
                var resSwap = IngestionRunner.ingestOnConnection(c,
                        new ByteArrayInputStream((swappedHeader + swappedLine + "\n").getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:c3f01_main.csv",
                        new DetectionCsvLoader.Filter(new BBox(-4, -54, -3, -53),
                                LocalDate.of(2026, 11, 12), LocalDate.of(2026, 11, 12)));
                c.commit();
                assertEquals(1L, resSwap.rowsInserted());
                assertEquals(0L, resSwap.rowsDeleted());
            }

            // 10. Arquivo modificado (checksum integral alterado mantendo a mesma linha bruta) sobre LEGACY_UNVERIFIED NAO reconcilia como identico
            String fileModUpdatedCsv = modisHeader + modLine + "\n"
                    + "-3.525,-53.525,311.0,1.0,1.0,2026-11-12,0716,A,81,6.1NRT,291.0,16.0,N\n";
            try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass)) {
                c.setAutoCommit(false);
                var resFileMod = IngestionRunner.ingestOnConnection(c,
                        new ByteArrayInputStream(fileModUpdatedCsv.getBytes(StandardCharsets.UTF_8)),
                        "MODIS_C6.1", "local:c3f01_filemod.csv",
                        new DetectionCsvLoader.Filter(new BBox(-54, -4, -53, -3),
                                LocalDate.of(2026, 11, 12), LocalDate.of(2026, 11, 12)));
                c.commit();
                assertEquals(1L, resFileMod.rowsDeleted(), "LEGACY_UNVERIFIED com file SHA diferente deve ser substituido de forma controlada");
                assertEquals(2L, resFileMod.rowsInserted());
                assertEquals(0L, resFileMod.rowsUnchanged());
            }
            try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass);
                 var st = c.createStatement();
                 var rs = st.executeQuery("SELECT id FROM detection WHERE id = " + origIdFileMod)) {
                assertFalse(rs.next(), "Registro legado substituido nao deve permanecer ativo");
            }
            // Confirma tambem out-of-bbox de main.csv preservado integralmente ate o fim
            try (Connection c = java.sql.DriverManager.getConnection(legacyUrl, user, pass);
                 var st = c.createStatement();
                 var rs = st.executeQuery("SELECT source_semantic_hash_status FROM detection WHERE id = " + origIdOutBbox)) {
                assertTrue(rs.next());
                assertEquals("LEGACY_UNVERIFIED", rs.getString(1));
            }
        } finally {
            try (Connection admin = java.sql.DriverManager.getConnection(adminUrl, user, pass);
                 var st = admin.createStatement()) {
                st.execute("DROP DATABASE IF EXISTS " + legacyDb);
            }
        }
    }

    @Test
    void c3f01ApiDistinguishesAllFourSemanticHashStatesAndRejectsAmbiguity() throws Exception {
        // Companion VIIRS para formar pares no mesmo instante/ponto
        insertDetection("VIIRS", "N20", "2026-11-12", "2026-11-12T07:10:00Z",
                -3.5001, -53.5001, 9.5f, "high", "h", "N", "2.0NRT", "VIIRS_NOAA-20", "local:viirs_states.csv");

        String currentSem = sha256HexIndependent("state-current-sem");
        String legacyUnvSem = sha256HexIndependent("state-legacy-unverified");
        String updatedLegacyOld = sha256HexIndependent("state-updated-old");
        String updatedLegacyNew = sha256HexIndependent("state-updated-new");

        // 1. Insere os 4 estados no mesmo recorte para validar /api/v1/harmonized ponta a ponta via HTTP real
        jdbc.update("""
                INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon, frp,
                                       confidence_raw, confidence_class, daynight, version, source_product, source_file,
                                       source_line_number, source_row_hash, source_semantic_hash, source_file_sha256,
                                       legacy_source_semantic_hash, source_semantic_hash_version, source_semantic_hash_status)
                VALUES ('MODIS', 'A', '2026-11-12', '2026-11-12T07:09:00Z', -3.500, -53.500, 11.0,
                        '80', 'h', 'N', '6.1NRT', 'MODIS_C6.1', 'local:state_current.csv',
                        2, ?, ?, ?, NULL, 'v2-ieee754-direct', 'CURRENT'),
                       ('MODIS', 'A', '2026-11-12', '2026-11-12T07:09:30Z', -3.5002, -53.5002, 12.0,
                        '80', 'h', 'N', '6.1NRT', 'MODIS_C6.1', 'local:state_unknown.csv',
                        NULL, NULL, NULL, NULL, NULL, NULL, 'UNKNOWN'),
                       ('MODIS', 'A', '2026-11-12', '2026-11-12T07:10:00Z', -3.5003, -53.5003, 13.0,
                        '80', 'h', 'N', '6.1NRT', 'MODIS_C6.1', 'local:state_legacy.csv',
                        2, ?, NULL, ?, ?, 'v1-legacy-unverified', 'LEGACY_UNVERIFIED'),
                       ('MODIS', 'A', '2026-11-12', '2026-11-12T07:10:30Z', -3.5004, -53.5004, 14.0,
                        '80', 'h', 'N', '6.1NRT', 'MODIS_C6.1', 'local:state_updated.csv',
                        2, ?, ?, ?, ?, 'v2-ieee754-direct', 'UPDATED_FROM_LEGACY')
                """,
                sha256HexIndependent("raw-cur"), currentSem, sha256HexIndependent("file-cur"),
                sha256HexIndependent("raw-leg"), sha256HexIndependent("file-leg"), legacyUnvSem,
                sha256HexIndependent("raw-upd"), updatedLegacyNew, sha256HexIndependent("file-upd"), updatedLegacyOld);

        ResponseEntity<String> rawResp = rest.getForEntity(
                "/api/v1/harmonized?bbox=-54,-4,-53,-3&start=2026-11-12&end=2026-11-12&includeAssociations=true",
                String.class);
        assertEquals(HttpStatus.OK, rawResp.getStatusCode());
        JsonNode root = objectMapper.readTree(rawResp.getBody());

        JsonNode modisCur = findAssociationByModisSource(root, "local:state_current.csv").path("modis");
        assertEquals("CURRENT", modisCur.path("sourceSemanticHashStatus").asText());
        assertEquals("v2-ieee754-direct", modisCur.path("sourceSemanticHashVersion").asText());
        assertEquals(currentSem, modisCur.path("sourceSemanticHash").asText());
        assertTrue(modisCur.get("legacySourceSemanticHash").isNull());

        JsonNode modisUnk = findAssociationByModisSource(root, "local:state_unknown.csv").path("modis");
        assertEquals("UNKNOWN", modisUnk.path("sourceSemanticHashStatus").asText());
        assertTrue(modisUnk.get("sourceSemanticHashVersion").isNull());
        assertTrue(modisUnk.get("sourceSemanticHash").isNull());
        assertTrue(modisUnk.get("legacySourceSemanticHash").isNull());

        JsonNode modisLeg = findAssociationByModisSource(root, "local:state_legacy.csv").path("modis");
        assertEquals("LEGACY_UNVERIFIED", modisLeg.path("sourceSemanticHashStatus").asText());
        assertEquals("v1-legacy-unverified", modisLeg.path("sourceSemanticHashVersion").asText());
        assertTrue(modisLeg.get("sourceSemanticHash").isNull());
        assertEquals(legacyUnvSem, modisLeg.path("legacySourceSemanticHash").asText());
        assertTrue(modisLeg.path("sourceRecordKey").asText().contains(":legacySem=" + legacyUnvSem.substring(0, 12)));

        JsonNode modisUpd = findAssociationByModisSource(root, "local:state_updated.csv").path("modis");
        assertEquals("UPDATED_FROM_LEGACY", modisUpd.path("sourceSemanticHashStatus").asText());
        assertEquals("v2-ieee754-direct", modisUpd.path("sourceSemanticHashVersion").asText());
        assertEquals(updatedLegacyNew, modisUpd.path("sourceSemanticHash").asText());
        assertEquals(updatedLegacyOld, modisUpd.path("legacySourceSemanticHash").asText());
        assertTrue(reasonsContain(root, "LEGACY_UNVERIFIED"));
        assertTrue(reasonsContain(root, "UPDATED_FROM_LEGACY"));

        // 2. Simula banco corrompido com duas linhas LEGACY_UNVERIFIED ambiguas para a mesma linha-fonte -> falha explicita com Savepoint
        String ambHeader = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight\n";
        String ambLine = "-3.505,-53.505,310.0,1.0,1.0,2026-11-12,0720,A,80,6.1NRT,290.0,1.0000000596046447753906251,N";
        String ambCsv = ambHeader + ambLine + "\n";
        String ambRowHash = sha256HexIndependent(ambLine);
        String ambFileSha = sha256HexIndependent(ambCsv);
        jdbc.update("""
                INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon,
                                       brightness, bright_t31, scan, track, frp,
                                       confidence_raw, confidence_class, daynight, version,
                                       source_product, source_file, source_line_number,
                                       source_row_hash, source_semantic_hash, source_file_sha256,
                                       legacy_source_semantic_hash, source_semantic_hash_version, source_semantic_hash_status)
                VALUES ('MODIS', 'A', '2026-11-12', '2026-11-12T07:20:00Z', -3.505, -53.505,
                        310.0, 290.0, 1.0, 1.0, 1.0000000596046447753906251::real,
                        '80', 'h', 'N', '6.1NRT', 'MODIS_C6.1', 'local:ambiguous_legacy.csv', 2,
                        ?, NULL, ?, 'legacy_dup_1', 'v1-legacy-unverified', 'LEGACY_UNVERIFIED'),
                       ('MODIS', 'A', '2026-11-12', '2026-11-12T07:20:00Z', -3.505, -53.505,
                        310.0, 290.0, 1.0, 1.0, 1.0000000596046447753906251::real,
                        '80', 'h', 'N', '6.1NRT', 'MODIS_C6.1', 'local:ambiguous_legacy.csv', 2,
                        ?, NULL, ?, 'legacy_dup_2', 'v1-legacy-unverified', 'LEGACY_UNVERIFIED')
                """, ambRowHash, ambFileSha, ambRowHash, ambFileSha);

        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                    IngestionRunner.ingestOnConnection(c,
                            new ByteArrayInputStream(ambCsv.getBytes(StandardCharsets.UTF_8)),
                            "MODIS_C6.1", "local:ambiguous_legacy.csv",
                            new DetectionCsvLoader.Filter(new BBox(-54, -4, -53, -3),
                                    LocalDate.of(2026, 11, 12), LocalDate.of(2026, 11, 12))));
            assertTrue(ex.getMessage().contains("Ambiguidade"));
            c.commit(); // rollback parcial via Savepoint deve ter preservado os dois registros intactos
        }
        assertEquals(2, jdbc.queryForObject(
                "SELECT count(*) FROM detection WHERE source_file = 'local:ambiguous_legacy.csv'", Integer.class));
        assertEquals(0, jdbc.queryForObject(
                "SELECT count(*) FROM ingestion_log WHERE source_key = 'local:ambiguous_legacy.csv'", Integer.class));
    }

    @Test
    void c4f01SemanticHashCheckConstraintRejectsAll90InvalidStatesViaInsertUpdateAndCopy() throws Exception {
        String hashA = sha256HexIndependent("c4f01-hash-a");
        String hashB = sha256HexIndependent("c4f01-hash-b");

        String[] statuses = {"CURRENT", "UNKNOWN", "LEGACY_UNVERIFIED", "UPDATED_FROM_LEGACY"};
        String[] currentHashes = {null, hashA};
        String[] legacyHashes = {null, hashA, hashB};
        String[] versions = {null, "v1-legacy-unverified", "v2-ieee754-direct", "v0-invalid"};

        Set<String> expectedValid = Set.of(
                "UNKNOWN|null|null|null",
                "LEGACY_UNVERIFIED|null|" + hashA + "|v1-legacy-unverified",
                "LEGACY_UNVERIFIED|null|" + hashB + "|v1-legacy-unverified",
                "CURRENT|" + hashA + "|null|v2-ieee754-direct",
                "CURRENT|" + hashA + "|" + hashA + "|v2-ieee754-direct",
                "UPDATED_FROM_LEGACY|" + hashA + "|" + hashB + "|v2-ieee754-direct"
        );

        List<String> unexpectedlyAccepted = new ArrayList<>();
        List<String> unexpectedlyRejected = new ArrayList<>();
        List<String> wrongSqlStates = new ArrayList<>();
        int totalTested = 0;
        int acceptedCount = 0;
        int rejectedCount = 0;

        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);

            // 1. Matriz exaustiva 4 x 2 x 3 x 4 = 96 combinacoes via INSERT no PostgreSQL real
            String insertSql = """
                    INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon,
                                           frp, source_product, source_file,
                                           source_semantic_hash, legacy_source_semantic_hash,
                                           source_semantic_hash_version, source_semantic_hash_status)
                    VALUES ('MODIS', 'A', '2026-11-12'::date, '2026-11-12T07:09:00Z'::timestamptz, -3.5, -53.5,
                            10.0, 'MODIS_C6.1', 'local:c4f01_matrix.csv', ?, ?, ?, ?)
                    """;
            for (String status : statuses) {
                for (String curr : currentHashes) {
                    for (String legacy : legacyHashes) {
                        for (String ver : versions) {
                            totalTested++;
                            String key = status + "|" + curr + "|" + legacy + "|" + ver;
                            boolean shouldBeValid = expectedValid.contains(key);
                            Savepoint sp = c.setSavepoint();
                            try (var ps = c.prepareStatement(insertSql)) {
                                ps.setString(1, curr);
                                ps.setString(2, legacy);
                                ps.setString(3, ver);
                                ps.setString(4, status);
                                ps.executeUpdate();
                                acceptedCount++;
                                if (!shouldBeValid) {
                                    unexpectedlyAccepted.add(key);
                                }
                            } catch (SQLException e) {
                                rejectedCount++;
                                if (!"23514".equals(e.getSQLState())) {
                                    wrongSqlStates.add(key + " -> SQLState=" + e.getSQLState());
                                }
                                if (shouldBeValid) {
                                    unexpectedlyRejected.add(key + " -> " + e.getMessage());
                                }
                            } finally {
                                c.rollback(sp);
                            }
                        }
                    }
                }
            }

            assertEquals(96, totalTested);
            assertEquals(List.of(), wrongSqlStates, "Toda violacao da CHECK deve retornar SQLState 23514");
            assertEquals(List.of(), unexpectedlyRejected, "Todos os 6 estados validos devem ser aceitos");
            assertEquals(List.of(), unexpectedlyAccepted, "Nenhuma combinacao invalida (incluindo version NULL) pode ser aceita");
            assertEquals(6, acceptedCount);
            assertEquals(90, rejectedCount);

            // 2. Variacoes adicionais fora da matriz (status invalido, version vazia '', particoes 2024 e default 2035)
            String[][] extraInvalid = {
                    {"BOGUS", hashA, null, "v2-ieee754-direct", "2026-11-12"},
                    {"BOGUS", null, null, null, "2026-11-12"},
                    {"CURRENT", hashA, null, "", "2026-11-12"},
                    {"UNKNOWN", null, null, "", "2026-11-12"},
                    {"LEGACY_UNVERIFIED", null, hashA, "", "2026-11-12"},
                    {"UPDATED_FROM_LEGACY", hashA, hashB, "", "2026-11-12"},
                    {"CURRENT", hashA, null, null, "2024-05-01"}, // particao detection_2024
                    {"CURRENT", hashA, null, null, "2035-01-01"}  // particao detection_default
            };
            String partitionInsertSql = """
                    INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon,
                                           frp, source_product, source_file,
                                           source_semantic_hash, legacy_source_semantic_hash,
                                           source_semantic_hash_version, source_semantic_hash_status)
                    VALUES ('MODIS', 'A', ?::date, ?::timestamptz, -3.5, -53.5,
                            10.0, 'MODIS_C6.1', 'local:c4f01_extra.csv', ?, ?, ?, ?)
                    """;
            for (String[] row : extraInvalid) {
                assertSqlState23514(c, () -> {
                    try (var ps = c.prepareStatement(partitionInsertSql)) {
                        ps.setString(1, row[4]);
                        ps.setString(2, row[4] + "T07:09:00Z");
                        ps.setString(3, row[1]);
                        ps.setString(4, row[2]);
                        ps.setString(5, row[3]);
                        ps.setString(6, row[0]);
                        ps.executeUpdate();
                    }
                }, "INSERT extra invalido: " + String.join("|", row));
            }

            // 3. UPDATE invalido (incluindo todas as 5 combinacoes vazadas anteriormente e status/hashes invalidos)
            long idUnknown = insertConstrainedDetection(c, "2026-11-12", null, null, null, "UNKNOWN");
            long idLegacy = insertConstrainedDetection(c, "2026-11-12", null, hashA, "v1-legacy-unverified", "LEGACY_UNVERIFIED");
            long idCurrent = insertConstrainedDetection(c, "2026-11-12", hashA, null, "v2-ieee754-direct", "CURRENT");
            long idCurrentWithLegacy = insertConstrainedDetection(c, "2026-11-12", hashA, hashA, "v2-ieee754-direct", "CURRENT");
            long idUpdated = insertConstrainedDetection(c, "2026-11-12", hashA, hashB, "v2-ieee754-direct", "UPDATED_FROM_LEGACY");

            assertSqlState23514(c, () -> execUpdate(c,
                    "UPDATE detection SET source_semantic_hash_version = NULL WHERE id = " + idCurrent),
                    "UPDATE CURRENT com version NULL");
            assertSqlState23514(c, () -> execUpdate(c,
                    "UPDATE detection SET source_semantic_hash_version = NULL WHERE id = " + idCurrentWithLegacy),
                    "UPDATE CURRENT(legacy=curr) com version NULL");
            assertSqlState23514(c, () -> execUpdate(c,
                    "UPDATE detection SET source_semantic_hash_version = NULL WHERE id = " + idLegacy),
                    "UPDATE LEGACY_UNVERIFIED com version NULL");
            assertSqlState23514(c, () -> execUpdate(c,
                    "UPDATE detection SET source_semantic_hash_version = NULL WHERE id = " + idUpdated),
                    "UPDATE UPDATED_FROM_LEGACY com version NULL");
            assertSqlState23514(c, () -> execUpdate(c,
                    "UPDATE detection SET source_semantic_hash_status = 'UPDATED_FROM_LEGACY' WHERE id = " + idCurrentWithLegacy),
                    "UPDATE UPDATED_FROM_LEGACY com legacy == current");
            assertSqlState23514(c, () -> execUpdate(c,
                    "UPDATE detection SET source_semantic_hash_version = 'v2-ieee754-direct' WHERE id = " + idUnknown),
                    "UPDATE UNKNOWN com version nao nula");
            assertSqlState23514(c, () -> execUpdate(c,
                    "UPDATE detection SET source_semantic_hash_status = 'BOGUS' WHERE id = " + idCurrent),
                    "UPDATE status invalido");

            // UPDATE valido permitido
            execUpdate(c, "UPDATE detection SET source_semantic_hash = '" + hashB
                    + "', source_semantic_hash_version = 'v2-ieee754-direct', source_semantic_hash_status = 'UPDATED_FROM_LEGACY' WHERE id = "
                    + idLegacy);

            // 4. COPY invalido rejeitado (5 combinacoes vazadas + casos invalidos) e COPY valido aceito
            String copySql = """
                    COPY detection (sensor, satellite, acq_date, acq_ts, lat, lon, frp, source_product, source_file,
                                    source_semantic_hash, legacy_source_semantic_hash,
                                    source_semantic_hash_version, source_semantic_hash_status)
                    FROM STDIN WITH (FORMAT csv, NULL '')
                    """;
            var copyApi = c.unwrap(PGConnection.class).getCopyAPI();
            String validCopyCsv = String.join("\n",
                    "MODIS,A,2026-11-12,2026-11-12T07:09:00Z,-3.5,-53.5,10.0,MODIS_C6.1,local:copy_ok.csv,,,,UNKNOWN",
                    "MODIS,A,2026-11-12,2026-11-12T07:09:00Z,-3.5,-53.5,10.0,MODIS_C6.1,local:copy_ok.csv,," + hashA + ",v1-legacy-unverified,LEGACY_UNVERIFIED",
                    "MODIS,A,2026-11-12,2026-11-12T07:09:00Z,-3.5,-53.5,10.0,MODIS_C6.1,local:copy_ok.csv," + hashA + ",,v2-ieee754-direct,CURRENT",
                    "MODIS,A,2026-11-12,2026-11-12T07:09:00Z,-3.5,-53.5,10.0,MODIS_C6.1,local:copy_ok.csv," + hashA + "," + hashB + ",v2-ieee754-direct,UPDATED_FROM_LEGACY"
            ) + "\n";
            long copied = copyApi.copyIn(copySql, new StringReader(validCopyCsv));
            assertEquals(4L, copied);

            String[] invalidCopyRows = {
                    "MODIS,A,2026-11-12,2026-11-12T07:09:00Z,-3.5,-53.5,10.0,MODIS_C6.1,local:copy_bad.csv," + hashA + ",,,CURRENT",
                    "MODIS,A,2026-11-12,2026-11-12T07:09:00Z,-3.5,-53.5,10.0,MODIS_C6.1,local:copy_bad.csv," + hashA + "," + hashA + ",,CURRENT",
                    "MODIS,A,2026-11-12,2026-11-12T07:09:00Z,-3.5,-53.5,10.0,MODIS_C6.1,local:copy_bad.csv,," + hashA + ",,LEGACY_UNVERIFIED",
                    "MODIS,A,2026-11-12,2026-11-12T07:09:00Z,-3.5,-53.5,10.0,MODIS_C6.1,local:copy_bad.csv,," + hashB + ",,LEGACY_UNVERIFIED",
                    "MODIS,A,2026-11-12,2026-11-12T07:09:00Z,-3.5,-53.5,10.0,MODIS_C6.1,local:copy_bad.csv," + hashA + "," + hashB + ",,UPDATED_FROM_LEGACY",
                    "MODIS,A,2026-11-12,2026-11-12T07:09:00Z,-3.5,-53.5,10.0,MODIS_C6.1,local:copy_bad.csv," + hashA + "," + hashB + ",v2-ieee754-direct,CURRENT",
                    "MODIS,A,2026-11-12,2026-11-12T07:09:00Z,-3.5,-53.5,10.0,MODIS_C6.1,local:copy_bad.csv,,,,BOGUS"
            };
            for (String badRow : invalidCopyRows) {
                assertSqlState23514(c, () -> copyApi.copyIn(copySql, new StringReader(badRow + "\n")),
                        "COPY invalido: " + badRow);
            }

            // 5. Confirma constraint presente e validada em detection e todas as 32 particoes
            try (var st = c.createStatement();
                 var rs = st.executeQuery("""
                         SELECT count(*) AS total_Hierarchy,
                                count(*) FILTER (WHERE c.convalidated = true) AS validated_cnt
                         FROM (
                             SELECT 'detection'::regclass AS reloid
                             UNION ALL
                             SELECT inhrelid FROM pg_inherits WHERE inhparent = 'detection'::regclass
                         ) hierarchy
                         LEFT JOIN pg_constraint c
                           ON c.conrelid = hierarchy.reloid
                          AND c.conname = 'chk_detection_semantic_hash_consistency'
                         """)) {
                assertTrue(rs.next());
                assertEquals(33L, rs.getLong("total_Hierarchy"));
                assertEquals(33L, rs.getLong("validated_cnt"));
            }

            c.rollback();
        }
    }

    @FunctionalInterface
    private interface SqlAction {
        void run() throws Exception;
    }

    private static void assertSqlState23514(Connection c, SqlAction action, String label) throws Exception {
        Savepoint sp = c.setSavepoint();
        try {
            action.run();
            fail("Esperava SQLState 23514 (check_violation) para " + label);
        } catch (SQLException e) {
            assertEquals("23514", e.getSQLState(),
                    "Esperava SQLState 23514 para " + label + ", obtido " + e.getSQLState() + ": " + e.getMessage());
        } finally {
            c.rollback(sp);
        }
    }

    private static void execUpdate(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.executeUpdate(sql);
        }
    }

    private static long insertConstrainedDetection(
            Connection c, String acqDate, String semHash, String legacyHash, String semVer, String status) throws SQLException {
        try (var ps = c.prepareStatement("""
                INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon,
                                       frp, source_product, source_file,
                                       source_semantic_hash, legacy_source_semantic_hash,
                                       source_semantic_hash_version, source_semantic_hash_status)
                VALUES ('MODIS', 'A', ?::date, (? || 'T07:09:00Z')::timestamptz, -3.5, -53.5,
                        10.0, 'MODIS_C6.1', 'local:c4f01_seed.csv', ?, ?, ?, ?)
                RETURNING id
                """)) {
            ps.setString(1, acqDate);
            ps.setString(2, acqDate);
            ps.setString(3, semHash);
            ps.setString(4, legacyHash);
            ps.setString(5, semVer);
            ps.setString(6, status);
            var rs = ps.executeQuery();
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private static long insertV4Detection(
            Connection c, String sensor, String satellite, String date, String ts,
            double lat, double lon, double brightness, double brightT31,
            double scan, double track, double frp,
            String confRaw, String confClass, String daynight,
            String version, String product, String sourceFile, long lineNumber,
            String rowHash, String semHash, String fileSha) throws SQLException {
        try (var ps = c.prepareStatement("""
                INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon,
                                       brightness, bright_t31, scan, track, frp,
                                       confidence_raw, confidence_class, daynight, version,
                                       source_product, source_file, source_line_number,
                                       source_row_hash, source_semantic_hash, source_file_sha256)
                VALUES (?, ?, ?::date, ?::timestamptz, ?, ?, ?, ?, ?, ?, ?::real, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
                """)) {
            ps.setString(1, sensor);
            ps.setString(2, satellite);
            ps.setString(3, date);
            ps.setString(4, ts);
            ps.setDouble(5, lat);
            ps.setDouble(6, lon);
            ps.setDouble(7, brightness);
            ps.setDouble(8, brightT31);
            ps.setDouble(9, scan);
            ps.setDouble(10, track);
            ps.setString(11, Double.toString(frp));
            ps.setString(12, confRaw);
            ps.setString(13, confClass);
            ps.setString(14, daynight);
            ps.setString(15, version);
            ps.setString(16, product);
            ps.setString(17, sourceFile);
            ps.setLong(18, lineNumber);
            ps.setString(19, rowHash);
            ps.setString(20, semHash);
            ps.setString(21, fileSha);
            var rs = ps.executeQuery();
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private MockMvc createStandaloneHarmonizationMockMvc(String jdbcUrl, String user, String pass) {
        var ds = new DriverManagerDataSource(jdbcUrl, user, pass);
        var namedJdbc = new NamedParameterJdbcTemplate(ds);
        var standaloneRepo = new DetectionRepository(namedJdbc);
        var standaloneTx = new JdbcTransactionManager(ds);
        var service = new HarmonizationService(standaloneRepo, standaloneTx, 10000L);
        var controller = new AnalysisController(standaloneRepo, service, 366, 31);
        return MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ApiExceptionHandler())
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    private static JsonNode findAssociationByModisId(JsonNode root, long modisId) {
        for (JsonNode assoc : root.path("associations")) {
            if (assoc.path("modis").path("id").asLong() == modisId) {
                return assoc;
            }
        }
        fail("Associacao com modis.id=" + modisId + " nao encontrada na resposta");
        return null;
    }

    private static JsonNode findAssociationByModisSource(JsonNode root, String sourceFile) {
        for (JsonNode assoc : root.path("associations")) {
            if (sourceFile.equals(assoc.path("modis").path("sourceFile").asText())) {
                return assoc;
            }
        }
        fail("Associacao com modis.sourceFile=" + sourceFile + " nao encontrada na resposta");
        return null;
    }

    private static boolean reasonsContain(JsonNode root, String token) {
        for (JsonNode r : root.path("scientificState").path("reasons")) {
            if (r.asText().contains(token)) return true;
        }
        return false;
    }

    private static String legacyV4BuggySemanticHash(
            String sensor, String satellite, String acqDate, String acqTs,
            double lat, double lon, String brightnessRaw, String brightT31Raw,
            String scanRaw, String trackRaw, String frpRaw,
            String confRaw, String confClass, String daynight,
            String version, String product, String sourceFile) throws Exception {
        // Implementacao historica pre-B.3 (passando float64 -> float32 com duplo arredondamento)
        String[] fields = {
                sensor, satellite, acqDate, acqTs,
                Double.toString(lat == 0.0 ? 0.0 : lat),
                Double.toString(lon == 0.0 ? 0.0 : lon),
                legacyDoubleRoundedFloat(brightnessRaw),
                legacyDoubleRoundedFloat(brightT31Raw),
                legacyDoubleRoundedFloat(scanRaw),
                legacyDoubleRoundedFloat(trackRaw),
                legacyDoubleRoundedFloat(frpRaw),
                confRaw, confClass, daynight, version, product, sourceFile
        };
        StringBuilder canonical = new StringBuilder(192);
        for (String f : fields) {
            String val = f == null ? "" : f;
            canonical.append(val.length()).append(':').append(val).append('|');
        }
        return sha256HexIndependent(canonical.toString());
    }

    private static String legacyDoubleRoundedFloat(String raw) {
        if (raw == null || raw.isEmpty()) return "";
        float f = (float) Double.parseDouble(raw);
        return Float.toString(f == 0.0f ? 0.0f : f);
    }

    private static String sha256HexIndependent(String value) throws Exception {
        var md = java.security.MessageDigest.getInstance("SHA-256");
        return java.util.HexFormat.of().formatHex(md.digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static String sig(Instant mTs, double mLat, double mLon,
                              String vSat, Instant vTs, double vLat, double vLon, long dtSec) {
        return String.format(Locale.ROOT, "%s|%.5f|%.5f|%s|%s|%.5f|%.5f|%d",
                mTs, mLat, mLon, vSat, vTs, vLat, vLon, dtSec);
    }
}
