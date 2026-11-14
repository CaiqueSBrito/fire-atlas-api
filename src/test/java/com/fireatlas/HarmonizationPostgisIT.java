package com.fireatlas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fireatlas.analysis.controller.AnalysisController;
import com.fireatlas.common.controller.ApiExceptionHandler;
import com.fireatlas.common.dto.Dto;
import com.fireatlas.region.entity.BBox;
import com.fireatlas.oracle.FirmsIndependentOracle;
import com.fireatlas.detection.repository.DetectionRepository;
import com.fireatlas.harmonization.service.HarmonizationService;
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

    /**
     * Carrega uma fixture real da FIRMS (CSV de teste) direto em detection por SQL, so para o teste de regressao do
     * pareamento contra o oraculo independente. A importacao de producao e somente a prata (SilverImportService).
     */
    private void loadFixture(String classpathPath, String product, String sourceKey) throws Exception {
        var resource = new ClassPathResource(classpathPath);
        String sensor = product.startsWith("MODIS") ? "MODIS" : "VIIRS";
        int kept = 0;
        byte[] bytes = resource.getInputStream().readAllBytes();
        String fileSha = sha256HexIndependent(new String(bytes, StandardCharsets.ISO_8859_1));
        long lineNo = 1;
        try (var reader = new java.io.BufferedReader(new java.io.InputStreamReader(new java.io.ByteArrayInputStream(bytes),
                StandardCharsets.UTF_8))) {
            String[] header = reader.readLine().split(",", -1);
            var col = new java.util.HashMap<String, Integer>();
            for (int i = 0; i < header.length; i++) col.put(header[i].trim(), i);
            String line;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                if (line.isBlank()) continue;
                String[] f = line.split(",", -1);
                double lat = Double.parseDouble(f[col.get("latitude")]);
                double lon = Double.parseDouble(f[col.get("longitude")]);
                String date = f[col.get("acq_date")];
                if (lat < -4 || lat > -3 || lon < -54 || lon > -53) continue;
                if (date.compareTo("2026-11-12") < 0 || date.compareTo("2026-11-13") > 0) continue;
                String time = ("0000" + f[col.get("acq_time")]).substring(f[col.get("acq_time")].length());
                String raw = f[col.get("confidence")];
                String klass = sensor.equals("MODIS") ? modisClass(raw) : switch (raw.toLowerCase()) {
                    case "l", "low" -> "l";
                    case "n", "nominal" -> "n";
                    case "h", "high" -> "h";
                    default -> null;
                };
                String b1 = col.containsKey("brightness") ? "brightness" : "bright_ti4";
                String b2 = col.containsKey("bright_t31") ? "bright_t31" : "bright_ti5";
                jdbc.update("""
                        INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon, brightness, bright_t31,
                                               scan, track, frp, confidence_raw, confidence_class, daynight, version,
                                               source_product, source_file, source_line_number,
                                               source_row_hash, source_file_sha256)
                        VALUES (?, ?, ?::date, ?::timestamptz, ?, ?, ?::real, ?::real, ?::real, ?::real, ?::real, ?, ?,
                                ?, ?, ?, ?, ?, ?, ?)""",
                        sensor, f[col.get("satellite")], date,
                        date + "T" + time.substring(0, 2) + ":" + time.substring(2, 4) + ":00Z", lat, lon,
                        f[col.get(b1)], f[col.get(b2)], f[col.get("scan")], f[col.get("track")], f[col.get("frp")],
                        raw, klass, f[col.get("daynight")], f[col.get("version")], product, sourceKey, lineNo,
                        sha256HexIndependent(line), fileSha);
                kept++;
            }
        }
        assertTrue(kept > 0);
        jdbc.update("DELETE FROM daily_grid WHERE acq_date BETWEEN '2026-11-12' AND '2026-11-13'");
        jdbc.update("""
                INSERT INTO daily_grid (acq_date, sensor, cx, cy, n, frp_sum, frp_max)
                SELECT acq_date, sensor, GREATEST(0, LEAST(floor((lon + 180) * 10)::int, 3599)),
                       GREATEST(0, LEAST(floor((lat + 90) * 10)::int, 1799)), count(*), coalesce(sum(frp), 0), max(frp)
                FROM detection WHERE acq_date BETWEEN '2026-11-12' AND '2026-11-13' GROUP BY 1, 2, 3, 4""");
    }

    private static String modisClass(String raw) {
        int v = Integer.parseInt(raw);
        return v < 30 ? "l" : v < 80 ? "n" : "h";
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
            assertEquals(64, apiPair.modis().sourceFileSha256().length());
            assertNotNull(apiPair.viirs().observationKey());
            assertNotNull(apiPair.viirs().sourceLineNumber());
            assertNotNull(apiPair.viirs().sourceRecordKey());
            assertNotNull(apiPair.viirs().sourceRawLocator());
            assertEquals(64, apiPair.viirs().sourceRowHash().length());
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
        // a lista de fontes vem do log de importacao da prata (coberto em SilverImportAcceptanceTest)

        var prefPut = rest.exchange("/api/v1/preferences", HttpMethod.PUT,
                new HttpEntity<>(new Dto.Preferences("Para", List.of(-54.0, -4.0, -53.0, -3.0), "BOTH", 8, List.of(-53.5, -3.5))),
                Dto.Preferences.class);
        assertEquals(HttpStatus.OK, prefPut.getStatusCode());
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
