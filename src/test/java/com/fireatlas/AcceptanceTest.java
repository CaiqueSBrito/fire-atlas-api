package com.fireatlas;

import com.fireatlas.detection.repository.DetectionRepository;
import com.fireatlas.region.entity.Region;
import com.fireatlas.h3.policy.H3Settings;
import com.fireatlas.h3.policy.H3Index;
import com.fireatlas.harmonization.repository.HarmonizationRunStore;
import com.fireatlas.ingestion.integration.DetectionCsvLoader;
import com.fireatlas.ingestion.integration.DetectionRowParser;
import com.fireatlas.ingestion.integration.H3Options;
import com.fireatlas.ingestion.integration.LakeSource;
import com.fireatlas.ingestion.integration.ParquetCsvReader;
import com.fireatlas.ingestion.repository.H3Aggregates;
import com.fireatlas.ingestion.repository.IngestionRepository;
import com.fireatlas.ingestion.service.ImportService;
import com.fireatlas.region.entity.BBox;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.extension.ExtendWith;
import org.postgresql.PGConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.Set;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Criterios de aceite CA01-CA04 contra um PostGIS real, em um schema temporario isolado
 * (o schema e removido ao final; o banco do docker compose precisa estar no ar).
 */
@ExtendWith(DbAvailable.class)
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AcceptanceTest {

    static final String SCHEMA = "it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);

    static final String HEADER =
            "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_t31,frp,daynight";
    static final String MODIS_ROW = "-10.5,-55.5,330.1,1.0,1.0,2026-09-02,1805,Aqua,MODIS,85,6.1NRT,300.2,25.5,D";
    static final String VIIRS_ROW = "-10.5001,-55.5001,340.0,0.4,0.4,2026-09-02,1750,N,VIIRS,n,2.0NRT,290.0,5.5,D";
    static final BBox BBOX = new BBox(-56, -11, -55, -10);
    static final DetectionCsvLoader.Filter FILTER =
            new DetectionCsvLoader.Filter(BBOX, LocalDate.of(2026, 9, 2), LocalDate.of(2026, 9, 2));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", () -> DbAvailable.URL + "?currentSchema=" + SCHEMA + ",public");
        r.add("spring.datasource.username", () -> DbAvailable.USER);
        r.add("spring.datasource.password", () -> DbAvailable.PASSWORD);
        r.add("spring.flyway.schemas", () -> SCHEMA + ",public");
        r.add("spring.flyway.default-schema", () -> SCHEMA);
        r.add("spring.flyway.create-schemas", () -> "true");
    }

    @AfterAll
    static void dropSchema() throws Exception {
        try (Connection c = DriverManager.getConnection(DbAvailable.URL, DbAvailable.USER, DbAvailable.PASSWORD);
             Statement s = c.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
        }
    }

    @Autowired MockMvc mvc;
    @Autowired DataSource dataSource;
    @Autowired JdbcTemplate jdbc;
    @Autowired HarmonizationRunStore runStore;
    @Autowired IngestionRepository ingestionRepository;

    private DetectionCsvLoader.Stats load(String product, String file, String... rows) throws Exception {
        String sensor = product.startsWith("MODIS") ? "MODIS" : "VIIRS";
        String csv = withH3(HEADER + "\n" + String.join("\n", rows) + "\n");
        try (Connection c = dataSource.getConnection()) {
            BufferedReader reader = new BufferedReader(new StringReader(csv));
            String header = reader.readLine(); // como o IngestionRunner: o cabecalho e consumido antes do loader
            return new DetectionCsvLoader().load(reader,
                    new DetectionRowParser(header, sensor, product, file), FILTER,
                    c.unwrap(PGConnection.class).getCopyAPI());
        }
    }

    @Test
    @Order(1)
    void ca01_inicializacao_sobe_sem_erro_de_configuracao() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    @Order(2)
    void ca02_persistencia_deteccao_valida_consultavel_com_sensor_e_fonte() throws Exception {
        var m = load("MODIS_C6.1", "it/modis.csv", MODIS_ROW);
        var v = load("VIIRS_S-NPP", "it/viirs.csv", VIIRS_ROW);
        assertEquals(1, m.kept());
        assertEquals(1, v.kept());

        mvc.perform(get("/api/v1/detections").param("bbox", "-56,-11,-55,-10")
                        .param("start", "2026-09-02").param("end", "2026-09-02").param("zoom", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.mode").value("points"))
                .andExpect(jsonPath("$.count").value(2))
                .andExpect(jsonPath("$.points[?(@.sensor=='MODIS')].satellite").value("Aqua"))
                .andExpect(jsonPath("$.points[?(@.sensor=='VIIRS')].satellite").value("N"));

        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT sensor, source_product, source_file FROM detection ORDER BY sensor");
        assertEquals(List.of(
                Map.of("sensor", "MODIS", "source_product", "MODIS_C6.1", "source_file", "it/modis.csv"),
                Map.of("sensor", "VIIRS", "source_product", "VIIRS_S-NPP", "source_file", "it/viirs.csv")), rows);
    }

    @Test
    @Order(3)
    void ca03_integridade_rejeita_invalidas_e_informa_o_motivo() throws Exception {
        long before = jdbc.queryForObject("SELECT count(*) FROM detection", Long.class);
        var s = load("MODIS_C6.1", "it/invalid.csv",
                MODIS_ROW.replace("-10.5,", "95,"),                 // latitude fora do intervalo
                MODIS_ROW.replace("2026-09-02", "2026-13-45"),      // data invalida
                MODIS_ROW.replace(",1805,", ",2575,"),              // hora invalida
                MODIS_ROW.replace(",Aqua,", ",,"),                  // satelite ausente
                MODIS_ROW.replace(",25.5,D", ",-3,D"),              // FRP negativo
                // invalida, mas fora da regiao pedida: ignorada, nao conta como rejeicao
                MODIS_ROW.replace("-10.5,-55.5", "-30.0,-40.0").replace(",25.5,D", ",-3,D"));
        assertEquals(0, s.kept());
        assertEquals(5, s.rejected());
        assertEquals(Map.of(
                DetectionRowParser.LATITUDE_INVALIDA, 1L,
                DetectionRowParser.DATA_INVALIDA, 1L,
                DetectionRowParser.HORA_INVALIDA, 1L,
                DetectionRowParser.SATELITE_AUSENTE, 1L,
                DetectionRowParser.FRP_INVALIDO, 1L), s.rejects());
        assertEquals(before, jdbc.queryForObject("SELECT count(*) FROM detection", Long.class),
                "nenhuma deteccao invalida pode ser persistida");

        // Defesa em profundidade: o proprio banco recusa valores fora do dominio.
        assertThrows(DataAccessException.class, () -> jdbc.update("""
                INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon, source_product, source_file)
                VALUES ('MODIS', 'Aqua', '2026-09-02', '2026-09-02T18:05:00Z', 95, -55, 'MODIS_C6.1', 'it/x.csv')"""));
    }

    @Test
    @Order(4)
    void ca04_preservacao_originais_permanecem_ao_salvar_derivado() {
        List<Map<String, Object>> before = jdbc.queryForList(
                "SELECT sensor, frp, brightness, lat, lon, acq_ts, source_file FROM detection ORDER BY sensor");
        assertEquals(2, before.size());

        long id = runStore.save("Pareamento espaco-temporal MODIS->VIIRS", "v0", BBOX,
                LocalDate.of(2026, 9, 2), LocalDate.of(2026, 9, 2),
                Map.of("maxDistanceMeters", 750, "maxMinutes", 180), Map.of("matchRate", 1.0));
        assertTrue(id > 0);
        assertEquals(1L, jdbc.queryForObject(
                "SELECT count(*) FROM harmonization_run WHERE id = ? AND result->>'matchRate' = '1.0'", Long.class, id));

        assertEquals(before, jdbc.queryForList(
                "SELECT sensor, frp, brightness, lat, lon, acq_ts, source_file FROM detection ORDER BY sensor"),
                "valores originais devem permanecer disponiveis e iguais");

        // O banco impede sobrescrever medicoes originais (RN05).
        DataAccessException ex = assertThrows(DataAccessException.class,
                () -> jdbc.update("UPDATE detection SET frp = 999 WHERE sensor = 'MODIS'"));
        assertTrue(ex.getMessage().contains("imutavel"), ex.getMessage());
        assertEquals(25.5f, jdbc.queryForObject(
                "SELECT frp FROM detection WHERE sensor = 'MODIS'", Float.class), 1e-6);
    }

    @Test
    @Order(5)
    void h3_indice_por_deteccao_agregacao_e_regiao_por_celulas() throws Exception {
        // Cada deteccao guarda o indice H3 (res 8) calculado a partir de lat/lon.
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT lat, lon, h3 FROM detection WHERE source_file IN ('it/modis.csv', 'it/viirs.csv')");
        assertEquals(2, rows.size());
        for (Map<String, Object> r : rows) {
            long expected = TestH3.cell(((Number) r.get("lat")).doubleValue(), ((Number) r.get("lon")).doubleValue());
            assertEquals(expected, ((Number) r.get("h3")).longValue());
        }

        // A formula SQL do ancestral (bits) coincide com a biblioteca H3 em todas as resolucoes agregadas.
        long h3 = jdbc.queryForObject("SELECT h3 FROM detection WHERE source_file = 'it/modis.csv'", Long.class);
        for (int res : H3Settings.defaults().aggResolutions()) {
            Long viaSql = jdbc.queryForObject(
                    "SELECT " + H3Index.parentSql("h3", res) + " FROM detection WHERE source_file = 'it/modis.csv'",
                    Long.class);
            assertEquals(H3Index.parent(h3, res), viaSql, "ancestral em SQL difere na resolucao " + res);
        }

        // Agregacao diaria por celula H3.
        try (Connection c = dataSource.getConnection()) {
            H3Aggregates.refresh(c, LocalDate.of(2026, 9, 2), LocalDate.of(2026, 9, 2),
                    H3Settings.defaults().aggResolutions());
        }
        assertEquals(H3Settings.defaults().aggResolutions().length, jdbc.queryForObject(
                "SELECT count(DISTINCT res) FROM daily_h3", Integer.class));

        // Regiao informada por celulas H3 (resolucao 4): mapa agregado em celulas H3 de resolucao 6.
        String r4 = H3Index.toHex(H3Index.parent(TestH3.cell(-10.5, -55.5), 4));
        mvc.perform(get("/api/v1/detections").param("cells", r4)
                        .param("start", "2026-09-02").param("end", "2026-09-02").param("zoom", "6"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.mode").value("grid"))
                .andExpect(jsonPath("$.resolution").value(6))
                .andExpect(jsonPath("$.count").value(2))
                .andExpect(jsonPath("$.cells[0].h3").isString());

        // Pontos individuais com regiao por celulas (usa o indice h3 de deteccoes).
        mvc.perform(get("/api/v1/detections").param("cells", r4)
                        .param("start", "2026-09-02").param("end", "2026-09-02").param("zoom", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.mode").value("points"))
                .andExpect(jsonPath("$.count").value(2));

        // Calendario e serie por celulas H3 (a API nao converte bbox em celulas).
        mvc.perform(get("/api/v1/calendar").param("cells", r4).param("month", "2026-09"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.daysWithFire").value(1));
        mvc.perform(get("/api/v1/timeseries").param("cells", r4)
                        .param("start", "2026-09-01").param("end", "2026-09-07"))
                .andExpect(status().isOk());

        // As respostas agregadas trazem so o H3 (sem coordenadas calculadas)
        mvc.perform(get("/api/v1/detections").param("cells", r4)
                        .param("start", "2026-09-02").param("end", "2026-09-02").param("zoom", "6"))
                .andExpect(jsonPath("$.cells[0].lat").doesNotExist())
                .andExpect(jsonPath("$.cells[0].lon").doesNotExist());

        // Consultas agregadas com bbox sao recusadas com mensagem clara; pontos por bbox seguem funcionando.
        for (String path : List.of("/api/v1/calendar", "/api/v1/timeseries", "/api/v1/detections")) {
            mvc.perform(get(path).param("bbox", "-56,-11,-55,-10").param("month", "2026-09")
                            .param("start", "2026-09-02").param("end", "2026-09-02").param("zoom", "6"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("cells")));
        }

        // bbox e cells juntos, ou nenhum, sao erro de validacao.
        mvc.perform(get("/api/v1/detections").param("bbox", "-56,-11,-55,-10").param("cells", r4)
                        .param("start", "2026-09-02").param("end", "2026-09-02"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/detections").param("start", "2026-09-02").param("end", "2026-09-02"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @Order(6)
    void sensor_como_entidade_catalogo_resolucao_e_integridade() throws Exception {
        // Catalogo semeado: sensores com resolucao nominal e satelites que os carregam.
        mvc.perform(get("/api/v1/meta/sensors"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[?(@.code=='MODIS')].nominalResolutionMeters").value(1000))
                .andExpect(jsonPath("$[?(@.code=='VIIRS')].nominalResolutionMeters").value(375))
                .andExpect(jsonPath("$[?(@.code=='MODIS')].satellites.length()").value(2))
                .andExpect(jsonPath("$[?(@.code=='VIIRS')].satellites.length()").value(3));

        // A resolucao nominal do /compare vem do catalogo, nao do codigo.
        mvc.perform(get("/api/v1/compare").param("bbox", "-56,-11,-55,-10")
                        .param("start", "2026-09-02").param("end", "2026-09-02"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].nominalResolutionMeters").value(1000))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].nominalResolutionMeters").value(375));

        // Toda deteccao precisa apontar para um sensor do catalogo.
        assertThrows(DataAccessException.class, () -> jdbc.update("""
                INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon, source_product, source_file)
                VALUES ('LANDSAT', 'L9', '2026-09-02', '2026-09-02T18:05:00Z', -10, -55, 'X', 'it/x.csv')"""));
        // Um satelite do catalogo nao pode apontar para sensor inexistente.
        assertThrows(DataAccessException.class, () -> jdbc.update(
                "INSERT INTO satellite (code, sensor, platform_name, source_product) VALUES ('Z', 'NAO_EXISTE', 'z', 'z')"));
    }

    // ------------------------------------------------------------------------------------------------
    // Card de ingestao: importacao de fontes (RN01-RN06, CA01-CA04), com uma fonte simulada em memoria
    // ------------------------------------------------------------------------------------------------

    static final String SRC_ROW = "-10.5,-55.5,340.0,0.4,0.4,%s,1750,SNPP,SNPP,n,2.0NRT,290.0,5.5,D";
    static final Set<String> APPROVED = Set.of("MODIS_C6.1", "VIIRS_S-NPP", "VIIRS_NOAA-20", "VIIRS_NOAA-21");

    /**
     * Simula o data lake entregando o dado JA CONVERTIDO: acrescenta a coluna h3 (hexadecimal) a cada linha.
     * O backend so a le; quem "converte" aqui e o teste, que faz o papel da fonte.
     */
    static String withH3(String csv) {
        String[] lines = csv.split("\n");
        StringBuilder sb = new StringBuilder(lines[0]).append(",h3\n");
        for (int i = 1; i < lines.length; i++) {
            if (lines[i].isBlank()) continue;
            sb.append(lines[i]).append(',').append(h3For(lines[i])).append('\n');
        }
        return sb.toString();
    }

    private static String h3For(String row) {
        try {
            String[] f = row.split(",");
            double lat = Double.parseDouble(f[0]);
            double lon = Double.parseDouble(f[1]);
            if (lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) return H3Index.toHex(TestH3.cell(lat, lon));
        } catch (RuntimeException ignored) {
            // linha invalida: a fonte nao teria H3 para ela
        }
        return "";
    }

    private static String rowAt(double lat, double lon) {
        return SRC_ROW.replace("-10.5,-55.5", lat + "," + lon).formatted("2026-09-15");
    }

    /** Fonte em memoria que simula o data lake e suas falhas. */
    static class MemorySource implements LakeSource {
        final Map<String, String> files = new LinkedHashMap<>();
        final Map<String, Manifest> manifests = new LinkedHashMap<>();
        IOException listFailure;
        IOException openFailure;
        IOException manifestFailure;
        /** true: entrega o texto como esta (o teste ja traz a coluna h3); false: acrescenta o H3 pronto. */
        boolean raw;

        @Override
        public List<String> listKeys(String prefix) throws IOException {
            if (listFailure != null) throw listFailure;
            return List.copyOf(files.keySet());
        }

        @Override
        public InputStream open(String key, DetectionCsvLoader.Filter filter) throws IOException {
            if (openFailure != null) throw openFailure;
            String text = raw ? files.get(key) : withH3(files.get(key));
            return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public Optional<Manifest> manifest(String csvKey) throws IOException {
            if (manifestFailure != null) throw manifestFailure;
            return Optional.ofNullable(manifests.get(csvKey));
        }
    }

    private ImportService service(MemorySource s) {
        return new ImportService(ingestionRepository, s, APPROVED);
    }

    private static ImportService.Request req(String day) {
        LocalDate d = LocalDate.parse(day);
        return new ImportService.Request(d, d, BBOX, Set.of("VIIRS"), null);
    }

    private static LakeSource.Manifest manifest(long rows, String min, String max) {
        return new LakeSource.Manifest(rows, LocalDate.parse(min), LocalDate.parse(max), "sha-" + rows);
    }

    private long countBySource(String key) {
        return jdbc.queryForObject("SELECT count(*) FROM detection WHERE source_file = ?", Long.class, key);
    }

    @Test
    @Order(7)
    void card_ca01_ca03_importacao_valida_normaliza_e_identifica_origem() throws Exception {
        var s = new MemorySource();
        String key = "bronze/raw/VIIRS_S-NPP/2026/fire_nrt_ok.csv";
        s.files.put(key, HEADER + "\n" + SRC_ROW.formatted("2026-09-10") + "\n"
                + SRC_ROW.replace("-10.5,-55.5", "-10.6,-55.4").formatted("2026-09-10") + "\n");
        s.manifests.put(key, manifest(2, "2026-09-10", "2026-09-10"));

        ImportService.Report report = service(s).run(req("2026-09-10"));

        // CA01: registros validos normalizados e disponiveis; arquivo conferido com o manifesto da fonte
        assertEquals(2, report.loaded());
        assertEquals(ImportService.Status.COMPLETED, report.files().get(0).status());
        assertTrue(report.files().get(0).verified());
        Map<String, Object> log = jdbc.queryForMap("""
                SELECT status, rows_loaded, expected_rows, source_sha256, verified
                FROM ingestion_log WHERE source_key = ?""", key);
        assertEquals("COMPLETED", log.get("status"));
        assertEquals(2L, ((Number) log.get("expected_rows")).longValue());
        assertEquals("sha-2", log.get("source_sha256"));
        assertEquals(true, log.get("verified"));

        // CA03: ao consultar, origem (produto e arquivo) e sensor estao identificados
        mvc.perform(get("/api/v1/detections").param("bbox", "-56,-11,-55,-10")
                        .param("start", "2026-09-10").param("end", "2026-09-10").param("zoom", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(2))
                .andExpect(jsonPath("$.points[0].sensor").value("VIIRS"))
                .andExpect(jsonPath("$.points[0].satellite").value("SNPP"))
                .andExpect(jsonPath("$.points[0].instrument").value("SNPP"))
                .andExpect(jsonPath("$.points[0].product").value("VIIRS_S-NPP"))
                .andExpect(jsonPath("$.points[0].sourceFile").value(key));

        // RN04: valores originais mantidos (FRP em MW, brilho em K) ao lado da confianca harmonizada
        Map<String, Object> row = jdbc.queryForMap("""
                SELECT frp, brightness, bright_t31, confidence_raw, confidence_class
                FROM detection WHERE source_file = ? ORDER BY lat LIMIT 1""", key);
        assertEquals(5.5f, ((Number) row.get("frp")).floatValue(), 1e-6);
        assertEquals(340.0f, ((Number) row.get("brightness")).floatValue(), 1e-6);
        assertEquals("n", row.get("confidence_raw"));
        assertEquals("n", row.get("confidence_class"));
        String comment = jdbc.queryForObject(
                "SELECT col_description('detection'::regclass, (SELECT attnum FROM pg_attribute "
                        + "WHERE attrelid = 'detection'::regclass AND attname = 'frp'))", String.class);
        assertTrue(comment.contains("megawatts"), "unidade do FRP documentada no esquema");
    }

    @Test
    @Order(8)
    void card_ca02_registro_invalido_e_rejeitado_com_motivo_registrado() throws Exception {
        var s = new MemorySource();
        String key = "bronze/raw/VIIRS_S-NPP/2026/fire_nrt_invalid.csv";
        s.files.put(key, HEADER + "\n" + SRC_ROW.formatted("2026-09-11") + "\n"
                + SRC_ROW.replace("-10.5,", "95,").formatted("2026-09-11") + "\n"      // coordenada invalida
                + SRC_ROW.formatted("2026-13-45") + "\n");                              // data invalida
        s.manifests.put(key, manifest(3, "2026-09-11", "2026-09-11"));

        ImportService.Report report = service(s).run(req("2026-09-11"));

        assertEquals(1, report.loaded());
        assertEquals(2, report.files().get(0).rejected());
        assertEquals(1L, countBySource(key), "so a deteccao valida e persistida");
        Map<String, Object> log = jdbc.queryForMap(
                "SELECT rows_rejected, rejects->>'latitude_invalida' AS lat, rejects->>'data_invalida' AS dt "
                        + "FROM ingestion_log WHERE source_key = ?", key);
        assertEquals(2L, ((Number) log.get("rows_rejected")).longValue());
        assertEquals("1", log.get("lat"));
        assertEquals("1", log.get("dt"));
    }

    @Test
    @Order(9)
    void card_ca04_fonte_indisponivel_falha_sem_criar_registros_ficticios() throws Exception {
        // arquivo indisponivel no momento da leitura
        var s = new MemorySource();
        String key = "bronze/raw/VIIRS_S-NPP/2026/fire_nrt_down.csv";
        s.files.put(key, HEADER + "\n" + SRC_ROW.formatted("2026-09-12") + "\n");
        s.manifests.put(key, manifest(1, "2026-09-12", "2026-09-12"));
        s.openFailure = new IOException("503 Service Unavailable");

        var ex = assertThrows(ImportService.ImportFailedException.class, () -> service(s).run(req("2026-09-12")));
        assertTrue(ex.getMessage().contains("NAO concluida"), ex.getMessage());
        assertEquals(0L, jdbc.queryForObject(
                "SELECT count(*) FROM detection WHERE acq_date = '2026-09-12'", Long.class),
                "nenhum registro e criado quando a fonte falha");
        Map<String, Object> log = jdbc.queryForMap(
                "SELECT status, error, rows_loaded FROM ingestion_log WHERE source_key = ?", key);
        assertEquals("FAILED", log.get("status"));
        assertTrue(log.get("error").toString().contains("503"));
        assertEquals(0L, ((Number) log.get("rows_loaded")).longValue());

        // fonte fora do ar ao listar os arquivos
        var down = new MemorySource();
        down.listFailure = new IOException("connection refused");
        assertThrows(ImportService.ImportFailedException.class, () -> service(down).run(req("2026-09-12")));
        assertEquals(1L, jdbc.queryForObject(
                "SELECT count(*) FROM ingestion_log WHERE source_key = '-' AND status = 'FAILED' "
                        + "AND error LIKE 'fonte indisponivel%'", Long.class));

        // manifesto indisponivel
        var noManifest = new MemorySource();
        noManifest.files.put(key, HEADER + "\n" + SRC_ROW.formatted("2026-09-12") + "\n");
        noManifest.manifestFailure = new IOException("timeout");
        assertThrows(ImportService.ImportFailedException.class, () -> service(noManifest).run(req("2026-09-12")));
        assertEquals(0L, countBySource(key));

        // a falha aparece na API de rastreabilidade, nao como importacao concluida
        mvc.perform(get("/api/v1/meta/sources"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.status=='FAILED')]").isNotEmpty());
    }

    @Test
    @Order(10)
    void card_rn06_resposta_incompleta_nao_vira_importacao_concluida() throws Exception {
        var s = new MemorySource();
        String key = "bronze/raw/VIIRS_S-NPP/2026/fire_nrt_truncated.csv";
        s.files.put(key, HEADER + "\n" + SRC_ROW.formatted("2026-09-13") + "\n"
                + SRC_ROW.replace("-10.5,-55.5", "-10.6,-55.4").formatted("2026-09-13") + "\n");
        s.manifests.put(key, manifest(3, "2026-09-13", "2026-09-13")); // a fonte declara 3 linhas; vieram 2

        var ex = assertThrows(ImportService.ImportFailedException.class, () -> service(s).run(req("2026-09-13")));
        assertTrue(ex.report().files().get(0).detail().contains("incompleta"), ex.report().files().get(0).detail());
        assertEquals(0L, countBySource(key), "importacao incompleta e desfeita por inteiro");
        assertEquals("FAILED", jdbc.queryForObject(
                "SELECT status FROM ingestion_log WHERE source_key = ?", String.class, key));
    }

    @Test
    @Order(11)
    void card_rn01_rn02_produto_aprovado_e_periodo_coberto_pela_fonte() throws Exception {
        LocalDate d = LocalDate.parse("2026-09-14");
        // RN01: produto fora da lista aprovada e recusado
        assertThrows(IllegalArgumentException.class, () -> service(new MemorySource())
                .run(new ImportService.Request(d, d, BBOX, Set.of("MODIS"), Set.of("MODIS_C5"))));

        // arquivos de produtos nao aprovados na pasta da fonte sao ignorados
        var other = new MemorySource();
        other.files.put("bronze/raw/MODIS_C5/2026/fire_nrt_x.csv", HEADER + "\n" + SRC_ROW.formatted("2026-09-14") + "\n");
        assertThrows(ImportService.ImportFailedException.class, () -> service(other).run(req("2026-09-14")));
        assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM detection WHERE acq_date = '2026-09-14'", Long.class));

        // RN02/RN06: a fonte so cobre ate julho; pedir setembro nao pode parecer importacao concluida
        var old = new MemorySource();
        String key = "bronze/raw/VIIRS_S-NPP/2026/fire_archive_old.csv";
        old.files.put(key, HEADER + "\n" + SRC_ROW.formatted("2026-07-01") + "\n");
        old.manifests.put(key, manifest(1, "2026-01-01", "2026-07-31"));
        var ex = assertThrows(ImportService.ImportFailedException.class, () -> service(old).run(req("2026-09-14")));
        assertTrue(ex.report().files().stream().anyMatch(f -> f.detail().contains("nenhum arquivo")));
        assertEquals(0L, countBySource(key));
    }

    @Test
    @Order(12)
    void h3_vem_pronto_da_fonte_e_nao_e_calculado_pelo_backend() throws Exception {
        var s = new MemorySource();
        s.raw = true; // o arquivo ja traz a coluna h3, como o data lake entregara
        String key = "bronze/raw/VIIRS_S-NPP/2026/fire_nrt_h3pronto.csv";
        long cellA = TestH3.cell(-10.5, -55.5);
        long cellB = TestH3.cell(-10.6, -55.4);
        String a = rowAt(-10.5, -55.5) + "," + H3Index.toHex(cellA);                       // hexadecimal
        String b = rowAt(-10.6, -55.4) + "," + cellB;                                       // inteiro decimal
        String c = rowAt(-10.7, -55.3) + ",";                                               // fonte sem H3 aqui
        String d = rowAt(-10.8, -55.2) + ",nao-e-h3";                                       // H3 invalido
        String e = rowAt(-10.9, -55.1) + "," + H3Index.toHex(TestH3.cell(-10.9, -55.1, 5)); // resolucao < 8
        s.files.put(key, HEADER + ",h3\n" + String.join("\n", a, b, c, d, e) + "\n");
        s.manifests.put(key, manifest(5, "2026-09-15", "2026-09-15"));

        ImportService.Report report = new ImportService(ingestionRepository, s, APPROVED).run(req("2026-09-15"));

        assertEquals(3, report.loaded());
        assertEquals(2, report.files().get(0).rejected());
        // gravado exatamente como veio da fonte
        assertEquals(cellA, jdbc.queryForObject(
                "SELECT h3 FROM detection WHERE source_file = ? AND abs(lat + 10.5) < 1e-9", Long.class, key));
        assertEquals(cellB, jdbc.queryForObject(
                "SELECT h3 FROM detection WHERE source_file = ? AND abs(lat + 10.6) < 1e-9", Long.class, key));
        // sem H3 na fonte: fica ausente, o backend nao calcula
        assertNull(jdbc.queryForObject(
                "SELECT h3 FROM detection WHERE source_file = ? AND abs(lat + 10.7) < 1e-9", Long.class, key));
        assertEquals("2", jdbc.queryForObject(
                "SELECT rejects->>'h3_invalido' FROM ingestion_log WHERE source_key = ?", String.class, key));
        // a agregacao por celula so considera os registros que trouxeram H3
        assertEquals(2L, jdbc.queryForObject(
                "SELECT sum(n) FROM daily_h3 WHERE res = 8 AND acq_date = '2026-09-15'", Long.class));
    }

    // ------------------------------------------------------------------------------------------------
    // Camada ouro: Parquet com um foco por linha e o H3 ja pronto
    // ------------------------------------------------------------------------------------------------

    /** Fonte que serve Parquet de uma pasta local, como a camada ouro do data lake. */
    static class LocalParquetSource implements LakeSource {
        final Map<String, Path> files = new LinkedHashMap<>();

        @Override
        public List<String> listKeys(String prefix) {
            return files.keySet().stream().filter(k -> k.startsWith(prefix)).toList();
        }

        @Override
        public InputStream open(String key, DetectionCsvLoader.Filter filter) throws IOException {
            return ParquetCsvReader.openAsCsv(files.get(key), filter);
        }

        @Override
        public Optional<Manifest> manifest(String key) throws IOException {
            var info = ParquetCsvReader.info(files.get(key));
            return Optional.of(new Manifest(info.rows(), info.minDate(), info.maxDate(), null, false));
        }
    }

    private static String pqRow(double lat, double lon, String date, String h3) {
        return lat + "," + lon + ",340.0,0.4,0.4," + date + ",1750,SNPP,SNPP,n,2.0NRT,290.0,5.5,D," + h3;
    }

    private static ImportService goldService(IngestionRepository repo, LakeSource src) {
        return new ImportService(repo, src, APPROVED, H3Options.DEFAULT, "gold/");
    }

    @Test
    @Order(13)
    void ouro_parquet_um_foco_por_linha_com_h3_pronto(@TempDir Path dir) throws Exception {
        String hexA = H3Index.toHex(TestH3.cell(-10.5, -55.5));
        String hexB = H3Index.toHex(TestH3.cell(-10.6, -55.4));
        String key = "gold/VIIRS_S-NPP/2026/focos.parquet";
        Path focos = ParquetFixtures.parquet(dir, key, HEADER + ",h3\n" + String.join("\n",
                pqRow(-10.5, -55.5, "2026-09-16", hexA),                                        // dentro
                pqRow(-10.6, -55.4, "2026-09-16", hexB),                                        // dentro
                pqRow(20.0, 10.0, "2026-09-16", H3Index.toHex(TestH3.cell(20.0, 10.0))),        // fora da regiao
                pqRow(-10.5, -55.5, "2026-10-20", hexA)) + "\n",                                // fora do periodo
                "{'acq_time': 'INTEGER'}");
        String oldKey = "gold/VIIRS_S-NPP/2026/antigo.parquet";
        Path antigo = ParquetFixtures.parquet(dir, oldKey, HEADER + ",h3\n"
                + pqRow(-10.5, -55.5, "2026-01-05", hexA) + "\n", "{'acq_time': 'INTEGER'}");
        var src = new LocalParquetSource();
        src.files.put(key, focos);
        src.files.put(oldKey, antigo);

        ImportService.Report report = goldService(ingestionRepository, src).run(req("2026-09-16"));

        var result = report.files().stream().filter(f -> f.key().equals(key)).findFirst().orElseThrow();
        assertEquals(ImportService.Status.COMPLETED, result.status());
        assertEquals(2, result.loaded());
        assertEquals(2, result.scanned(), "o filtro e aplicado na leitura: so os 2 focos pedidos sao lidos");
        assertTrue(result.verified());
        // arquivo fora da cobertura do periodo e pulado
        assertTrue(report.files().stream()
                .anyMatch(f -> f.key().equals(oldKey) && f.status() == ImportService.Status.SKIPPED));
        // conferencia pelo rodape do Parquet (4 linhas no arquivo), sem sha256
        Map<String, Object> log = jdbc.queryForMap(
                "SELECT status, expected_rows, source_sha256, verified FROM ingestion_log WHERE source_key = ?", key);
        assertEquals("COMPLETED", log.get("status"));
        assertEquals(4L, ((Number) log.get("expected_rows")).longValue());
        assertNull(log.get("source_sha256"));
        assertEquals(true, log.get("verified"));
        // o H3 foi gravado exatamente como veio no arquivo; origem aponta para a camada ouro
        assertEquals(TestH3.cell(-10.5, -55.5), jdbc.queryForObject(
                "SELECT h3 FROM detection WHERE source_file = ? AND abs(lat + 10.5) < 1e-9", Long.class, key));
        assertEquals(TestH3.cell(-10.6, -55.4), jdbc.queryForObject(
                "SELECT h3 FROM detection WHERE source_file = ? AND abs(lat + 10.6) < 1e-9", Long.class, key));
        mvc.perform(get("/api/v1/detections").param("bbox", "-56,-11,-55,-10")
                        .param("start", "2026-09-16").param("end", "2026-09-16").param("zoom", "10"))
                .andExpect(jsonPath("$.count").value(2))
                .andExpect(jsonPath("$.points[0].product").value("VIIRS_S-NPP"))
                .andExpect(jsonPath("$.points[0].sourceFile").value(key));
    }

    @Test
    @Order(14)
    void ouro_parquet_corrompido_falha_sem_criar_registros(@TempDir Path dir) throws Exception {
        String key = "gold/VIIRS_S-NPP/2026/quebrado.parquet";
        Path bad = dir.resolve(key);
        Files.createDirectories(bad.getParent());
        Files.writeString(bad, "isto nao e um arquivo parquet");
        var src = new LocalParquetSource();
        src.files.put(key, bad);

        var ex = assertThrows(ImportService.ImportFailedException.class,
                () -> goldService(ingestionRepository, src).run(req("2026-09-17")));

        assertTrue(ex.report().files().get(0).detail().contains("Parquet"), ex.report().files().get(0).detail());
        assertEquals(0L, countBySource(key));
        assertEquals("FAILED", jdbc.queryForObject(
                "SELECT status FROM ingestion_log WHERE source_key = ?", String.class, key));
    }

    @Test
    @Order(15)
    void ouro_com_uma_coluna_h3_por_resolucao_usa_base_6_e_agrega_de_0_a_6(@TempDir Path dir) throws Exception {
        var settings = new H3Settings(6);
        var repo6 = new IngestionRepository(dataSource, new com.fasterxml.jackson.databind.ObjectMapper(), settings);
        String cols = ",h3_0,h3_1,h3_2,h3_3,h3_4,h3_5,h3_6";
        java.util.function.BiFunction<Double, Double, String> h3cols = (lat, lon) -> {
            long c6 = TestH3.cell(lat, lon, 6);
            StringBuilder sb = new StringBuilder();
            for (int r = 0; r <= 6; r++) sb.append(',').append(H3Index.toHex(H3Index.parent(c6, r)));
            return sb.toString();
        };
        String key = "gold/VIIRS_S-NPP/2026/focos_h3_por_resolucao.parquet";
        Path file = ParquetFixtures.parquet(dir, key, HEADER + cols + "\n"
                + rowAt(-10.5, -55.5).replace("2026-09-15", "2026-09-18") + h3cols.apply(-10.5, -55.5) + "\n"
                + rowAt(-10.6, -55.4).replace("2026-09-15", "2026-09-18") + h3cols.apply(-10.6, -55.4) + "\n",
                "{'acq_time': 'INTEGER'}");
        var src = new LocalParquetSource();
        src.files.put(key, file);

        var report = new ImportService(repo6, src, APPROVED, new H3Options("h3", 6), "gold/").run(req("2026-09-18"));

        assertEquals(2, report.loaded());
        // o indice guardado e o da resolucao-base (6), exatamente como veio na coluna h3_6
        assertEquals(TestH3.cell(-10.5, -55.5, 6), jdbc.queryForObject(
                "SELECT h3 FROM detection WHERE source_file = ? AND abs(lat + 10.5) < 1e-9", Long.class, key));
        // agregacao diaria em todas as resolucoes de 0 a 6 (nao ha 7 nem 8)
        assertEquals(7, jdbc.queryForObject(
                "SELECT count(DISTINCT res) FROM daily_h3 WHERE acq_date = '2026-09-18'", Integer.class));
        assertEquals(0, jdbc.queryForObject(
                "SELECT count(*) FROM daily_h3 WHERE acq_date = '2026-09-18' AND res > 6", Integer.class));
        for (int res = 0; res <= 6; res++) {
            assertEquals(2L, jdbc.queryForObject(
                    "SELECT sum(n) FROM daily_h3 WHERE acq_date = '2026-09-18' AND res = ?", Long.class, res));
        }
        // consulta por celulas (regiao de resolucao 4) com a base 6: acha os 2 focos
        var repo = new DetectionRepository(new org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate(
                dataSource), settings);
        var region = Region.of(null, H3Index.toHex(H3Index.parent(TestH3.cell(-10.5, -55.5, 6), 4)));
        assertEquals(2, repo.findDetections(region, LocalDate.parse("2026-09-18"), LocalDate.parse("2026-09-18"),
                List.of("VIIRS"), 100).size());
    }

    // ------------------------------------------------------------------------------------------------
    // Card 3: comparacao entre periodos, rankings/regioes criticas e erros com parametros identificados
    // ------------------------------------------------------------------------------------------------

    private static String detRow(double lat, double lon, String date, String sat, String inst, String conf) {
        return lat + "," + lon + ",340.0,0.4,0.4," + date + ",1750," + sat + "," + inst + "," + conf
                + ",2.0NRT,290.0,5.5,D";
    }

    private String card3Cells;

    /** Dados de teste do card 3 (datas 20 a 28/09/2026), importados pelo mesmo caminho da importacao real. */
    private String seedCard3() throws Exception {
        if (card3Cells != null) return card3Cells;
        List<String> viirs = new java.util.ArrayList<>();
        List<String> modis = new java.util.ArrayList<>();
        double lat0 = -10.5;
        double lon0 = -55.5;
        // periodo A (20 a 22): VIIRS 2 + 1 + 0 = 3 | periodo B (23 a 26): VIIRS 3 + 3 + 2 + 0 = 8; MODIS so no B
        for (int i = 0; i < 2; i++) viirs.add(detRow(lat0, lon0, "2026-09-20", "SNPP", "SNPP", "n"));
        viirs.add(detRow(lat0, lon0, "2026-09-21", "SNPP", "SNPP", "n"));
        for (int i = 0; i < 3; i++) viirs.add(detRow(lat0, lon0, "2026-09-23", "SNPP", "SNPP", "n"));
        for (int i = 0; i < 3; i++) viirs.add(detRow(lat0, lon0, "2026-09-24", "SNPP", "SNPP", "n"));
        for (int i = 0; i < 2; i++) viirs.add(detRow(lat0, lon0, "2026-09-25", "SNPP", "SNPP", "n"));
        modis.add(detRow(lat0, lon0, "2026-09-24", "Aqua", "MODIS", "85"));
        // ranking (28/09): uma celula com 12 deteccoes (10 VIIRS + 2 MODIS) e 24 celulas com 1 VIIRS cada
        for (int i = 0; i < 10; i++) viirs.add(detRow(lat0, lon0, "2026-09-28", "SNPP", "SNPP", "n"));
        for (int i = 0; i < 2; i++) modis.add(detRow(lat0, lon0, "2026-09-28", "Aqua", "MODIS", "85"));
        java.util.Set<Long> cells8 = new java.util.HashSet<>();
        cells8.add(TestH3.cell(lat0, lon0));
        for (int i = 1; i <= 24; i++) {
            double lat = lat0 - 0.01 * i;
            viirs.add(detRow(lat, lon0, "2026-09-28", "SNPP", "SNPP", "n"));
            cells8.add(TestH3.cell(lat, lon0));
        }
        assertTrue(cells8.size() >= 21, "o conjunto de teste precisa de celulas suficientes: " + cells8.size());

        var s = new MemorySource();
        s.files.put("bronze/raw/VIIRS_S-NPP/2026/fire_nrt_card3.csv", HEADER + "\n" + String.join("\n", viirs) + "\n");
        s.files.put("bronze/raw/MODIS_C6.1/2026/fire_nrt_card3.csv", HEADER + "\n" + String.join("\n", modis) + "\n");
        var request = new ImportService.Request(LocalDate.parse("2026-09-20"), LocalDate.parse("2026-09-28"),
                new BBox(-60, -15, -50, -5), Set.of("MODIS", "VIIRS"), null);
        var report = service(s).run(request);
        assertEquals(viirs.size() + modis.size(), report.loaded());

        // regiao: as celulas de resolucao 3 que contem todos os pontos (a API so recebe H3, nunca converte bbox)
        java.util.Set<String> r3 = new java.util.LinkedHashSet<>();
        for (long c : cells8) r3.add(H3Index.toHex(H3Index.parent(c, 3)));
        card3Cells = String.join(",", r3);
        return card3Cells;
    }

    @Test
    @Order(16)
    void card3_comparacao_entre_periodos_so_calcula_com_dados_nos_dois() throws Exception {
        String cells = seedCard3();
        mvc.perform(get("/api/v1/compare/periods").param("cells", cells)
                        .param("startA", "2026-09-20").param("endA", "2026-09-22")
                        .param("startB", "2026-09-23").param("endB", "2026-09-26"))
                .andExpect(status().isOk())
                // VIIRS: A = 3 deteccoes em 3 dias (1,0/dia); B = 8 em 4 dias (2,0/dia) => +100%
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].sufficient").value(true))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].periodA.detections").value(3))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].periodB.detections").value(8))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].periodA.detectionsPerDay").value(1.0))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].periodB.detectionsPerDay").value(2.0))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].detectionsPerDayChangePct").value(100.0))
                // MODIS: sem registros no periodo A => nada de numeros inventados
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].sufficient").value(false))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].detectionsPerDayChangePct").isEmpty())
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].note")
                        .value(org.hamcrest.Matchers.hasItem(org.hamcrest.Matchers.containsString("periodo A"))))
                // RN04: recorte, fonte, unidade e metodo identificados
                .andExpect(jsonPath("$.meta.source").value(org.hamcrest.Matchers.containsString("FIRMS")))
                .andExpect(jsonPath("$.meta.units.frp").value("MW (megawatts)"))
                .andExpect(jsonPath("$.meta.method").value(org.hamcrest.Matchers.containsString("media")));

        // sem registros em nenhum periodo: resultado vazio e mensagem, sem numeros
        mvc.perform(get("/api/v1/compare/periods").param("cells", cells)
                        .param("startA", "2026-01-01").param("endA", "2026-01-03")
                        .param("startB", "2026-01-04").param("endB", "2026-01-06"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("Nenhum foco")))
                .andExpect(jsonPath("$.sensors[0].sufficient").value(false))
                .andExpect(jsonPath("$.sensors[0].detectionsPerDayChangePct").doesNotExist());

        // /compare e /timeseries tambem identificam fonte, unidade e metodo
        mvc.perform(get("/api/v1/compare").param("bbox", "-60,-15,-50,-5")
                        .param("start", "2026-09-20").param("end", "2026-09-28"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.meta.units.frp").value("MW (megawatts)"))
                .andExpect(jsonPath("$.meta.method").isString());
    }

    @Test
    @Order(17)
    void card3_ranking_e_regioes_criticas_por_celula_h3() throws Exception {
        String cells = seedCard3();
        mvc.perform(get("/api/v1/rankings/cells").param("cells", cells)
                        .param("start", "2026-09-28").param("end", "2026-09-28")
                        .param("resolution", "8").param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(5))
                .andExpect(jsonPath("$.items[0].rank").value(1))
                .andExpect(jsonPath("$.items[0].detections").value(12))
                .andExpect(jsonPath("$.items[0].viirsDetections").value(10))
                .andExpect(jsonPath("$.items[0].modisDetections").value(2))
                .andExpect(jsonPath("$.items[0].critical").value(true))        // 12 > percentil 95 (= 1)
                .andExpect(jsonPath("$.items[1].critical").value(false))       // 1 nao e maior que o percentil
                .andExpect(jsonPath("$.criticalThreshold").value(1.0))
                .andExpect(jsonPath("$.totalCells").value(org.hamcrest.Matchers.greaterThanOrEqualTo(20)))
                .andExpect(jsonPath("$.meta.method").value(org.hamcrest.Matchers.containsString("percentil 95")));

        // por FRP tambem funciona, e o campo h3 sai como veio (hexadecimal)
        mvc.perform(get("/api/v1/rankings/cells").param("cells", cells)
                        .param("start", "2026-09-28").param("end", "2026-09-28")
                        .param("resolution", "8").param("metric", "frp").param("limit", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].h3").value(H3Index.toHex(TestH3.cell(-10.5, -55.5))));

        // poucas celulas com registro: o limiar nao e calculado e nada e marcado como critico (RN05)
        mvc.perform(get("/api/v1/rankings/cells").param("cells", cells)
                        .param("start", "2026-09-20").param("end", "2026-09-22").param("resolution", "8"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.criticalThreshold").doesNotExist())
                .andExpect(jsonPath("$.items[0].critical").value(false))
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("poucas celulas")));

        // sem dados: vazio, sem numeros
        mvc.perform(get("/api/v1/rankings/cells").param("cells", cells)
                        .param("start", "2026-01-01").param("end", "2026-01-02"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(0))
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("Nenhum foco")));
    }

    @Test
    @Order(18)
    void card3_parametros_invalidos_listam_cada_parametro_rejeitado() throws Exception {
        String cells = seedCard3();
        // varios erros de uma vez: regiao, periodo A e sensor
        mvc.perform(get("/api/v1/compare/periods").param("bbox", "1,2,3")
                        .param("startA", "2026-09-22").param("endA", "2026-09-20")
                        .param("startB", "2026-09-23").param("endB", "2026-09-26").param("sensor", "X"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='bbox')]").isNotEmpty())
                .andExpect(jsonPath("$.errors[?(@.parameter=='endA')]").isNotEmpty())
                .andExpect(jsonPath("$.errors[?(@.parameter=='sensor')]").isNotEmpty());

        // parametros do ranking
        mvc.perform(get("/api/v1/rankings/cells").param("cells", cells)
                        .param("start", "2026-09-28").param("end", "2026-09-28")
                        .param("metric", "zzz").param("limit", "0").param("resolution", "99")
                        .param("criticalPercentile", "10"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.length()").value(4))
                .andExpect(jsonPath("$.errors[?(@.parameter=='metric')]").isNotEmpty())
                .andExpect(jsonPath("$.errors[?(@.parameter=='limit')]").isNotEmpty())
                .andExpect(jsonPath("$.errors[?(@.parameter=='resolution')]").isNotEmpty())
                .andExpect(jsonPath("$.errors[?(@.parameter=='criticalPercentile')]").isNotEmpty());

        // zoom e celulas invalidas
        mvc.perform(get("/api/v1/detections").param("cells", "xyz")
                        .param("start", "2026-09-28").param("end", "2026-09-28").param("zoom", "99"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='cells')]").isNotEmpty())
                .andExpect(jsonPath("$.errors[?(@.parameter=='zoom')]").isNotEmpty());

        // parametro obrigatorio ausente e valor com tipo errado, tambem com o nome do parametro
        mvc.perform(get("/api/v1/calendar").param("cells", cells))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].parameter").value("month"));
        mvc.perform(get("/api/v1/detections").param("cells", cells)
                        .param("start", "nao-e-data").param("end", "2026-09-28"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].parameter").value("start"));
    }

    @Test
    @Order(19)
    void card3_contagem_exata_por_sensor_com_meta_e_sem_numeros_inventados() throws Exception {
        String cells = seedCard3();
        mvc.perform(get("/api/v1/detections/count").param("cells", cells)
                        .param("start", "2026-09-20").param("end", "2026-09-28"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(48))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].detections").value(45))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].detections").value(3))
                .andExpect(jsonPath("$.meta.units.frp").value("MW (megawatts)"))
                .andExpect(jsonPath("$.meta.method").value(org.hamcrest.Matchers.containsString("Contagem exata")));
        mvc.perform(get("/api/v1/detections/count").param("cells", cells).param("sensor", "MODIS")
                        .param("start", "2026-09-20").param("end", "2026-09-28"))
                .andExpect(jsonPath("$.total").value(3))
                .andExpect(jsonPath("$.sensors.length()").value(1));
        mvc.perform(get("/api/v1/detections/count").param("cells", cells)
                        .param("start", "2026-01-01").param("end", "2026-01-02"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(0))
                .andExpect(jsonPath("$.sensors.length()").value(0))
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("Nenhum foco")));
        mvc.perform(get("/api/v1/detections/count").param("bbox", "1,2,3")
                        .param("start", "2026-09-22").param("end", "2026-09-20"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='bbox')]").isNotEmpty())
                .andExpect(jsonPath("$.errors[?(@.parameter=='end')]").isNotEmpty());
    }

    @Test
    @Order(20)
    void card3_correspondencias_modis_viirs_trazem_os_dois_lados_com_origem() throws Exception {
        String cells = seedCard3();
        mvc.perform(get("/api/v1/harmonized/matches").param("cells", cells)
                        .param("start", "2026-09-24").param("end", "2026-09-24"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.truncated").value(false))
                .andExpect(jsonPath("$.matches[0].modis.sensor").value("MODIS"))
                .andExpect(jsonPath("$.matches[0].viirs.sensor").value("VIIRS"))
                .andExpect(jsonPath("$.matches[0].distanceMeters").value(0.0))
                .andExpect(jsonPath("$.matches[0].modis.sourceFile").isString())
                .andExpect(jsonPath("$.matches[0].viirs.sourceFile").isString())
                .andExpect(jsonPath("$.method.limitations").isArray());
        // limit corta e sinaliza truncamento: 2 MODIS em 28/09
        mvc.perform(get("/api/v1/harmonized/matches").param("cells", cells)
                        .param("start", "2026-09-28").param("end", "2026-09-28").param("limit", "1"))
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.truncated").value(true));
        mvc.perform(get("/api/v1/harmonized/matches").param("cells", cells)
                        .param("start", "2026-01-01").param("end", "2026-01-02"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(0))
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("Nenhuma correspondencia")));
        mvc.perform(get("/api/v1/harmonized/matches").param("cells", cells)
                        .param("start", "2026-09-24").param("end", "2026-09-24").param("limit", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].parameter").value("limit"));
    }

    @Test
    @Order(21)
    void card3_comparacao_com_historico_exige_anos_suficientes() throws Exception {
        String cells = seedCard3();
        // mesmas datas (23 a 26/09) em 2025 (4), 2024 (4) e 2023 (5 deteccoes VIIRS)
        int[] perYear = {4, 4, 5};
        for (int i = 0; i < 3; i++) {
            int year = 2025 - i;
            List<String> rows = new java.util.ArrayList<>();
            for (int k = 0; k < perYear[i]; k++) {
                rows.add(detRow(-10.5, -55.5, year + "-09-" + (23 + (k % 4)), "SNPP", "SNPP", "n"));
            }
            var s = new MemorySource();
            s.files.put("bronze/raw/VIIRS_S-NPP/" + year + "/fire_archive_hist.csv",
                    HEADER + "\n" + String.join("\n", rows) + "\n");
            var req = new ImportService.Request(LocalDate.parse(year + "-09-23"), LocalDate.parse(year + "-09-26"),
                    new BBox(-60, -15, -50, -5), Set.of("VIIRS"), null);
            service(s).run(req);
        }
        // atual (2026): 8 deteccoes em 4 dias = 2,0/dia; historico 1,0 / 1,0 / 1,25 por dia
        mvc.perform(get("/api/v1/compare/historical").param("cells", cells)
                        .param("start", "2026-09-23").param("end", "2026-09-26").param("sensor", "VIIRS"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sensors[0].sufficient").value(true))
                .andExpect(jsonPath("$.sensors[0].yearsWithData").value(3))
                .andExpect(jsonPath("$.sensors[0].current.detectionsPerDay").value(2.0))
                .andExpect(jsonPath("$.sensors[0].meanDetectionsPerDay").value(org.hamcrest.Matchers.closeTo(1.0833, 0.001)))
                .andExpect(jsonPath("$.sensors[0].anomalous").value(true))
                .andExpect(jsonPath("$.sensors[0].zScore").value(org.hamcrest.Matchers.greaterThan(2.0)))
                .andExpect(jsonPath("$.sensors[0].changeVsMeanPct").value(org.hamcrest.Matchers.closeTo(84.6, 0.1)))
                .andExpect(jsonPath("$.meta.method").value(org.hamcrest.Matchers.containsString("3 anos")));
        // so 2 anos pedidos: insuficiente, sem media nem anomalia
        mvc.perform(get("/api/v1/compare/historical").param("cells", cells)
                        .param("start", "2026-09-23").param("end", "2026-09-26").param("sensor", "VIIRS")
                        .param("years", "2"))
                .andExpect(jsonPath("$.sensors[0].sufficient").value(false))
                .andExpect(jsonPath("$.sensors[0].meanDetectionsPerDay").doesNotExist())
                .andExpect(jsonPath("$.sensors[0].anomalous").doesNotExist())
                .andExpect(jsonPath("$.sensors[0].note").value(org.hamcrest.Matchers.containsString("insuficientes")));
        // MODIS tem registro so em 2026: sem historico, nada e inventado
        mvc.perform(get("/api/v1/compare/historical").param("cells", cells)
                        .param("start", "2026-09-23").param("end", "2026-09-26").param("sensor", "MODIS"))
                .andExpect(jsonPath("$.sensors[0].sufficient").value(false))
                .andExpect(jsonPath("$.sensors[0].yearsWithData").value(0));
        mvc.perform(get("/api/v1/compare/historical").param("cells", cells)
                        .param("start", "2026-09-23").param("end", "2026-09-26").param("years", "11"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='years')]").isNotEmpty());
    }
}
