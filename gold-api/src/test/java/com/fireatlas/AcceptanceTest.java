package com.fireatlas;

import com.fireatlas.h3.policy.H3Index;
import com.fireatlas.ingestion.repository.IngestionRepository;
import com.fireatlas.ingestion.service.CatalogImportService;
import com.fireatlas.ingestion.service.GoldImportService;
import com.fireatlas.ingestion.service.HarmonizedImportService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Criterios de aceite contra um PostGIS real, em um schema temporario isolado (removido ao final; o banco do docker
 * compose precisa estar no ar). A fonte e uma tabela Delta de verdade (Parquet + _delta_log) gerada em uma pasta
 * local no formato da gold; o dado entra pelo mesmo caminho da importacao real.
 */
@ExtendWith(DbAvailable.class)
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AcceptanceTest {

    static final String SCHEMA = "it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    static final String TABLE = "gold/map_grid/";

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
    @Autowired JdbcTemplate jdbc;
    @Autowired IngestionRepository ingestionRepository;

    private GoldImportService service(GoldFixtures.LocalStorage storage) {
        return new GoldImportService(ingestionRepository, storage, TABLE);
    }

    private static GoldImportService.Request period(String start, String end) {
        return new GoldImportService.Request(LocalDate.parse(start), LocalDate.parse(end), null);
    }

    private void clean() {
        jdbc.update("DELETE FROM daily_h3");
        jdbc.update("DELETE FROM ingestion_log");
    }

    @Test
    @Order(1)
    void sobe_sem_erro_e_o_esquema_so_tem_a_grade() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM daily_h3", Integer.class));
        // a bronze deixou de existir: nao ha tabela de deteccoes individuais
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM information_schema.tables "
                + "WHERE table_schema = ? AND table_name = 'detection'", Integer.class, SCHEMA));
        mvc.perform(get("/api/v1/meta/sensors")).andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.code=='MODIS')].nominalResolutionMeters").value(1000))
                .andExpect(jsonPath("$[?(@.code=='VIIRS')].nominalResolutionMeters").value(375));
    }

    // ------------------------------------------------------------------------------------------------
    // Importacao da gold (Delta Lake)
    // ------------------------------------------------------------------------------------------------

    @Test
    @Order(2)
    void importa_so_os_arquivos_ativos_do_delta_com_proveniencia_e_e_idempotente(@TempDir Path dir)
            throws Exception {
        clean();
        long cell = TestH3.cell(-10.5, -55.5, 3);
        var table = GoldFixtures.table(dir, TABLE)
                .append(List.of(GoldFixtures.Row.viirs(3, cell, "2026-09-02", 1, 5.5)))     // v0
                .overwrite(3, 2026, List.of(GoldFixtures.Row.viirs(3, cell, "2026-09-03", 4, 5.5),
                        GoldFixtures.Row.modis(3, cell, "2026-09-03", 2, 25.5)));            // v1 substitui a particao
        var storage = new GoldFixtures.LocalStorage(dir);

        var report = service(storage).run(period("2026-09-01", "2026-09-30"));
        assertFalse(report.failed());
        // so o arquivo ativo (v1) foi lido: o dia 02/09 do arquivo removido nao existe
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM daily_h3 WHERE acq_date = '2026-09-02'",
                Integer.class));
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM daily_h3", Integer.class));
        assertEquals(4L, jdbc.queryForObject("SELECT n FROM daily_h3 WHERE sensor = 'VIIRS'", Long.class));
        // valores originais preservados (FRP somado e maximo, confianca)
        assertEquals(22.0, jdbc.queryForObject("SELECT frp_sum FROM daily_h3 WHERE sensor = 'VIIRS'", Double.class));
        assertEquals(80.0, jdbc.queryForObject("SELECT modis_conf_mean FROM daily_h3 WHERE sensor = 'MODIS'",
                Double.class));
        // proveniencia: particao e versao do Delta; e conferencia com o log
        assertEquals("gold/map_grid/resolution=3/year=2026@v1", jdbc.queryForObject(
                "SELECT DISTINCT source_key FROM daily_h3", String.class));
        var log = jdbc.queryForMap("SELECT * FROM ingestion_log WHERE status = 'COMPLETED'");
        assertEquals("map_grid", log.get("product"));
        assertEquals(2L, log.get("rows_loaded"));
        assertEquals(2L, log.get("expected_rows"));
        assertEquals(true, log.get("verified"));

        // idempotente: importar de novo nao duplica
        service(storage).run(period("2026-09-01", "2026-09-30"));
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM daily_h3", Integer.class));
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM ingestion_log WHERE status = 'COMPLETED'",
                Integer.class));
        assertEquals(table.tableDir().resolve("_delta_log").toFile().isDirectory(), true);
    }

    @Test
    @Order(3)
    void linhas_invalidas_sao_rejeitadas_com_motivo_e_contadas(@TempDir Path dir) throws Exception {
        clean();
        long c3 = TestH3.cell(-10.5, -55.5, 3);
        long c4 = TestH3.cell(-10.5, -55.5, 4);
        GoldFixtures.table(dir, TABLE).append(List.of(
                GoldFixtures.Row.viirs(3, c3, "2026-09-02", 3, 5.5),
                new GoldFixtures.Row(3, c3, "2026-09-03", "FOO", 1, 1, 1, 0, 1, 0, null),
                GoldFixtures.Row.viirs(3, c4, "2026-09-04", 1, 5.5),
                new GoldFixtures.Row(3, c3, "2026-09-05", "VIIRS", 2, -4.0, 2, 0, 2, 0, null)));
        service(new GoldFixtures.LocalStorage(dir)).run(period("2026-09-01", "2026-09-30"));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM daily_h3", Integer.class));
        var log = jdbc.queryForMap("SELECT rows_scanned, rows_loaded, rows_rejected, rejects::text AS rejects "
                + "FROM ingestion_log WHERE status = 'COMPLETED'");
        assertEquals(4L, log.get("rows_scanned"));
        assertEquals(1L, log.get("rows_loaded"));
        assertEquals(3L, log.get("rows_rejected"));
        assertTrue(log.get("rejects").toString().contains("sensor_invalido"));
        assertTrue(log.get("rejects").toString().contains("h3_invalido"));
        assertTrue(log.get("rejects").toString().contains("frp_negativo"));
    }

    @Test
    @Order(4)
    void falhas_da_fonte_ficam_registradas_nada_e_gravado_e_a_execucao_falha(@TempDir Path dir) throws Exception {
        clean();
        long cell = TestH3.cell(-10.5, -55.5, 3);
        var table = GoldFixtures.table(dir, TABLE)
                .append(List.of(GoldFixtures.Row.viirs(3, cell, "2026-09-02", 1, 5.5)));

        // arquivo de dados corrompido (o log esta ok)
        table.corruptDataFiles();
        var ex = assertThrows(GoldImportService.ImportFailedException.class,
                () -> service(new GoldFixtures.LocalStorage(dir)).run(period("2026-09-01", "2026-09-30")));
        assertTrue(ex.report().failed());
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM daily_h3", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM ingestion_log WHERE status = 'FAILED'",
                Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM ingestion_log WHERE status = 'COMPLETED'",
                Integer.class));

        // fonte fora do ar ao ler o log do Delta
        clean();
        var down = new GoldFixtures.LocalStorage(dir).failingOn(k -> k.contains("_delta_log"));
        assertThrows(GoldImportService.ImportFailedException.class,
                () -> service(down).run(period("2026-09-01", "2026-09-30")));
        var failed = jdbc.queryForMap("SELECT status, error FROM ingestion_log");
        assertEquals("FAILED", failed.get("status"));
        assertTrue(failed.get("error").toString().contains("fonte indisponivel"), failed.get("error").toString());

        // nada cobre o pedido: nao pode parecer uma importacao concluida
        clean();
        var good = GoldFixtures.table(dir.resolve("ok"), TABLE)
                .append(List.of(GoldFixtures.Row.viirs(3, cell, "2026-09-02", 1, 5.5)));
        var none = assertThrows(GoldImportService.ImportFailedException.class,
                () -> service(new GoldFixtures.LocalStorage(dir.resolve("ok"))).run(period("2024-01-01", "2024-01-31")));
        assertTrue(none.getMessage().contains("NAO concluida"));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM ingestion_log WHERE status = 'FAILED'",
                Integer.class));
        assertEquals(good.tableDir().toFile().isDirectory(), true);

        // argumentos invalidos
        assertThrows(IllegalArgumentException.class, () -> period("2026-09-30", "2026-09-01"));
        assertThrows(IllegalArgumentException.class, () -> new GoldImportService.Request(
                LocalDate.parse("2026-09-01"), LocalDate.parse("2026-09-30"), Set.of(7)));
    }

    // ------------------------------------------------------------------------------------------------
    // Dados de teste da API: celula A (varios dias), 24 celulas com 1 deteccao e 3 anos de historico
    // ------------------------------------------------------------------------------------------------

    private String regionCells;
    private boolean seeded;

    /** Gera a gold de teste (20 a 28/09/2026 + 23 a 26/09 de 2023 a 2025) e importa pelo caminho real. */
    private String seed() throws Exception {
        if (seeded) return regionCells;
        clean();
        double lat0 = -10.5;
        double lon0 = -55.5;
        List<GoldFixtures.Row> rows = new ArrayList<>();
        // periodo A (20 a 22): VIIRS 2 + 1 + 0 = 3 | periodo B (23 a 26): VIIRS 3 + 3 + 2 + 0 = 8; MODIS 1 em 24/09
        int[][] viirsDays = {{20, 2}, {21, 1}, {23, 3}, {24, 3}, {25, 2}, {28, 10}};
        for (int[] d : viirsDays) {
            rows.addAll(GoldFixtures.allResolutions(lat0, lon0, "2026-09-" + d[0], "VIIRS", d[1], 5.5));
        }
        rows.addAll(GoldFixtures.allResolutions(lat0, lon0, "2026-09-24", "MODIS", 1, 25.5));
        rows.addAll(GoldFixtures.allResolutions(lat0, lon0, "2026-09-28", "MODIS", 2, 25.5));
        // Portugal (Europa): fora de regionCells, so entra nas consultas por pais/continente
        rows.addAll(GoldFixtures.allResolutions(38.7, -9.1, "2026-09-20", "VIIRS", 2, 5.5));
        rows.addAll(GoldFixtures.allResolutions(38.7, -9.1, "2026-09-28", "VIIRS", 5, 5.5));
        Set<String> parents = new LinkedHashSet<>();
        parents.add(H3Index.toHex(TestH3.cell(lat0, lon0, 3)));
        // ranking (28/09): a celula A tem 12 deteccoes (10 VIIRS + 2 MODIS); 24 celulas distantes tem 1 VIIRS cada
        for (int i = 1; i <= 24; i++) {
            double lat = lat0 - 0.2 * i;
            rows.addAll(GoldFixtures.allResolutions(lat, lon0, "2026-09-28", "VIIRS", 1, 5.5));
            parents.add(H3Index.toHex(TestH3.cell(lat, lon0, 3)));
        }
        // historico (mesmas datas 23 a 26/09): 2025 = 4, 2024 = 4, 2023 = 5 deteccoes VIIRS
        int[][] history = {{2025, 1, 1, 1, 1}, {2024, 1, 1, 1, 1}, {2023, 2, 1, 1, 1}};
        for (int[] h : history) {
            for (int k = 0; k < 4; k++) {
                rows.addAll(GoldFixtures.allResolutions(lat0, lon0, h[0] + "-09-" + (23 + k), "VIIRS", h[1 + k], 5.5));
            }
        }
        Path dir = java.nio.file.Files.createTempDirectory("fireatlas-it-gold-");
        GoldFixtures.table(dir, TABLE).append(rows);
        var report = service(new GoldFixtures.LocalStorage(dir)).run(period("2023-01-01", "2026-12-31"));
        assertFalse(report.failed());
        assertTrue(report.files().stream().filter(f -> f.status() == GoldImportService.Status.COMPLETED)
                .allMatch(GoldImportService.FileResult::verified));
        regionCells = String.join(",", parents);
        seeded = true;
        return regionCells;
    }

    @Test
    @Order(5)
    void grade_de_celulas_h3_por_zoom_sem_pontos_individuais() throws Exception {
        String cells = seed();
        mvc.perform(get("/api/v1/detections").param("cells", cells).param("start", "2026-09-20")
                        .param("end", "2026-09-22").param("zoom", "3"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.mode").value("grid"))
                .andExpect(jsonPath("$.resolution").value(3))
                .andExpect(jsonPath("$.count").value(3))
                .andExpect(jsonPath("$.cells[0].h3").value(H3Index.toHex(TestH3.cell(-10.5, -55.5, 3))))
                .andExpect(jsonPath("$.cells[0].sensor").value("VIIRS"))
                .andExpect(jsonPath("$.points").doesNotExist());
        // zoom alto: a gold so vai ate a resolucao 6, e a celula (res 3) e expandida para ela
        mvc.perform(get("/api/v1/detections").param("cells", cells).param("start", "2026-09-20")
                        .param("end", "2026-09-22").param("zoom", "15"))
                .andExpect(jsonPath("$.resolution").value(6))
                .andExpect(jsonPath("$.count").value(3));
        // sem dados: vazio e mensagem, sem numeros
        mvc.perform(get("/api/v1/detections").param("cells", cells).param("start", "2026-01-01")
                        .param("end", "2026-01-02"))
                .andExpect(jsonPath("$.cells.length()").value(0))
                .andExpect(jsonPath("$.message").value(containsString("Nenhum foco")));
    }

    @Test
    @Order(6)
    void contagem_por_sensor_com_meta_e_sem_numeros_inventados() throws Exception {
        String cells = seed();
        mvc.perform(get("/api/v1/detections/count").param("cells", cells)
                        .param("start", "2026-09-20").param("end", "2026-09-28"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(48))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].detections").value(45))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].detections").value(3))
                .andExpect(jsonPath("$.meta.units.frp").value("MW (megawatts)"))
                .andExpect(jsonPath("$.meta.source").value(containsString("gold/map_grid")))
                .andExpect(jsonPath("$.meta.method").value(containsString("Soma das deteccoes")));
        mvc.perform(get("/api/v1/detections/count").param("cells", cells).param("sensor", "MODIS")
                        .param("start", "2026-09-20").param("end", "2026-09-28"))
                .andExpect(jsonPath("$.total").value(3))
                .andExpect(jsonPath("$.sensors.length()").value(1));
        mvc.perform(get("/api/v1/detections/count").param("cells", cells)
                        .param("start", "2026-01-01").param("end", "2026-01-02"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(0))
                .andExpect(jsonPath("$.sensors.length()").value(0))
                .andExpect(jsonPath("$.message").value(containsString("Nenhum foco")));
    }

    @Test
    @Order(7)
    void calendario_diario_separa_modis_e_viirs() throws Exception {
        String cells = seed();
        mvc.perform(get("/api/v1/calendar").param("cells", cells).param("month", "2026-09"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.days.length()").value(30))
                .andExpect(jsonPath("$.daysWithFire").value(6))
                .andExpect(jsonPath("$.days[23].viirs.count").value(3))   // 24/09
                .andExpect(jsonPath("$.days[23].modis.count").value(1));
    }

    @Test
    @Order(8)
    void compara_sensores_com_confianca_do_viirs_e_media_do_modis_sem_mediana() throws Exception {
        String cells = seed();
        mvc.perform(get("/api/v1/compare").param("cells", cells)
                        .param("start", "2026-09-20").param("end", "2026-09-28"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].detections").value(45))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].frpMean").value(5.5))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].frpMax").value(6.5))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].confidence.nominal").value(45))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].nominalResolutionMeters").value(375))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].detections").value(3))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].frpMean").value(25.5))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].confidenceMean").value(80.0))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].confidence").isEmpty())
                .andExpect(jsonPath("$.sensors[0].frpMedian").doesNotExist())
                .andExpect(jsonPath("$.notice").value(containsString("NAO sao diretamente comparaveis")))
                .andExpect(jsonPath("$.meta.method").value(containsString("Nao ha mediana")));
    }

    @Test
    @Order(9)
    void comparacao_entre_periodos_so_calcula_com_dados_nos_dois() throws Exception {
        String cells = seed();
        mvc.perform(get("/api/v1/compare/periods").param("cells", cells)
                        .param("startA", "2026-09-20").param("endA", "2026-09-22")
                        .param("startB", "2026-09-23").param("endB", "2026-09-26"))
                .andExpect(status().isOk())
                // VIIRS: A = 3 deteccoes em 3 dias (1,0/dia); B = 8 em 4 dias (2,0/dia) => +100%
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].sufficient").value(true))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].periodA.detections").value(3))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].periodB.detections").value(8))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].detectionsPerDayChangePct").value(100.0))
                // MODIS: sem registros no periodo A => nada de numeros inventados
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].sufficient").value(false))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].detectionsPerDayChangePct").isEmpty())
                .andExpect(jsonPath("$.sensors[?(@.sensor=='MODIS')].note").value(hasItem(containsString("periodo A"))))
                .andExpect(jsonPath("$.meta.source").value(containsString("FIRMS")));
        mvc.perform(get("/api/v1/compare/periods").param("cells", cells)
                        .param("startA", "2026-01-01").param("endA", "2026-01-03")
                        .param("startB", "2026-01-04").param("endB", "2026-01-06"))
                .andExpect(jsonPath("$.message").value(containsString("Nenhum foco")))
                .andExpect(jsonPath("$.sensors[0].sufficient").value(false))
                .andExpect(jsonPath("$.sensors[0].detectionsPerDayChangePct").doesNotExist());
    }

    @Test
    @Order(10)
    void comparacao_com_historico_usa_os_anos_da_gold_e_exige_anos_suficientes() throws Exception {
        String cells = seed();
        // atual (2026): 8 deteccoes em 4 dias = 2,0/dia; historico 1,0 / 1,0 / 1,25 por dia
        mvc.perform(get("/api/v1/compare/historical").param("cells", cells)
                        .param("start", "2026-09-23").param("end", "2026-09-26").param("sensor", "VIIRS"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sensors[0].sufficient").value(true))
                .andExpect(jsonPath("$.sensors[0].yearsWithData").value(3))
                .andExpect(jsonPath("$.sensors[0].current.detectionsPerDay").value(2.0))
                .andExpect(jsonPath("$.sensors[0].meanDetectionsPerDay").value(closeTo(1.0833, 0.001)))
                .andExpect(jsonPath("$.sensors[0].anomalous").value(true))
                .andExpect(jsonPath("$.sensors[0].zScore").value(greaterThan(2.0)))
                .andExpect(jsonPath("$.sensors[0].changeVsMeanPct").value(closeTo(84.6, 0.1)));
        mvc.perform(get("/api/v1/compare/historical").param("cells", cells)
                        .param("start", "2026-09-23").param("end", "2026-09-26").param("sensor", "VIIRS")
                        .param("years", "2"))
                .andExpect(jsonPath("$.sensors[0].sufficient").value(false))
                .andExpect(jsonPath("$.sensors[0].meanDetectionsPerDay").doesNotExist())
                .andExpect(jsonPath("$.sensors[0].anomalous").doesNotExist())
                .andExpect(jsonPath("$.sensors[0].note").value(containsString("insuficientes")));
        // MODIS so tem registro em 2026: sem historico, nada e inventado
        mvc.perform(get("/api/v1/compare/historical").param("cells", cells)
                        .param("start", "2026-09-23").param("end", "2026-09-26").param("sensor", "MODIS"))
                .andExpect(jsonPath("$.sensors[0].sufficient").value(false))
                .andExpect(jsonPath("$.sensors[0].yearsWithData").value(0));
    }

    @Test
    @Order(11)
    void serie_temporal_e_ranking_com_regioes_criticas() throws Exception {
        String cells = seed();
        mvc.perform(get("/api/v1/timeseries").param("cells", cells)
                        .param("start", "2026-09-20").param("end", "2026-09-28").param("sensor", "VIIRS"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.series.length()").value(9))
                .andExpect(jsonPath("$.series[8].count").value(34))     // 28/09: 10 + 24
                .andExpect(jsonPath("$.series[8].unusual").value(true))
                .andExpect(jsonPath("$.meta.method").value(containsString("z-score robusto")));

        mvc.perform(get("/api/v1/rankings/cells").param("cells", cells)
                        .param("start", "2026-09-28").param("end", "2026-09-28")
                        .param("resolution", "6").param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(5))
                .andExpect(jsonPath("$.items[0].rank").value(1))
                .andExpect(jsonPath("$.items[0].detections").value(12))
                .andExpect(jsonPath("$.items[0].viirsDetections").value(10))
                .andExpect(jsonPath("$.items[0].modisDetections").value(2))
                .andExpect(jsonPath("$.items[0].critical").value(true))
                .andExpect(jsonPath("$.items[1].critical").value(false))
                .andExpect(jsonPath("$.items[0].h3").value(H3Index.toHex(TestH3.cell(-10.5, -55.5, 6))))
                .andExpect(jsonPath("$.totalCells").value(greaterThanOrEqualTo(20)));
        // poucas celulas com registro: limiar nao calculado, nada e marcado como critico (RN05)
        mvc.perform(get("/api/v1/rankings/cells").param("cells", cells)
                        .param("start", "2026-09-20").param("end", "2026-09-22").param("resolution", "6"))
                .andExpect(jsonPath("$.criticalThreshold").doesNotExist())
                .andExpect(jsonPath("$.items[0].critical").value(false))
                .andExpect(jsonPath("$.message").value(containsString("poucas celulas")));
    }

    @Test
    @Order(12)
    void bbox_e_rejeitado_com_mensagem_clara_em_toda_consulta_agregada() throws Exception {
        for (String path : List.of("/api/v1/detections", "/api/v1/detections/count", "/api/v1/compare")) {
            mvc.perform(get(path).param("bbox", "-56,-11,-55,-10").param("start", "2026-09-20")
                            .param("end", "2026-09-28"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.errors[?(@.parameter=='bbox')].message")
                            .value(hasItem(containsString("cells"))));
        }
    }

    @Test
    @Order(13)
    void parametros_invalidos_listam_cada_parametro_rejeitado() throws Exception {
        String cells = seed();
        mvc.perform(get("/api/v1/compare/periods").param("bbox", "1,2,3")
                        .param("startA", "2026-09-22").param("endA", "2026-09-20")
                        .param("startB", "2026-09-23").param("endB", "2026-09-26").param("sensor", "X"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='bbox')]").isNotEmpty())
                .andExpect(jsonPath("$.errors[?(@.parameter=='endA')]").isNotEmpty())
                .andExpect(jsonPath("$.errors[?(@.parameter=='sensor')]").isNotEmpty());
        mvc.perform(get("/api/v1/rankings/cells").param("cells", cells)
                        .param("start", "2026-09-28").param("end", "2026-09-28")
                        .param("metric", "zzz").param("limit", "0").param("resolution", "99")
                        .param("criticalPercentile", "10"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.length()").value(4));
        mvc.perform(get("/api/v1/compare/historical").param("cells", cells)
                        .param("start", "2026-09-23").param("end", "2026-09-26").param("years", "11"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='years')]").isNotEmpty());
        mvc.perform(get("/api/v1/detections").param("cells", "xyz")
                        .param("start", "2026-09-28").param("end", "2026-09-28").param("zoom", "99"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='cells')]").isNotEmpty())
                .andExpect(jsonPath("$.errors[?(@.parameter=='zoom')]").isNotEmpty());
        mvc.perform(get("/api/v1/calendar").param("cells", cells))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].parameter").value("month"));
        mvc.perform(get("/api/v1/detections").param("cells", cells)
                        .param("start", "nao-e-data").param("end", "2026-09-28"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].parameter").value("start"));
    }

    @Test
    @Order(15)
    void metadados_de_cobertura_e_de_fontes_importadas() throws Exception {
        seed();
        mvc.perform(get("/api/v1/meta/coverage"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.sensor=='VIIRS')].minDate").value("2023-09-23"))
                .andExpect(jsonPath("$[?(@.sensor=='VIIRS')].maxDate").value("2026-09-28"));
        mvc.perform(get("/api/v1/meta/sources"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].product").value("map_grid"))
                .andExpect(jsonPath("$[0].status").value("COMPLETED"))
                .andExpect(jsonPath("$[0].verified").value(true))
                .andExpect(jsonPath("$[0].expectedRows").isNumber());
    }

    // ------------------------------------------------------------------------------------------------
    // Lugares (gold/h3_places) e previsao (gold/forecast)
    // ------------------------------------------------------------------------------------------------

    private boolean catalogSeeded;

    /** Pontos de teste: A e 24 celulas ao sul (Brasil; cidade 111 ate 12 e cidade 222 a 30 km nas demais) e Portugal. */
    private void seedCatalog() throws Exception {
        seed();
        if (catalogSeeded) return;
        jdbc.update("DELETE FROM h3_place");
        jdbc.update("DELETE FROM forecast");
        double lat0 = -10.5;
        double lon0 = -55.5;
        java.util.Map<String, CatalogFixtures.PlaceRow> places = new java.util.LinkedHashMap<>();
        for (int i = 0; i <= 24; i++) {
            double lat = i == 0 ? lat0 : lat0 - 0.2 * i;
            long cityId = i <= 12 ? 111 : 222;
            double km = i <= 12 ? 4.0 + i : 30.0;
            for (int res = 0; res <= 6; res++) {
                long cell = TestH3.cell(lat, lon0, res);
                boolean fine = res >= 5;
                places.putIfAbsent(res + "|" + cell, new CatalogFixtures.PlaceRow(res, cell, "South America", "BRA",
                        "BR", "Brazil", "Brasil", 1.0, fine ? cityId : null, fine ? "Cidade " + cityId : null,
                        fine ? "BR" : null, fine ? km : null));
            }
        }
        for (int res = 0; res <= 6; res++) {
            long cell = TestH3.cell(38.7, -9.1, res);
            places.putIfAbsent(res + "|" + cell, new CatalogFixtures.PlaceRow(res, cell, "Europe", "PRT", "PT",
                    "Portugal", "Portugal", 0.8, null, null, null, null));
        }
        // celula do oceano: sem pais nem continente
        long ocean = TestH3.cell(-30, -20, 4);
        places.putIfAbsent("4|" + ocean, new CatalogFixtures.PlaceRow(4, ocean, null, null, null, null, null, null,
                null, null, null, null));
        Path dir = java.nio.file.Files.createTempDirectory("fireatlas-it-catalog-");
        CatalogFixtures.writePlaces(dir, new ArrayList<>(places.values()));

        List<CatalogFixtures.ForecastRow> fc = new ArrayList<>();
        fc.add(new CatalogFixtures.ForecastRow("2026-09-28", TestH3.cell(lat0, lon0, 6), TestH3.cell(lat0, lon0, 1),
                0.90, true, 12, 65));
        fc.add(new CatalogFixtures.ForecastRow("2026-09-28", TestH3.cell(lat0 - 0.2, lon0, 6),
                TestH3.cell(lat0 - 0.2, lon0, 1), 0.40, false, 1, 4));
        fc.add(new CatalogFixtures.ForecastRow("2026-09-28", TestH3.cell(lat0 - 0.4, lon0, 6),
                TestH3.cell(lat0 - 0.4, lon0, 1), 0.10, false, 0, 1));
        fc.add(new CatalogFixtures.ForecastRow("2026-09-28", TestH3.cell(38.7, -9.1, 6), TestH3.cell(38.7, -9.1, 1),
                0.70, true, 5, 20));
        fc.add(new CatalogFixtures.ForecastRow("2026-09-27", TestH3.cell(lat0, lon0, 6), TestH3.cell(lat0, lon0, 1),
                0.50, true, 3, 30));
        CatalogFixtures.writeForecast(dir, fc);

        var catalog = new CatalogImportService(ingestionRepository, new GoldFixtures.LocalStorage(dir));
        assertFalse(catalog.importPlaces(null).failed());
        assertFalse(catalog.importForecast().failed());
        catalogSeeded = true;
    }

    @Test
    @Order(16)
    void importa_lugares_e_previsao_conferidos_com_o_log_do_delta() throws Exception {
        seedCatalog();
        assertEquals(3, jdbc.queryForObject("SELECT count(DISTINCT product) FROM ingestion_log "
                + "WHERE status = 'COMPLETED' AND product IN ('h3_places', 'forecast', 'map_grid')", Integer.class));
        assertEquals(true, jdbc.queryForObject("SELECT bool_and(verified) FROM ingestion_log WHERE product IN "
                + "('h3_places', 'forecast')", Boolean.class));
        // celula sem pais (oceano) e valida e fica sem pais
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM h3_place WHERE country_iso_a2 IS NULL",
                Integer.class));
        // idempotente: reimportar nao duplica
        long places = jdbc.queryForObject("SELECT count(*) FROM h3_place", Long.class);
        long forecasts = jdbc.queryForObject("SELECT count(*) FROM forecast", Long.class);
        catalogSeeded = false;
        seedCatalog();
        assertEquals(places, jdbc.queryForObject("SELECT count(*) FROM h3_place", Long.class));
        assertEquals(forecasts, jdbc.queryForObject("SELECT count(*) FROM forecast", Long.class));
    }

    @Test
    @Order(17)
    void catalogo_de_lugares_lista_continentes_paises_e_cidades() throws Exception {
        seedCatalog();
        mvc.perform(get("/api/v1/places/continents"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(2))
                .andExpect(jsonPath("$.items[?(@.name=='Europe')].countries").value(1))
                .andExpect(jsonPath("$.note").value(containsString("fronteiras")));
        // o nome do continente vale em portugues e sem acento
        mvc.perform(get("/api/v1/places/countries").param("continent", "America do Sul"))
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.items[0].isoA2").value("BR"))
                .andExpect(jsonPath("$.items[0].isoA3").value("BRA"))
                .andExpect(jsonPath("$.items[0].namePt").value("Brasil"));
        mvc.perform(get("/api/v1/places/cities").param("country", "BR"))
                .andExpect(jsonPath("$.items[0].geonameId").value(111))
                .andExpect(jsonPath("$.items.length()").value(1))      // a 222 esta a 30 km: fora do raio de 25 km
                .andExpect(jsonPath("$.note").value(containsString("MAIS PROXIMA")));
        mvc.perform(get("/api/v1/places/cities").param("cityRadiusKm", "50").param("q", "222"))
                .andExpect(jsonPath("$.items[0].geonameId").value(222));
        mvc.perform(get("/api/v1/places/countries").param("continent", "Atlantis"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='continent')]").isNotEmpty());
    }

    @Test
    @Order(18)
    void consultas_por_pais_continente_e_cidade_juntam_a_grade_com_h3_place() throws Exception {
        seedCatalog();
        String period = "&start=2026-09-20&end=2026-09-28";
        // Brasil = a celula A e as 24 ao sul (48 deteccoes); Portugal = 2 + 5
        mvc.perform(get("/api/v1/detections/count?country=BR" + period))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(48))
                .andExpect(jsonPath("$.meta.region").value("pais:BR"));
        mvc.perform(get("/api/v1/detections/count?country=pt" + period))
                .andExpect(jsonPath("$.total").value(7));
        mvc.perform(get("/api/v1/detections/count?country=PRT" + period))   // o codigo de 3 letras tambem vale
                .andExpect(jsonPath("$.total").value(7));
        mvc.perform(get("/api/v1/detections/count?continent=Europa" + period))
                .andExpect(jsonPath("$.total").value(7))
                .andExpect(jsonPath("$.meta.region").value("continente:Europe"));
        mvc.perform(get("/api/v1/detections/count?continent=south america" + period))
                .andExpect(jsonPath("$.total").value(48));
        // cidade: usa a resolucao 5; 111 (ate 16 km) tem A e as 12 primeiras; 222 esta a 30 km
        mvc.perform(get("/api/v1/detections/count?city=111" + period))
                .andExpect(jsonPath("$.total").value(36));       // A (24) + 12
        mvc.perform(get("/api/v1/detections/count?city=222" + period))
                .andExpect(jsonPath("$.total").value(0))
                .andExpect(jsonPath("$.message").value(containsString("Nenhum foco")));
        mvc.perform(get("/api/v1/detections/count?city=222&cityRadiusKm=50" + period))
                .andExpect(jsonPath("$.total").value(12));
        // as outras consultas aceitam o mesmo filtro
        mvc.perform(get("/api/v1/calendar?country=BR&month=2026-09"))
                .andExpect(jsonPath("$.daysWithFire").value(6));
        mvc.perform(get("/api/v1/compare?country=BR" + period))
                .andExpect(jsonPath("$.sensors[?(@.sensor=='VIIRS')].detections").value(45));
        mvc.perform(get("/api/v1/rankings/cells?country=BR&start=2026-09-28&end=2026-09-28&resolution=6&limit=1"))
                .andExpect(jsonPath("$.items[0].detections").value(12))
                .andExpect(jsonPath("$.items[0].h3").value(H3Index.toHex(TestH3.cell(-10.5, -55.5, 6))));
        mvc.perform(get("/api/v1/detections?continent=Europe&zoom=4&start=2026-09-28&end=2026-09-28"))
                .andExpect(jsonPath("$.count").value(5))
                .andExpect(jsonPath("$.resolution").value(4));
    }

    @Test
    @Order(19)
    void regiao_exige_um_unico_criterio_e_valida_cada_parametro_de_lugar() throws Exception {
        seedCatalog();
        String cells = regionCells;
        String period = "&start=2026-09-20&end=2026-09-28";
        mvc.perform(get("/api/v1/detections/count?country=BR&cells=" + cells + period))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].message").value(containsString("somente um")));
        mvc.perform(get("/api/v1/detections/count?start=2026-09-20&end=2026-09-28"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/detections/count?country=Brasil" + period))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='country')]").isNotEmpty());
        mvc.perform(get("/api/v1/detections/count?city=abc" + period))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='city')]").isNotEmpty());
        mvc.perform(get("/api/v1/detections/count?city=111&cityRadiusKm=9999" + period))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='cityRadiusKm')]").isNotEmpty());
    }

    @Test
    @Order(20)
    void previsao_lista_as_celulas_da_regiao_da_maior_para_a_menor_probabilidade() throws Exception {
        seedCatalog();
        mvc.perform(get("/api/v1/forecast/issues"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(2))
                .andExpect(jsonPath("$.issues[0].issueDate").value("2026-09-28"))
                .andExpect(jsonPath("$.issues[0].cells").value(4))
                .andExpect(jsonPath("$.issues[0].activeCells").value(2));
        // sem issueDate: a emissao mais recente; so o Brasil, em ordem de probabilidade
        mvc.perform(get("/api/v1/forecast?country=BR"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.issueDate").value("2026-09-28"))
                .andExpect(jsonPath("$.count").value(3))
                .andExpect(jsonPath("$.items[0].h3").value(H3Index.toHex(TestH3.cell(-10.5, -55.5, 6))))
                .andExpect(jsonPath("$.items[0].probability7d").value(closeTo(0.90, 0.001)))
                .andExpect(jsonPath("$.items[0].region").value(H3Index.toHex(TestH3.cell(-10.5, -55.5, 1))))
                .andExpect(jsonPath("$.items[0].detections7d").value(65.0))
                .andExpect(jsonPath("$.items[0].modisBrightnessMean7d").doesNotExist())   // nulo na fonte: nada inventado
                .andExpect(jsonPath("$.items[1].probability7d").value(closeTo(0.40, 0.001)))
                .andExpect(jsonPath("$.method").value(containsString("nao recalcula")))
                .andExpect(jsonPath("$.source").value(containsString("gold/forecast")));
        mvc.perform(get("/api/v1/forecast?continent=Europe&minProbability=0.5"))
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.items[0].probability7d").value(closeTo(0.70, 0.001)));
        mvc.perform(get("/api/v1/forecast?country=BR&onlyActive=true"))
                .andExpect(jsonPath("$.count").value(1));
        mvc.perform(get("/api/v1/forecast?country=BR&limit=2"))
                .andExpect(jsonPath("$.count").value(2))
                .andExpect(jsonPath("$.truncated").value(true));
        // emissao anterior e emissao inexistente
        mvc.perform(get("/api/v1/forecast?country=BR&issueDate=2026-09-27"))
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.items[0].probability7d").value(closeTo(0.50, 0.001)));
        mvc.perform(get("/api/v1/forecast?country=BR&issueDate=2026-01-01"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(0))
                .andExpect(jsonPath("$.message").value(containsString("2026-09-28")));
        // por celulas H3: qualquer resolucao vira as celulas de resolucao 6 dentro dela
        mvc.perform(get("/api/v1/forecast").param("cells", H3Index.toHex(TestH3.cell(-10.5, -55.5, 3))))
                .andExpect(jsonPath("$.count").value(3));
        mvc.perform(get("/api/v1/forecast?country=BR&minProbability=2&limit=0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.length()").value(2));
        mvc.perform(get("/api/v1/forecast?bbox=-56,-11,-55,-10"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @Order(21)
    void importacao_de_lugares_rejeita_linhas_invalidas_e_falha_sem_gravar(@TempDir Path dir) throws Exception {
        seedCatalog();
        long cell3 = TestH3.cell(-10.5, -55.5, 3);
        long cell4 = TestH3.cell(-10.5, -55.5, 4);
        CatalogFixtures.writePlaces(dir, List.of(
                new CatalogFixtures.PlaceRow(3, cell3, "South America", "BRA", "BR", "Brazil", "Brasil", 1.0,
                        null, null, null, null),
                new CatalogFixtures.PlaceRow(3, cell4, "South America", "BRA", "BR", "Brazil", "Brasil", 1.0,
                        null, null, null, null),   // celula de outra resolucao
                new CatalogFixtures.PlaceRow(3, TestH3.cell(0, 0, 3), "Africa", "GAB", "GA", "Gabon", "Gabao", 1.7,
                        null, null, null, null))); // fracao fora de 0 a 1
        var svc = new CatalogImportService(ingestionRepository, new GoldFixtures.LocalStorage(dir));
        svc.importPlaces(java.util.Set.of(3));
        var log = jdbc.queryForMap("SELECT rows_scanned, rows_loaded, rows_rejected, rejects::text AS rejects "
                + "FROM ingestion_log WHERE product = 'h3_places' ORDER BY id DESC LIMIT 1");
        assertEquals(3L, log.get("rows_scanned"));
        assertEquals(1L, log.get("rows_loaded"));
        assertTrue(log.get("rejects").toString().contains("h3_invalido"));
        assertTrue(log.get("rejects").toString().contains("share_invalido"));
        catalogSeeded = false; // a resolucao 3 foi trocada pelo teste: recarrega o catalogo completo

        // fonte sem a tabela Delta: falha registrada e erro (saida diferente de zero)
        var empty = new GoldFixtures.LocalStorage(dir.resolve("nao-existe"));
        assertThrows(GoldImportService.ImportFailedException.class,
                () -> new CatalogImportService(ingestionRepository, empty).importForecast());
        assertEquals("FAILED", jdbc.queryForObject("SELECT status FROM ingestion_log ORDER BY id DESC LIMIT 1",
                String.class));
        seedCatalog();
    }

    // ------------------------------------------------------------------------------------------------
    // Atividade de fogo harmonizada (gold/harmonized)
    // ------------------------------------------------------------------------------------------------

    private boolean activitySeeded;

    private static String two(int d) {
        return d < 10 ? "0" + d : String.valueOf(d);
    }

    /**
     * Celula A (Brasil) em setembro de 2023, 2024 e 2025 com 1,0 / 2,0 / 3,0 por dia (somas 30, 60 e 90; mediana 60) e
     * setembro de 2026 com 2,0 por dia, menos os dias 10 a 12 (contributing = 0, sem dado): soma 54 em 27 dias.
     * Agosto de 2026 com 1,5 por dia. Portugal so em setembro de 2026 (5,0 por dia). Niveis 3 e 4.
     */
    private void seedActivity() throws Exception {
        seedCatalog();
        if (activitySeeded) return;
        jdbc.update("DELETE FROM harmonized_cell");
        jdbc.update("DELETE FROM harmonized_availability");
        List<CatalogFixtures.HarmRow> rows = new ArrayList<>();
        int[] years = {2023, 2024, 2025};
        for (int res : new int[]{3, 4}) {
            long a = TestH3.cell(-10.5, -55.5, res);
            for (int i = 0; i < years.length; i++) {
                for (int d = 1; d <= 30; d++) {
                    rows.add(new CatalogFixtures.HarmRow(res, a, years[i] + "-09-" + two(d), i + 1.0, 2));
                }
            }
            for (int d = 1; d <= 30; d++) {
                boolean none = d >= 10 && d <= 12;
                rows.add(new CatalogFixtures.HarmRow(res, a, "2026-09-" + two(d), none ? 0.0 : 2.0, none ? 0 : 3));
            }
            for (int d = 1; d <= 31; d++) {
                rows.add(new CatalogFixtures.HarmRow(res, a, "2026-08-" + two(d), 1.5, 3));
            }
        }
        long p4 = TestH3.cell(38.7, -9.1, 4);
        for (int d = 1; d <= 30; d++) rows.add(new CatalogFixtures.HarmRow(4, p4, "2026-09-" + two(d), 5.0, 2));
        Path dir = java.nio.file.Files.createTempDirectory("fireatlas-it-harmonized-");
        CatalogFixtures.writeHarmonized(dir, rows);
        CatalogFixtures.writeAvailability(dir, List.of(
                new CatalogFixtures.AvailRow("2026-09-01", "terra", 120, 100.0, true, true),
                new CatalogFixtures.AvailRow("2026-09-01", "aqua", 130, 110.0, true, true),
                new CatalogFixtures.AvailRow("2026-09-01", "snpp", 0, 90.0, false, false),
                new CatalogFixtures.AvailRow("2026-09-02", "aqua", 125, 110.0, true, true)));
        var svc = new HarmonizedImportService(ingestionRepository, new GoldFixtures.LocalStorage(dir));
        var cells = svc.importCells(LocalDate.parse("2023-01-01"), LocalDate.parse("2026-12-31"), null);
        assertFalse(cells.failed());
        assertTrue(cells.files().stream().allMatch(GoldImportService.FileResult::verified));
        assertFalse(svc.importAvailability(LocalDate.parse("2023-01-01"), LocalDate.parse("2026-12-31")).failed());
        activitySeeded = true;
    }

    @Test
    @Order(22)
    void importa_atividade_harmonizada_com_contributing_zero_valido_e_idempotente() throws Exception {
        seedActivity();
        assertEquals(true, jdbc.queryForObject("SELECT bool_and(verified) FROM ingestion_log WHERE product IN "
                + "('harmonized_cells', 'harmonized_availability')", Boolean.class));
        // contributing = 0 e valido (sem dado), nao e rejeicao: 3 dias x 2 niveis
        assertEquals(6, jdbc.queryForObject("SELECT count(*) FROM harmonized_cell WHERE contributing = 0",
                Integer.class));
        long cells = jdbc.queryForObject("SELECT count(*) FROM harmonized_cell", Long.class);
        long avail = jdbc.queryForObject("SELECT count(*) FROM harmonized_availability", Long.class);
        assertEquals(4L, avail);
        activitySeeded = false;
        seedActivity();
        assertEquals(cells, jdbc.queryForObject("SELECT count(*) FROM harmonized_cell", Long.class));
        assertEquals(avail, jdbc.queryForObject("SELECT count(*) FROM harmonized_availability", Long.class));
    }

    @Test
    @Order(23)
    void calendario_harmonizado_traz_rotulo_avisos_e_dia_sem_dado_sem_numero() throws Exception {
        seedActivity();
        String cell3 = H3Index.toHex(TestH3.cell(-10.5, -55.5, 3));
        mvc.perform(get("/api/v1/activity/calendar").param("cells", cell3).param("month", "2026-09"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.days.length()").value(30))
                .andExpect(jsonPath("$.daysWithData").value(27))
                .andExpect(jsonPath("$.days[0].harmonized").value(2.0))
                .andExpect(jsonPath("$.days[0].contributing").value(3))
                .andExpect(jsonPath("$.days[9].hasData").value(false))         // dia 10: contributing = 0
                .andExpect(jsonPath("$.days[9].contributing").value(0))
                .andExpect(jsonPath("$.days[9].harmonized").doesNotExist())   // sem dado nao e zero fogo
                .andExpect(jsonPath("$.meta.resolution").value(3))
                .andExpect(jsonPath("$.meta.unitLabelPt").value(containsString("equivalentes ao MODIS Aqua")))
                .andExpect(jsonPath("$.meta.unitLabelEn")
                        .value("Harmonized fire activity (MODIS Aqua-equivalent active cells)"))
                .andExpect(jsonPath("$.meta.warnings").value(hasItem(containsString("contributing = 0"))))
                .andExpect(jsonPath("$.meta.warnings").value(hasItem(containsString("nao e numero de focos"))))
                .andExpect(jsonPath("$.meta.warnings").value(hasItem(containsString("dia local"))));
        // por pais: junta com h3_place na resolucao 4
        mvc.perform(get("/api/v1/activity/calendar?country=BR&month=2026-09"))
                .andExpect(jsonPath("$.meta.resolution").value(4))
                .andExpect(jsonPath("$.meta.region").value("pais:BR"))
                .andExpect(jsonPath("$.days[0].harmonized").value(2.0));
        mvc.perform(get("/api/v1/activity/calendar?country=PT&month=2026-09"))
                .andExpect(jsonPath("$.days[0].harmonized").value(5.0));
        // mes sem registro: vazio, mensagem e nenhum numero
        mvc.perform(get("/api/v1/activity/calendar").param("cells", cell3).param("month", "2020-01"))
                .andExpect(jsonPath("$.daysWithData").value(0))
                .andExpect(jsonPath("$.message").value(containsString("Sem dados")))
                .andExpect(jsonPath("$.days[0].harmonized").doesNotExist());
    }

    @Test
    @Order(24)
    void somas_mensais_ficam_em_ordem_e_marcam_mes_completo_e_dias_com_dado() throws Exception {
        seedActivity();
        String cell3 = H3Index.toHex(TestH3.cell(-10.5, -55.5, 3));
        mvc.perform(get("/api/v1/activity/monthly").param("cells", cell3).param("start", "2023-09")
                        .param("end", "2026-09"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.months.length()").value(5))
                .andExpect(jsonPath("$.months[0].month").value("2023-09"))
                .andExpect(jsonPath("$.months[0].harmonized").value(30.0))
                .andExpect(jsonPath("$.months[0].complete").value(true))
                .andExpect(jsonPath("$.months[2].harmonized").value(90.0))
                .andExpect(jsonPath("$.months[3].month").value("2026-08"))
                .andExpect(jsonPath("$.months[3].harmonized").value(46.5))
                .andExpect(jsonPath("$.months[4].month").value("2026-09"))
                .andExpect(jsonPath("$.months[4].harmonized").value(54.0))
                .andExpect(jsonPath("$.months[4].daysWithData").value(27))
                .andExpect(jsonPath("$.months[4].daysInMonth").value(30))
                .andExpect(jsonPath("$.meta.unitLabelPt").isString());
        mvc.perform(get("/api/v1/activity/monthly").param("cells", cell3).param("start", "2026-09")
                        .param("end", "2026-08"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/activity/monthly").param("cells", cell3).param("start", "2000-01")
                        .param("end", "2026-09"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='end')]").isNotEmpty());
    }

    @Test
    @Order(25)
    void anomalia_compara_com_a_mediana_do_mesmo_mes_e_exige_anos_suficientes() throws Exception {
        seedActivity();
        String cell3 = H3Index.toHex(TestH3.cell(-10.5, -55.5, 3));
        // 2026-09: 54 contra a mediana de 30, 60 e 90 = 60  =>  razao 0,9 e desvio de -10%
        mvc.perform(get("/api/v1/activity/anomaly").param("cells", cell3).param("month", "2026-09"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sufficient").value(true))
                .andExpect(jsonPath("$.current").value(54.0))
                .andExpect(jsonPath("$.currentDaysWithData").value(27))
                .andExpect(jsonPath("$.yearsWithData").value(3))
                .andExpect(jsonPath("$.baselineMedian").value(60.0))
                .andExpect(jsonPath("$.ratioToMedian").value(closeTo(0.9, 1e-9)))
                .andExpect(jsonPath("$.deviationPct").value(closeTo(-10.0, 1e-9)))
                .andExpect(jsonPath("$.baseline[0].year").value(2025))
                .andExpect(jsonPath("$.baseline[0].harmonized").value(90.0))
                .andExpect(jsonPath("$.meta.warnings").value(hasItem(containsString("nao e numero de focos"))));
        // so 2 anos pedidos (2024 e 2025): insuficiente, sem mediana nem desvio inventados
        mvc.perform(get("/api/v1/activity/anomaly").param("cells", cell3).param("month", "2026-09")
                        .param("years", "2"))
                .andExpect(jsonPath("$.sufficient").value(false))
                .andExpect(jsonPath("$.yearsWithData").value(2))
                .andExpect(jsonPath("$.baselineMedian").doesNotExist())
                .andExpect(jsonPath("$.deviationPct").doesNotExist())
                .andExpect(jsonPath("$.note").value(containsString("insuficientes")));
        // Portugal so tem 2026: sem historico
        mvc.perform(get("/api/v1/activity/anomaly?country=PT&month=2026-09"))
                .andExpect(jsonPath("$.current").value(150.0))
                .andExpect(jsonPath("$.sufficient").value(false))
                .andExpect(jsonPath("$.yearsWithData").value(0));
        // mes sem dado: nao ha numero atual
        mvc.perform(get("/api/v1/activity/anomaly").param("cells", cell3).param("month", "2025-01"))
                .andExpect(jsonPath("$.current").doesNotExist())
                .andExpect(jsonPath("$.sufficient").value(false))
                .andExpect(jsonPath("$.note").value(containsString("sem dado")));
        mvc.perform(get("/api/v1/activity/anomaly").param("cells", cell3).param("month", "2026-09")
                        .param("years", "11"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='years')]").isNotEmpty());
        mvc.perform(get("/api/v1/activity/anomaly").param("cells", cell3).param("month", "2026-9"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='month')]").isNotEmpty());
    }

    @Test
    @Order(26)
    void disponibilidade_dos_satelites_filtra_por_satelite_e_valida_o_nome() throws Exception {
        seedActivity();
        mvc.perform(get("/api/v1/activity/availability?start=2026-09-01&end=2026-09-30"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(4))
                .andExpect(jsonPath("$.days[0].platform").value("aqua"))
                .andExpect(jsonPath("$.days[?(@.platform=='snpp')].available").value(false));
        mvc.perform(get("/api/v1/activity/availability?start=2026-09-01&end=2026-09-30&platform=AQUA"))
                .andExpect(jsonPath("$.count").value(2))
                .andExpect(jsonPath("$.days[0].units").value(130));
        mvc.perform(get("/api/v1/activity/availability?start=2026-09-01&end=2026-09-30&platform=landsat"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='platform')]").isNotEmpty());
        mvc.perform(get("/api/v1/activity/availability?start=2020-01-01&end=2020-01-31"))
                .andExpect(jsonPath("$.count").value(0))
                .andExpect(jsonPath("$.message").value(containsString("Sem registro")));
    }

    @Test
    @Order(27)
    void area_pequena_avisa_e_bbox_e_rejeitado() throws Exception {
        seedActivity();
        // uma celula de resolucao 6 (~36 km2) e uma area pequena: o aviso do erro mensal entra na resposta
        String cell6 = H3Index.toHex(TestH3.cell(-10.5, -55.5, 6));
        mvc.perform(get("/api/v1/activity/calendar").param("cells", cell6).param("month", "2026-09"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.meta.smallArea").value(true))
                .andExpect(jsonPath("$.meta.warnings").value(hasItem(containsString("Area pequena"))))
                .andExpect(jsonPath("$.message").value(containsString("Sem dados")));
        mvc.perform(get("/api/v1/activity/calendar?bbox=-56,-11,-55,-10&month=2026-09"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[?(@.parameter=='bbox')]").isNotEmpty());
    }

    @Test
    @Order(28)
    void importacao_da_atividade_rejeita_linhas_invalidas_e_falha_sem_gravar(@TempDir Path dir) throws Exception {
        seedActivity();
        long c3 = TestH3.cell(-10.5, -55.5, 3);
        long c4 = TestH3.cell(-10.5, -55.5, 4);
        CatalogFixtures.writeHarmonized(dir, List.of(
                new CatalogFixtures.HarmRow(3, c3, "2026-09-01", 2.0, 3),
                new CatalogFixtures.HarmRow(3, c4, "2026-09-02", 2.0, 3),    // celula de outra resolucao
                new CatalogFixtures.HarmRow(3, c3, "2026-09-03", -1.0, 3),   // harmonizado negativo
                new CatalogFixtures.HarmRow(3, c3, "2026-09-04", 2.0, 9)));  // 9 satelites nao existem
        var svc = new HarmonizedImportService(ingestionRepository, new GoldFixtures.LocalStorage(dir));
        svc.importCells(LocalDate.parse("2026-09-01"), LocalDate.parse("2026-09-30"), java.util.Set.of(3));
        var log = jdbc.queryForMap("SELECT rows_scanned, rows_loaded, rows_rejected, rejects::text AS rejects "
                + "FROM ingestion_log WHERE product = 'harmonized_cells' ORDER BY id DESC LIMIT 1");
        assertEquals(4L, log.get("rows_scanned"));
        assertEquals(1L, log.get("rows_loaded"));
        assertTrue(log.get("rejects").toString().contains("h3_invalido"));
        assertTrue(log.get("rejects").toString().contains("harmonizado_invalido"));
        assertTrue(log.get("rejects").toString().contains("contribuintes_invalidos"));
        activitySeeded = false; // o teste trocou setembro de 2026 no nivel 3: recarrega

        // fonte sem a tabela Delta: falha registrada e erro (saida diferente de zero)
        var empty = new GoldFixtures.LocalStorage(dir.resolve("nao-existe"));
        assertThrows(GoldImportService.ImportFailedException.class, () -> new HarmonizedImportService(
                ingestionRepository, empty).importAvailability(LocalDate.parse("2026-09-01"),
                LocalDate.parse("2026-09-30")));
        assertEquals("FAILED", jdbc.queryForObject("SELECT status FROM ingestion_log ORDER BY id DESC LIMIT 1",
                String.class));
        assertThrows(IllegalArgumentException.class, () -> svc.importCells(LocalDate.parse("2026-09-30"),
                LocalDate.parse("2026-09-01"), null));
        seedActivity();
    }
}
