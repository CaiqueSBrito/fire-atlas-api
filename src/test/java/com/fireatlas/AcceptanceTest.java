package com.fireatlas;

import com.fireatlas.h3.policy.H3Index;
import com.fireatlas.ingestion.repository.IngestionRepository;
import com.fireatlas.ingestion.service.GoldImportService;
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
    @Order(14)
    void harmonizacao_e_correspondencias_respondem_501_explicando_o_motivo() throws Exception {
        for (String path : List.of("/api/v1/harmonized", "/api/v1/harmonized/matches")) {
            mvc.perform(get(path))
                    .andExpect(status().isNotImplemented())
                    .andExpect(jsonPath("$.title").value(containsString("indisponivel")))
                    .andExpect(jsonPath("$.detail").value(containsString("deteccoes individuais")));
        }
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
}
