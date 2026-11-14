package com.fireatlas.ingestion;

import com.fireatlas.gold.DbAvailable;
import com.fireatlas.gold.GoldFixtures;
import com.fireatlas.ingestion.repository.SilverIngestionRepository;
import com.fireatlas.ingestion.service.SilverImportService;
import com.fireatlas.region.entity.BBox;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Prata (Delta real em pasta local) -> tabela detection, contra PostGIS num schema isolado. */
@ExtendWith(DbAvailable.class)
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SilverImportAcceptanceTest {

    static final String SCHEMA = "it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    static final String SHA = "a".repeat(64);
    static final long CELL = 604000000000000001L;

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
    @Autowired SilverIngestionRepository repository;

    private SilverImportService service(Path dir) {
        return new SilverImportService(repository, new GoldFixtures.LocalStorage(dir), "prata/silver/");
    }

    private static SilverImportService.Request period(String s, String e, BBox bbox) {
        return new SilverImportService.Request(LocalDate.parse(s), LocalDate.parse(e), bbox);
    }

    private void clean() {
        jdbc.update("DELETE FROM detection");
        jdbc.update("DELETE FROM daily_grid");
        jdbc.update("DELETE FROM lake_import_log");
    }

    private static List<SilverFixtures.Row> rows() {
        return List.of(
                SilverFixtures.Row.modis(SHA, 1, "Aqua", -3.5, -53.5, "2026-11-12", "17:30:00", 12.5, 85, CELL),
                SilverFixtures.Row.viirs(SHA, 2, "NOAA-20", -3.501, -53.501, "2026-11-12", "17:40:00", 3.2, "n", CELL),
                SilverFixtures.Row.viirs(SHA, 3, "NOAA-21", -20.0, -40.0, "2026-11-12", "17:45:00", 1.0, "l", CELL + 1),
                SilverFixtures.Row.modis(SHA, 4, "Terra", -3.7, -53.7, "2026-11-12", "13:00:00", 5.0, 20, CELL),
                SilverFixtures.Row.viirs(SHA, 5, "NOAA-20", -3.8, -53.8, "2026-11-12", "13:10:00", -1.0, "h", CELL));
    }

    @Test
    @Order(1)
    void mapeia_satelite_confianca_brilho_h3_e_proveniencia_e_rejeita_frp_negativo(@TempDir Path dir)
            throws Exception {
        clean();
        SilverFixtures.write(dir, rows(), null);
        var report = service(dir).run(period("2026-11-12", "2026-11-12", null));

        assertEquals(4, report.inserted());
        var rejected = report.files().get(0).rejected();
        assertEquals(1, rejected);

        Map<String, Object> aqua = jdbc.queryForMap("SELECT * FROM detection WHERE record_id = ?", SHA + ":1");
        assertEquals("MODIS", aqua.get("sensor"));
        assertEquals("A", aqua.get("satellite"));
        assertEquals("85", aqua.get("confidence_raw"));
        assertEquals("h", aqua.get("confidence_class"));
        assertEquals(330.1f, ((Number) aqua.get("brightness")).floatValue(), 0.01);
        assertEquals(300.2f, ((Number) aqua.get("bright_t31")).floatValue(), 0.01);
        assertEquals(CELL, ((Number) aqua.get("h3_r6")).longValue());
        assertEquals(SHA, aqua.get("source_file_sha256"));
        assertEquals(1L, ((Number) aqua.get("source_line_number")).longValue());

        Map<String, Object> n20 = jdbc.queryForMap("SELECT * FROM detection WHERE record_id = ?", SHA + ":2");
        assertEquals("VIIRS", n20.get("sensor"));
        assertEquals("N20", n20.get("satellite"));
        assertEquals("n", n20.get("confidence_class"));
        assertEquals(340.0f, ((Number) n20.get("brightness")).floatValue(), 0.01);   // I-4
        assertEquals(290.0f, ((Number) n20.get("bright_t31")).floatValue(), 0.01);   // I-5

        assertEquals("N21", jdbc.queryForObject("SELECT satellite FROM detection WHERE record_id = ?",
                String.class, SHA + ":3"));
        assertEquals("T", jdbc.queryForObject("SELECT satellite FROM detection WHERE record_id = ?",
                String.class, SHA + ":4"));
        assertEquals("l", jdbc.queryForObject("SELECT confidence_class FROM detection WHERE record_id = ?",
                String.class, SHA + ":4"));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM detection WHERE record_id = ?",
                Integer.class, SHA + ":5"));
        assertTrue(jdbc.queryForObject("SELECT count(*) FROM daily_grid", Integer.class) > 0);

        Map<String, Object> logRow = jdbc.queryForMap("SELECT * FROM lake_import_log");
        assertEquals("COMPLETED", logRow.get("status"));
        assertEquals(1L, ((Number) logRow.get("rows_rejected")).longValue());
        assertTrue(logRow.get("rejects").toString().contains("frp_negativo"));
        assertEquals(Boolean.TRUE, logRow.get("verified"));
    }

    @Test
    @Order(2)
    void reimportar_nao_duplica_e_nao_mexe_nas_linhas_do_csv(@TempDir Path dir) throws Exception {
        clean();
        jdbc.update("""
                INSERT INTO detection (sensor, satellite, acq_date, acq_ts, lat, lon, source_product, source_file)
                VALUES ('MODIS', 'A', '2026-11-12', '2026-11-12T10:00:00Z', -3.0, -53.0, 'MODIS_C6.1', 'csv.csv')""");
        SilverFixtures.write(dir, rows(), null);
        assertEquals(4, service(dir).run(period("2026-11-12", "2026-11-12", null)).inserted());
        var again = service(dir).run(period("2026-11-12", "2026-11-12", null));
        assertEquals(0, again.inserted());
        assertEquals(4, again.files().get(0).unchanged());
        assertEquals(5, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertNull(jdbc.queryForObject("SELECT record_id FROM detection WHERE source_file = 'csv.csv'", String.class));
    }

    @Test
    @Order(3)
    void filtra_por_regiao_e_periodo(@TempDir Path dir) throws Exception {
        clean();
        SilverFixtures.write(dir, rows(), null);
        var inBbox = service(dir).run(period("2026-11-12", "2026-11-12", new BBox(-54, -4, -53, -3)));
        assertEquals(3, inBbox.inserted());   // exclui o ponto em -20,-40 (e o FRP negativo)
        clean();
        var ex = assertThrows(SilverImportService.ImportFailedException.class,
                () -> service(dir).run(period("2026-11-01", "2026-11-11", null)));
        assertTrue(ex.getMessage().contains("nenhum arquivo da prata cobre o periodo"));
    }

    @Test
    @Order(4)
    void numRecords_divergente_desfaz_o_arquivo_e_registra_falha(@TempDir Path dir) throws Exception {
        clean();
        SilverFixtures.write(dir, rows(), 99L);
        var ex = assertThrows(SilverImportService.ImportFailedException.class,
                () -> service(dir).run(period("2026-11-12", "2026-11-12", null)));
        assertTrue(ex.report().failed());
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM lake_import_log WHERE status = 'FAILED'",
                Integer.class));
    }

    @Test
    @Order(5)
    void harmonizacao_enxerga_os_focos_da_prata(@TempDir Path dir) throws Exception {
        clean();
        SilverFixtures.write(dir, rows(), null);
        service(dir).run(period("2026-11-12", "2026-11-12", null));
        mvc.perform(get("/api/v1/harmonized").param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12").param("end", "2026-11-13"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.summary.candidateAssociationsTotal").value(1));
    }
}
