package com.fireatlas.ingestion;

import com.fireatlas.gold.DbAvailable;
import com.fireatlas.gold.GoldFixtures;
import com.fireatlas.ingestion.repository.SilverIngestionRepository;
import com.fireatlas.ingestion.service.SilverImportService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Card 1 (CA02 e CA04): deteccao consultada com sensor e fonte; derivados nao alteram o original. */
@ExtendWith(DbAvailable.class)
@SpringBootTest
@AutoConfigureMockMvc
class DetectionContractAcceptanceTest {

    static final String SCHEMA = "it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    static final String SHA = "b".repeat(64);

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

    @Test
    void consulta_traz_sensor_e_fonte_e_derivados_nao_alteram_o_original(@TempDir Path dir) throws Exception {
        SilverFixtures.write(dir, List.of(
                SilverFixtures.Row.modis(SHA, 1, "Aqua", -3.5, -53.5, "2026-11-12", "17:30:00", 12.5, 85, 1L),
                SilverFixtures.Row.viirs(SHA, 2, "NOAA-20", -3.501, -53.501, "2026-11-12", "17:40:00", 3.2, "n", 1L)),
                null);
        new SilverImportService(repository, new GoldFixtures.LocalStorage(dir), "prata/silver/")
                .run(new SilverImportService.Request(LocalDate.parse("2026-11-12"), LocalDate.parse("2026-11-12"),
                        null));

        // CA02: ponto com sensor, satelite e fonte (produto e arquivo de origem)
        mvc.perform(get("/api/v1/detections").param("bbox", "-54,-4,-53,-3").param("start", "2026-11-12")
                        .param("end", "2026-11-12").param("zoom", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.mode").value("points"))
                .andExpect(jsonPath("$.points[?(@.sensor=='MODIS')].sourceProduct").value("MODIS_NRT"))
                .andExpect(jsonPath("$.points[?(@.sensor=='VIIRS')].satellite").value("N20"))
                .andExpect(jsonPath("$.points[?(@.sensor=='VIIRS')].sourceFile").value("prata/silver/" + SHA));

        // CA04: gerar derivados (harmonizacao + grade agregada) nao muda nenhum valor original
        String before = snapshot();
        mvc.perform(get("/api/v1/harmonized").param("bbox", "-54,-4,-53,-3").param("start", "2026-11-12")
                .param("end", "2026-11-12")).andExpect(status().isOk());
        mvc.perform(get("/api/v1/detections").param("bbox", "-54,-4,-53,-3").param("start", "2026-11-12")
                .param("end", "2026-11-12").param("zoom", "5")).andExpect(status().isOk());
        assertEquals(before, snapshot());
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM detection", Integer.class));
    }

    @Test
    void rota_inexistente_e_404_e_metodo_errado_e_405() throws Exception {
        mvc.perform(get("/api/v2/sources")).andExpect(status().isNotFound());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete("/api/v1/detections"))
                .andExpect(status().isMethodNotAllowed());
    }

    private String snapshot() {
        return jdbc.queryForObject("""
                SELECT md5(string_agg(concat_ws('|', sensor, satellite, acq_ts, lat, lon, frp, brightness, bright_t31,
                       confidence_raw, confidence_class, source_product, source_file, record_id), ';'
                       ORDER BY record_id))
                FROM detection""", String.class);
    }
}
