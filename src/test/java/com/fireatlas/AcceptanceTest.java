package com.fireatlas;

import com.fireatlas.domain.BBox;
import com.fireatlas.ingestion.DetectionCsvLoader;
import com.fireatlas.ingestion.DetectionRowParser;
import com.fireatlas.query.HarmonizationRunStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
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
import java.io.StringReader;
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

    private DetectionCsvLoader.Stats load(String product, String file, String... rows) throws Exception {
        String sensor = product.startsWith("MODIS") ? "MODIS" : "VIIRS";
        String csv = HEADER + "\n" + String.join("\n", rows) + "\n";
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
                MODIS_ROW.replace(",25.5,D", ",-3,D"));             // FRP negativo
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
}
