package com.fireatlas.prediction;

import com.fireatlas.gold.DbAvailable;
import com.fireatlas.prediction.integration.ForecastTablePredictionProvider;
import com.fireatlas.prediction.integration.PredictionProvider;
import com.fireatlas.region.entity.BBox;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Provedor que le a gold/forecast ja importada (fireatlas.prediction.source=forecast-table). */
@ExtendWith(DbAvailable.class)
@SpringBootTest(properties = "fireatlas.prediction.source=forecast-table")
class ForecastTableProviderTest {

    static final String SCHEMA = "it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);

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

    @Autowired PredictionProvider provider;
    @Autowired JdbcTemplate jdbc;

    private static PredictionProvider.ProviderRequest req(String start, String end) {
        return new PredictionProvider.ProviderRequest("t", new BBox(-54, -4, -53, -3), Instant.now(),
                LocalDate.parse(start), LocalDate.parse(end));
    }

    @Test
    void usa_o_provedor_da_forecast_e_cobre_todos_os_estados() {
        assertInstanceOf(ForecastTablePredictionProvider.class, provider);
        jdbc.update("DELETE FROM forecast"); jdbc.update("DELETE FROM h3_place"); jdbc.update("DELETE FROM forecast_run");

        // sem previsao importada
        assertEquals(PredictionProvider.OutcomeStatus.INSUFFICIENT_DATA_COVERAGE,
                provider.predict(req("2026-11-13", "2026-11-14")).status());

        jdbc.update("INSERT INTO h3_place (res, cell, latitude, longitude) VALUES (6, 1, -3.5, -53.5), (6, 2, -3.6, -53.6), (6, 3, -20, -40)");
        jdbc.update("INSERT INTO forecast (issue_date, h3_r6, prob_fire_7d) VALUES ('2026-11-12', 1, 0.2), ('2026-11-12', 2, 0.9), ('2026-11-12', 3, 0.5), ('2026-11-11', 1, 0.99)");
        jdbc.update("INSERT INTO forecast_run (issue_date, source_key, model_sha256, method_version, silver_version) VALUES ('2026-11-12', 'k', 'abcdef0123456789', 'v1', '648')");

        var ok = provider.predict(req("2026-11-13", "2026-11-14"));
        assertEquals(PredictionProvider.OutcomeStatus.AVAILABLE, ok.status());
        assertEquals(2, ok.predictions().size());                         // a celula fora do bbox nao entra
        assertEquals(0.9, ok.predictions().get(0).occurrenceProbability(), 1e-6);   // so a emissao mais recente, ordenada
        assertEquals("fire_detection_probability_7d", ok.predictionTarget());
        assertEquals(7, ok.validatedHorizon().maxLead());
        assertEquals("modelo sha256 abcdef012345 | metodo v1 | prata v648", ok.modelVersion());
        assertEquals(false, ok.evaluationMetrics().get("evaluationMetricsPublished"));

        // janela fora da validade (emissao + 7 dias)
        assertEquals(PredictionProvider.OutcomeStatus.OUT_OF_APPROVED_SCOPE,
                provider.predict(req("2026-12-01", "2026-12-02")).status());
        assertTrue(provider.isEnabled());
    }
}
