package com.fireatlas.prediction.integration;

import com.fireatlas.common.dto.Dto;
import com.fireatlas.region.entity.BBox;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Provedor de predicao que le a previsao publicada pelo time de dados ({@code gold/forecast}), ja importada para a
 * tabela {@code forecast}. O numero vem pronto do pipeline (modelo dele): este provedor so filtra a regiao, informa
 * a emissao, o modelo e o horizonte, e nunca recalcula nem ajusta a probabilidade.
 * <p>
 * A regiao pedida (bbox) e aplicada sobre o centro de cada celula H3 de resolucao 6, que vem em {@code h3_place}
 * (latitude e longitude do pipeline): nada e convertido em H3 aqui. Cada celula vira um ponto.
 * <p>
 * Estados: sem previsao importada ou sem celulas na regiao = {@code INSUFFICIENT_DATA_COVERAGE}; janela pedida fora do
 * horizonte da emissao mais recente = {@code OUT_OF_APPROVED_SCOPE}. As metricas de avaliacao nao sao publicadas pela
 * fonte e isso e dito na resposta. Fica desligado por padrao (ativa com {@code fireatlas.prediction.source=forecast-table}).
 */
public class ForecastTablePredictionProvider implements PredictionProvider {

    public static final String TARGET = "fire_detection_probability_7d";
    public static final int HORIZON_DAYS = 7;

    private final JdbcTemplate jdbc;
    private final int maxPredictions;

    public ForecastTablePredictionProvider(JdbcTemplate jdbc, int maxPredictions) {
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc obrigatorio");
        if (maxPredictions <= 0) throw new IllegalArgumentException("maxPredictions deve ser positivo");
        this.maxPredictions = maxPredictions;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public ProviderResult predict(ProviderRequest request) {
        Objects.requireNonNull(request, "request obrigatorio");
        LocalDate issue = jdbc.queryForObject("SELECT max(issue_date) FROM forecast", LocalDate.class);
        if (issue == null) {
            return unavailable(OutcomeStatus.INSUFFICIENT_DATA_COVERAGE,
                    "Nenhuma previsao importada (gold/forecast): rode a importacao da forecast.", null, null);
        }
        LocalDate validUntil = issue.plusDays(HORIZON_DAYS);
        if (request.end().isBefore(issue) || request.start().isAfter(validUntil)) {
            return unavailable(OutcomeStatus.OUT_OF_APPROVED_SCOPE,
                    "A previsao disponivel e da emissao " + issue + " e vale ate " + validUntil + " ("
                            + HORIZON_DAYS + " dias); a janela pedida (" + request.start() + " a " + request.end()
                            + ") esta fora dela.", issue, runVersion(issue));
        }
        BBox b = request.bbox();
        List<Dto.PredictionResponse.PredictionPoint> points = new ArrayList<>();
        var forecastTimestamp = issue.atStartOfDay().toInstant(ZoneOffset.UTC);
        jdbc.query("""
                SELECT p.latitude, p.longitude, f.prob_fire_7d
                FROM forecast f
                JOIN h3_place p ON p.res = 6 AND p.cell = f.h3_r6
                WHERE f.issue_date = ? AND f.prob_fire_7d IS NOT NULL
                  AND p.latitude BETWEEN ? AND ? AND p.longitude BETWEEN ? AND ?
                ORDER BY f.prob_fire_7d DESC, f.h3_r6
                LIMIT ?
                """, rs -> {
            points.add(new Dto.PredictionResponse.PredictionPoint(rs.getDouble(1), rs.getDouble(2),
                    forecastTimestamp, rs.getDouble(3), null, null));
        }, issue, b.minLat(), b.maxLat(), b.minLon(), b.maxLon(), maxPredictions);

        String version = runVersion(issue);
        if (points.isEmpty()) {
            return unavailable(OutcomeStatus.INSUFFICIENT_DATA_COVERAGE,
                    "Nenhuma celula da previsao (emissao " + issue + ") cai na regiao pedida.", issue, version);
        }
        return new ProviderResult(OutcomeStatus.AVAILABLE,
                "Previsao do pipeline de dados (gold/forecast), emissao " + issue + ", horizonte de " + HORIZON_DAYS
                        + " dias; probabilidade por celula H3 de resolucao 6, sem ajuste desta API.",
                TARGET,
                new Dto.PredictionResponse.ValidatedHorizon("day", 0, HORIZON_DAYS, "emissao diaria",
                        "celula H3 de resolucao 6 (~36 km2)"),
                version, metrics(issue), points);
    }

    private ProviderResult unavailable(OutcomeStatus status, String reason, LocalDate issue, String version) {
        return new ProviderResult(status, reason, null, null, version, issue == null ? null : metrics(issue), null);
    }

    /** O pipeline nao publica metricas de avaliacao na tabela: isso fica explicito, sem numero inventado. */
    private static Map<String, Object> metrics(LocalDate issue) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("issueDate", issue.toString());
        m.put("evaluationMetricsPublished", false);
        m.put("note", "A fonte (gold/forecast) nao publica metricas de avaliacao; a validacao do modelo e do time de "
                + "dados (veja models/ no data lake).");
        return m;
    }

    /** Versao do modelo como o pipeline a gravou no commit do Delta (forecast_run); nulo se nao houver. */
    private String runVersion(LocalDate issue) {
        List<String> found = jdbc.query("""
                SELECT concat_ws(' | ', 'modelo sha256 ' || left(model_sha256, 12), 'metodo ' || method_version,
                                 'prata v' || silver_version)
                FROM forecast_run WHERE issue_date = ?""", (rs, i) -> rs.getString(1), issue);
        return found.isEmpty() || found.get(0) == null || found.get(0).isBlank() ? null : found.get(0);
    }
}
