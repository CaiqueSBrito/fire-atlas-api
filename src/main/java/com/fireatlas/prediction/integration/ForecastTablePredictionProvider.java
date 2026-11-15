package com.fireatlas.prediction.integration;

import com.fireatlas.common.dto.Dto;
import com.fireatlas.lake.integration.AthenaQuery;
import com.fireatlas.lake.integration.DeltaTable;
import com.fireatlas.lake.integration.LakeStorage;
import com.fireatlas.region.entity.BBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Provedor de predicao que le a previsao publicada pelo time de dados direto no data lake pelo Athena
 * ({@code forecast}, tabela Delta registrada no Glue): nada e importado nem materializado no PostGIS. O numero vem
 * pronto do pipeline (modelo dele): este provedor so filtra a regiao, informa a emissao, o modelo e o horizonte, e
 * nunca recalcula nem ajusta a probabilidade.
 * <p>
 * A regiao pedida (bbox) e aplicada sobre o centro de cada celula H3 de resolucao 6, que vem de {@code h3_places}
 * (latitude e longitude do pipeline): nada e convertido em H3 aqui. Cada celula vira um ponto. Toda consulta leva o
 * filtro de particao ({@code year}/{@code month} na previsao, {@code resolution} nos lugares): sem ele o Athena
 * varreria a tabela inteira. A emissao mais recente e achada pela particao mais nova, nunca pela tabela toda.
 * <p>
 * Estados: sem previsao publicada ou sem celulas na regiao = {@code INSUFFICIENT_DATA_COVERAGE}; janela pedida fora
 * do horizonte da emissao mais recente = {@code OUT_OF_APPROVED_SCOPE}. As metricas de avaliacao nao sao publicadas
 * pela fonte e isso e dito na resposta. Fica desligado por padrao (ativa com
 * {@code fireatlas.prediction.source=forecast-table}).
 */
public class ForecastTablePredictionProvider implements PredictionProvider {

    public static final String TARGET = "fire_detection_probability_7d";
    public static final int HORIZON_DAYS = 7;

    static final String FORECAST = "forecast";
    static final String PLACES = "h3_places";
    /** Prefixo da tabela Delta da previsao: de onde vem o commitInfo com a versao do modelo. */
    static final String FORECAST_PATH = "gold/forecast/";
    private static final int PLACE_RESOLUTION = 6;

    private static final Logger log = LoggerFactory.getLogger(ForecastTablePredictionProvider.class);

    /** Particao da previsao (ano e mes), unica unidade que as consultas podem tocar. */
    private record Partition(int year, int month) {}

    private final AthenaQuery athena;
    private final LakeStorage storage;
    private final int maxPredictions;

    public ForecastTablePredictionProvider(AthenaQuery athena, LakeStorage storage, int maxPredictions) {
        this.athena = Objects.requireNonNull(athena, "athena obrigatorio");
        this.storage = Objects.requireNonNull(storage, "storage obrigatorio");
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
        Partition partition = latestPartition();
        LocalDate issue = partition == null ? null : latestIssue(partition);
        if (issue == null) {
            return unavailable(OutcomeStatus.INSUFFICIENT_DATA_COVERAGE,
                    "Nenhuma previsao publicada no data lake (gold/forecast).", null, null);
        }
        LocalDate validUntil = issue.plusDays(HORIZON_DAYS);
        if (request.end().isBefore(issue) || request.start().isAfter(validUntil)) {
            return unavailable(OutcomeStatus.OUT_OF_APPROVED_SCOPE,
                    "A previsao disponivel e da emissao " + issue + " e vale ate " + validUntil + " ("
                            + HORIZON_DAYS + " dias); a janela pedida (" + request.start() + " a " + request.end()
                            + ") esta fora dela.", issue, runVersion());
        }
        BBox b = request.bbox();
        var forecastTimestamp = issue.atStartOfDay().toInstant(ZoneOffset.UTC);
        String sql = "SELECT p.latitude AS latitude, p.longitude AS longitude, f.prob_fire_7d AS prob_fire_7d"
                + " FROM " + FORECAST + " f"
                + " JOIN " + PLACES + " p ON p.h3_cell = f.h3_r6"
                + " WHERE f.year = " + partition.year() + " AND f.month = " + partition.month()
                + " AND p.resolution = " + PLACE_RESOLUTION
                + " AND f.issue_date = DATE '" + issue + "' AND f.prob_fire_7d IS NOT NULL"
                + " AND p.latitude BETWEEN " + b.minLat() + " AND " + b.maxLat()
                + " AND p.longitude BETWEEN " + b.minLon() + " AND " + b.maxLon()
                + " ORDER BY f.prob_fire_7d DESC, f.h3_r6"
                + " LIMIT " + maxPredictions;
        List<Dto.PredictionResponse.PredictionPoint> points = new ArrayList<>();
        for (AthenaQuery.Row r : athena.run(sql)) {
            points.add(new Dto.PredictionResponse.PredictionPoint(r.dblOr("latitude", 0), r.dblOr("longitude", 0),
                    forecastTimestamp, r.dblOr("prob_fire_7d", 0), null, null));
        }

        String version = runVersion();
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

    /** Particao mais recente: maior {@code year} e, dentro dele, maior {@code month}; so colunas de particao. */
    private Partition latestPartition() {
        String sql = "SELECT year, month FROM " + FORECAST
                + " GROUP BY year, month ORDER BY year DESC, month DESC LIMIT 1";
        return athena.run(sql).stream().findFirst()
                .filter(r -> r.integer("year") != null && r.integer("month") != null)
                .map(r -> new Partition(r.intOr("year", 0), r.intOr("month", 0)))
                .orElse(null);
    }

    /** Emissao mais recente DENTRO da particao mais nova: a tabela inteira nunca e varrida. */
    private LocalDate latestIssue(Partition partition) {
        String sql = "SELECT max(issue_date) AS issue_date FROM " + FORECAST
                + " WHERE year = " + partition.year() + " AND month = " + partition.month();
        return athena.run(sql).stream().findFirst().map(r -> r.date("issue_date")).orElse(null);
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

    /**
     * Versao do modelo como o pipeline a gravou no commit do log Delta de {@code gold/forecast}; as partes ausentes
     * sao puladas e o resultado e nulo quando nao ha nenhuma delas ou quando o log nao pode ser lido.
     */
    private String runVersion() {
        try {
            Map<String, String> commit = DeltaTable.commitInfo(storage, FORECAST_PATH,
                    DeltaTable.loadRaw(storage, FORECAST_PATH).version());
            List<String> parts = new ArrayList<>();
            String model = value(commit, "model_sha256");
            if (model != null) parts.add("modelo sha256 " + model.substring(0, Math.min(12, model.length())));
            String method = value(commit, "method_version");
            if (method != null) parts.add("metodo " + method);
            String silver = value(commit, "silver_version");
            if (silver != null) parts.add("prata v" + silver);
            return parts.isEmpty() ? null : String.join(" | ", parts);
        } catch (Exception e) {
            log.warn("Versao do modelo indisponivel no log Delta de {}: {}", FORECAST_PATH, e.toString());
            return null;
        }
    }

    private static String value(Map<String, String> commit, String key) {
        String v = commit.get(key);
        return v == null || v.isBlank() ? null : v;
    }
}
