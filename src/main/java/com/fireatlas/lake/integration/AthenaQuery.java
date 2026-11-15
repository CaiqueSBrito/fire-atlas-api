package com.fireatlas.lake.integration;

import com.fireatlas.gold.common.policy.SourceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultReuseByAgeConfiguration;
import software.amazon.awssdk.services.athena.model.ResultReuseConfiguration;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Consulta a camada ouro direto no S3 pelo Amazon Athena (tabelas Delta registradas no Glue), sem copiar nada para o
 * PostGIS. Toda consulta vai com reuso de resultado ligado: repeticao do mesmo texto SQL dentro da validade nao
 * escaneia byte nenhum e responde em fracao de segundo, por isso o SQL precisa ser canonico (veja
 * {@code TrinoFilters}).
 * <p>
 * Credenciais de {@code fireatlas.s3.access-key-id}/{@code secret-access-key} ou, se vazias, da cadeia padrao da AWS
 * (perfil ou IAM Role) — as mesmas do {@link S3LakeStorage}.
 */
@Component
public class AthenaQuery implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AthenaQuery.class);
    private static final long POLL_MS = 200;

    /** Linha do resultado: valores por nome de coluna, nulo quando a coluna veio vazia. */
    public record Row(Map<String, String> values) {

        public String str(String column) {
            return values.get(column);
        }

        public Double dbl(String column) {
            String v = values.get(column);
            return v == null ? null : Double.valueOf(v);
        }

        public double dblOr(String column, double fallback) {
            Double v = dbl(column);
            return v == null ? fallback : v;
        }

        public Long lng(String column) {
            String v = values.get(column);
            return v == null ? null : Long.valueOf(v);
        }

        public long lngOr(String column, long fallback) {
            Long v = lng(column);
            return v == null ? fallback : v;
        }

        public Integer integer(String column) {
            String v = values.get(column);
            return v == null ? null : Integer.valueOf(v);
        }

        public int intOr(String column, int fallback) {
            Integer v = integer(column);
            return v == null ? fallback : v;
        }

        public LocalDate date(String column) {
            String v = values.get(column);
            return v == null ? null : LocalDate.parse(v.length() > 10 ? v.substring(0, 10) : v);
        }

        public Boolean bool(String column) {
            String v = values.get(column);
            return v == null ? null : Boolean.valueOf(v);
        }
    }

    private final AthenaClient athena;
    private final String database;
    private final String workgroup;
    private final int reuseMinutes;
    private final long timeoutMs;

    public AthenaQuery(@Value("${fireatlas.athena.database}") String database,
                       @Value("${fireatlas.athena.workgroup}") String workgroup,
                       @Value("${fireatlas.athena.result-reuse-minutes:1440}") int reuseMinutes,
                       @Value("${fireatlas.athena.timeout-ms:30000}") long timeoutMs,
                       @Value("${fireatlas.s3.region}") String region,
                       @Value("${fireatlas.s3.access-key-id:}") String accessKeyId,
                       @Value("${fireatlas.s3.secret-access-key:}") String secretAccessKey,
                       @Value("${fireatlas.s3.session-token:}") String sessionToken) {
        this.database = database;
        this.workgroup = workgroup;
        this.reuseMinutes = reuseMinutes;
        this.timeoutMs = timeoutMs;
        var builder = AthenaClient.builder().region(Region.of(region));
        if (!accessKeyId.isBlank() && !secretAccessKey.isBlank()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(sessionToken.isBlank()
                    ? AwsBasicCredentials.create(accessKeyId, secretAccessKey)
                    : AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken)));
        }
        this.athena = builder.build();
    }

    /** Executa o SQL e devolve as linhas; falha de execucao vira {@link SourceUnavailableException}. */
    public List<Row> run(String sql) {
        long t0 = System.currentTimeMillis();
        String id = start(sql);
        QueryExecution execution = await(id);
        List<Row> rows = fetch(id);
        var stats = execution.statistics();
        boolean reused = stats != null && stats.resultReuseInformation() != null
                && Boolean.TRUE.equals(stats.resultReuseInformation().reusedPreviousResult());
        log.info("athena {} {} linhas, {} MB escaneados, reuso={}, {} ms", id, rows.size(),
                String.format("%.1f", (stats == null || stats.dataScannedInBytes() == null
                        ? 0 : stats.dataScannedInBytes()) / 1e6), reused, System.currentTimeMillis() - t0);
        return rows;
    }

    private String start(String sql) {
        try {
            return athena.startQueryExecution(StartQueryExecutionRequest.builder()
                    .queryString(sql)
                    .workGroup(workgroup)
                    .queryExecutionContext(QueryExecutionContext.builder().database(database).build())
                    .resultReuseConfiguration(ResultReuseConfiguration.builder()
                            .resultReuseByAgeConfiguration(ResultReuseByAgeConfiguration.builder()
                                    .enabled(true).maxAgeInMinutes(reuseMinutes).build())
                            .build())
                    .build()).queryExecutionId();
        } catch (AthenaException e) {
            throw unavailable(e.awsErrorDetails() == null ? e.getMessage() : e.awsErrorDetails().errorMessage(), e);
        }
    }

    private QueryExecution await(String id) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            QueryExecution e = athena.getQueryExecution(GetQueryExecutionRequest.builder()
                    .queryExecutionId(id).build()).queryExecution();
            QueryExecutionState state = e.status().state();
            if (state == QueryExecutionState.SUCCEEDED) return e;
            if (state == QueryExecutionState.FAILED || state == QueryExecutionState.CANCELLED) {
                String reason = e.status().stateChangeReason();
                log.error("athena {} {}: {}", id, state, reason);
                throw unavailable(reason, null);
            }
            if (System.currentTimeMillis() > deadline) {
                athena.stopQueryExecution(r -> r.queryExecutionId(id));
                throw unavailable("consulta ao data lake passou de " + timeoutMs + " ms", null);
            }
            sleep();
        }
    }

    private List<Row> fetch(String id) {
        List<Row> rows = new ArrayList<>();
        List<String> columns = null;
        boolean first = true;
        for (var page : athena.getQueryResultsPaginator(GetQueryResultsRequest.builder()
                .queryExecutionId(id).build())) {
            if (columns == null) {
                columns = page.resultSet().resultSetMetadata().columnInfo().stream().map(ColumnInfo::name).toList();
            }
            var data = page.resultSet().rows();
            // a primeira linha da primeira pagina e o cabecalho
            for (int i = first ? 1 : 0; i < data.size(); i++) {
                List<Datum> cells = data.get(i).data();
                Map<String, String> values = new LinkedHashMap<>();
                for (int c = 0; c < columns.size() && c < cells.size(); c++) {
                    values.put(columns.get(c), cells.get(c).varCharValue());
                }
                rows.add(new Row(values));
            }
            first = false;
        }
        return rows;
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL_MS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new SourceUnavailableException("consulta ao data lake interrompida");
        }
    }

    private static SourceUnavailableException unavailable(String reason, Exception cause) {
        if (cause != null) log.error("athena indisponivel: {}", reason, cause);
        return new SourceUnavailableException("Consulta ao data lake indisponivel no momento"
                + (reason == null || reason.isBlank() ? "" : ": " + reason));
    }

    @Override
    public void close() {
        athena.close();
    }
}
