package com.fireatlas.lake.integration;

import com.fireatlas.gold.common.policy.SourceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import java.util.function.Consumer;

/**
 * Consulta tabelas Delta registradas no Glue pelo Amazon Athena. SELECTs usam SQL canonico e reuso de resultado
 * (veja {@code TrinoFilters}); comandos UNLOAD do cache anual desativam reuso e devolvem apenas o manifesto.
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

    /** UNLOAD busca apenas estado e endereco do manifesto, nunca DATA_ROWS/CSV. */
    public record Command(String queryId, String manifestLocation) {}

    /** Apenas falhas terminais podem abandonar um destino UNLOAD possivelmente nao vazio. */
    public static final class QueryFailedException extends SourceUnavailableException {
        QueryFailedException(String message) { super(message); }
    }

    @Autowired
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

    AthenaQuery(AthenaClient athena, String database, String workgroup, int reuseMinutes, long timeoutMs) {
        this.athena = athena;
        this.database = database;
        this.workgroup = workgroup;
        this.reuseMinutes = reuseMinutes;
        this.timeoutMs = timeoutMs;
    }

    public String sourceIdentity() {
        return database + "/" + workgroup;
    }

    /** Usa a localizacao efetiva do workgroup, nao um bucket presumido. */
    public String workgroupOutputLocation() {
        try {
            var configuration = athena.getWorkGroup(r -> r.workGroup(workgroup)).workGroup().configuration();
            String location = configuration.resultConfiguration() == null ? null
                    : configuration.resultConfiguration().outputLocation();
            if (location == null || location.isBlank()) {
                throw unavailable("workgroup sem OutputLocation S3; defina BOOTSTRAP_EXPORT_PREFIX", null);
            }
            return location;
        } catch (AthenaException e) {
            throw unavailable(e.awsErrorDetails() == null ? e.getMessage() : e.awsErrorDetails().errorMessage(), e);
        }
    }

    /** Token duravel permite reencontrar a execucao apos uma queda antes de salvar seu id. */
    public String startCommand(String sql, String clientToken) {
        return start(sql, clientToken, false);
    }

    public Command awaitCommand(String id, long commandTimeoutMs) {
        try {
            var execution = await(id, commandTimeoutMs);
            String manifest = execution.statistics() == null ? null : execution.statistics().dataManifestLocation();
            if (manifest == null || manifest.isBlank()) {
                throw unavailable("UNLOAD " + id + " terminou sem DataManifestLocation", null);
            }
            return new Command(id, manifest);
        } catch (RuntimeException e) {
            if (Thread.currentThread().isInterrupted()) {
                try {
                    athena.stopQueryExecution(r -> r.queryExecutionId(id));
                } catch (RuntimeException stop) {
                    log.warn("Nao foi possivel cancelar UNLOAD {}", id, stop);
                }
            }
            throw e;
        }
    }

    /** Executa o SQL e devolve as linhas; falha de execucao vira {@link SourceUnavailableException}. */
    public List<Row> run(String sql) {
        List<Row> rows = new ArrayList<>();
        stream(sql, rows::addAll);
        return rows;
    }

    /** Resultados paginados: o consumidor persiste cada pagina antes da proxima, sem buffer anual. */
    public long stream(String sql, Consumer<List<Row>> consumer) {
        long t0 = System.currentTimeMillis();
        String id = start(sql);
        QueryExecution execution = await(id);
        long count = fetch(id, consumer);
        var stats = execution.statistics();
        boolean reused = stats != null && stats.resultReuseInformation() != null
                && Boolean.TRUE.equals(stats.resultReuseInformation().reusedPreviousResult());
        log.info("athena {} {} linhas, {} MB escaneados, reuso={}, {} ms", id, count,
                String.format("%.1f", (stats == null || stats.dataScannedInBytes() == null
                        ? 0 : stats.dataScannedInBytes()) / 1e6), reused, System.currentTimeMillis() - t0);
        return count;
    }

    private String start(String sql) {
        return start(sql, null, true);
    }

    private String start(String sql, String clientToken, boolean reuse) {
        try {
            var request = StartQueryExecutionRequest.builder()
                    .queryString(sql)
                    .workGroup(workgroup)
                    .queryExecutionContext(QueryExecutionContext.builder().database(database).build())
                    .resultReuseConfiguration(ResultReuseConfiguration.builder()
                            .resultReuseByAgeConfiguration(ResultReuseByAgeConfiguration.builder()
                                    .enabled(reuse).maxAgeInMinutes(reuseMinutes).build())
                            .build());
            if (clientToken != null) request.clientRequestToken(clientToken);
            return athena.startQueryExecution(request.build()).queryExecutionId();
        } catch (AthenaException e) {
            throw unavailable(e.awsErrorDetails() == null ? e.getMessage() : e.awsErrorDetails().errorMessage(), e);
        }
    }

    private QueryExecution await(String id) {
        return await(id, timeoutMs);
    }

    private QueryExecution await(String id, long queryTimeoutMs) {
        long deadline = System.currentTimeMillis() + queryTimeoutMs;
        while (true) {
            QueryExecution e = athena.getQueryExecution(GetQueryExecutionRequest.builder()
                    .queryExecutionId(id).build()).queryExecution();
            QueryExecutionState state = e.status().state();
            if (state == QueryExecutionState.SUCCEEDED) return e;
            if (state == QueryExecutionState.FAILED || state == QueryExecutionState.CANCELLED) {
                String reason = e.status().stateChangeReason();
                log.error("athena {} {}: {}", id, state, reason);
                throw new QueryFailedException("Consulta ao data lake " + id + " " + state + ": " + reason);
            }
            if (System.currentTimeMillis() > deadline) {
                athena.stopQueryExecution(r -> r.queryExecutionId(id));
                throw new QueryFailedException("Consulta ao data lake passou de " + queryTimeoutMs + " ms");
            }
            sleep();
        }
    }

    private long fetch(String id, Consumer<List<Row>> consumer) {
        long count = 0;
        List<String> columns = null;
        boolean first = true;
        for (var page : athena.getQueryResultsPaginator(GetQueryResultsRequest.builder()
                .queryExecutionId(id).build())) {
            List<Row> rows = new ArrayList<>();
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
            count += rows.size();
            consumer.accept(rows);
        }
        return count;
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
