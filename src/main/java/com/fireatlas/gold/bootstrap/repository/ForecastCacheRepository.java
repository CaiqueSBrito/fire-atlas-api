package com.fireatlas.gold.bootstrap.repository;

import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.common.policy.SourceUnavailableException;
import com.fireatlas.lake.integration.AnnualCacheExports;
import com.fireatlas.lake.integration.AnnualCacheSource;
import com.fireatlas.lake.integration.ForecastCacheSource;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** COPY por arquivo, retomada duravel e publicacao atomica sem sobrepor a carga anual. */
@Repository
public class ForecastCacheRepository {
    private static final Logger log = LoggerFactory.getLogger(ForecastCacheRepository.class);
    private static final int IMPORT_WORKERS = 4;
    private static final int ANNUAL_LOCK_NAMESPACE = 1178686550;
    private static final int LOCK_NAMESPACE = 1178686551;
    private static final int LOAD_LOCK = 0;
    private static final int OWNER_LOCK = 1;
    private static final Comparator<AnnualCacheSource.Partition> LARGEST_FIRST = Comparator.comparing(
            AnnualCacheSource.Partition::expectedRows, Comparator.nullsFirst(Comparator.reverseOrder()));

    public static final class Lease implements AutoCloseable {
        private final Connection connection;
        private boolean annualLocked;
        private boolean loadLocked;
        private boolean ownerLocked;
        private boolean closed;
        private Lease(Connection connection) { this.connection = connection; }
        @Override public void close() {
            if (closed) return;
            closed = true;
            boolean released = true;
            try (var statement = connection.prepareStatement("SELECT pg_advisory_unlock(?, ?)")) {
                if (ownerLocked) released &= unlock(statement, LOCK_NAMESPACE, OWNER_LOCK);
                if (loadLocked) released &= unlock(statement, LOCK_NAMESPACE, LOAD_LOCK);
                if (annualLocked) released &= unlock(statement, ANNUAL_LOCK_NAMESPACE, LOAD_LOCK);
            } catch (SQLException e) {
                released = false;
                log.warn("Falha liberando locks da previsao", e);
            } finally {
                if (!released) {
                    // Uma conexao devolvida ao pool nao pode conservar locks de uma carga encerrada.
                    try { connection.abort(Runnable::run); }
                    catch (SQLException e) { log.warn("Falha encerrando sessao com locks da previsao", e); }
                }
                try { connection.close(); } catch (SQLException e) { log.warn("Falha fechando conexao da previsao", e); }
            }
        }

        private boolean unlock(PreparedStatement statement, int namespace, int key) {
            try {
                statement.setInt(1, namespace);
                statement.setInt(2, key);
                statement.execute();
                return true;
            } catch (SQLException e) {
                log.warn("Falha liberando lock da previsao ({}, {})", namespace, key, e);
                return false;
            }
        }
    }

    private final DataSource dataSource;
    private final JdbcTemplate jdbc;
    private final ForecastCacheSource source;
    private final long refreshHours;

    public ForecastCacheRepository(DataSource dataSource, JdbcTemplate jdbc, ForecastCacheSource source,
                                   @Value("${fireatlas.bootstrap.refresh-hours:6}") long refreshHours) {
        if (refreshHours < 1) throw new IllegalArgumentException("bootstrap.refresh-hours deve ser >= 1");
        if (dataSource instanceof HikariDataSource pool && pool.getMaximumPoolSize() > 0
                && pool.getMaximumPoolSize() < IMPORT_WORKERS + BootstrapRepository.RESERVED_CONNECTIONS) {
            throw new IllegalArgumentException("Pool JDBC deve reservar " + IMPORT_WORKERS + " importadores da previsao + "
                    + BootstrapRepository.RESERVED_CONNECTIONS + " conexoes");
        }
        this.dataSource = dataSource;
        this.jdbc = jdbc;
        this.source = source;
        this.refreshHours = refreshHours;
    }

    public BootstrapStatus status() { return status(jdbc); }

    private BootstrapStatus status(JdbcTemplate transaction) {
        return transaction.queryForObject("""
                SELECT b.state, b.message, b.error, b.phase, b.planned_units,
                    b.finalization_completed, b.finalization_total,
                    (SELECT count(*) FROM forecast_cache_partition p
                        WHERE p.snapshot_id=coalesce(b.staging_snapshot,b.active_snapshot)) AS total,
                    (SELECT count(*) FROM forecast_cache_partition p
                        WHERE p.snapshot_id=coalesce(b.staging_snapshot,b.active_snapshot) AND p.completed) AS completed,
                    (SELECT coalesce(sum(rows_loaded),0) FROM forecast_cache_partition p
                        WHERE p.snapshot_id=coalesce(b.staging_snapshot,b.active_snapshot) AND p.completed) AS rows,
                    EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=? AND objid=?
                        AND pid=b.loader_pid AND objsubid=2 AND granted
                        AND database=(SELECT oid FROM pg_database WHERE datname=current_database())) AS running
                FROM forecast_cache_bootstrap b WHERE b.id
                """, (rs, index) -> {
            String state = rs.getString("state");
            String message = rs.getString("message");
            String error = rs.getString("error");
            long total = rs.getLong("total");
            long completed = rs.getLong("completed");
            if (state.equals("loading") && !rs.getBoolean("running")) {
                state = "failed";
                message = "Carga da previsao interrompida; tente novamente para retomar os arquivos confirmados";
                error = "O processo anterior terminou antes de concluir a carga";
            } else if (state.equals("ready") && (total == 0 || total != rs.getLong("planned_units") || completed != total
                    || rs.getLong("finalization_completed") != rs.getLong("finalization_total"))) {
                state = "failed";
                message = "Cache da previsao incompleto";
                error = "O plano duravel ainda possui particoes pendentes";
            }
            return new BootstrapStatus(state, completed, total, rs.getLong("rows"), message, error,
                    rs.getString("phase"), rs.getLong("finalization_completed"), rs.getLong("finalization_total"));
        }, LOCK_NAMESPACE, OWNER_LOCK);
    }

    /** GET e consultas sao passivos; somente o servico chama claim. */
    public Lease claim() {
        Lease lease = null;
        try {
            lease = new Lease(dataSource.getConnection());
            JdbcTemplate locked = template(lease.connection);
            lease.annualLocked = Boolean.TRUE.equals(locked.queryForObject("SELECT pg_try_advisory_lock(?, ?)",
                    Boolean.class, ANNUAL_LOCK_NAMESPACE, LOAD_LOCK));
            if (!lease.annualLocked) {
                lease.close();
                return null;
            }
            lease.loadLocked = Boolean.TRUE.equals(locked.queryForObject("SELECT pg_try_advisory_lock(?, ?)",
                    Boolean.class, LOCK_NAMESPACE, LOAD_LOCK));
            if (!lease.loadLocked) {
                lease.close();
                return null;
            }
            locked.queryForObject("SELECT pg_advisory_lock(?, ?)", Object.class, LOCK_NAMESPACE, OWNER_LOCK);
            lease.ownerLocked = true;
            var checked = locked.queryForMap("SELECT state, checked_at FROM forecast_cache_bootstrap WHERE id");
            var timestamp = (java.sql.Timestamp) checked.get("checked_at");
            if ("ready".equals(checked.get("state")) && status(locked).state().equals("ready") && timestamp != null
                    && timestamp.toInstant().isAfter(Instant.now().minus(refreshHours, ChronoUnit.HOURS))) {
                lease.close();
                return null;
            }
            locked.update("UPDATE forecast_cache_bootstrap SET state='loading', error=NULL,"
                    + " message='Lendo manifestos Delta da previsao mundial', phase='preparing',"
                    + " finalization_completed=0, finalization_total=0, planned_units=0, loader_pid=pg_backend_pid() WHERE id");
            return lease;
        } catch (SQLException | RuntimeException e) {
            if (lease != null) lease.close();
            log.error("Nao foi possivel reivindicar a carga compartilhada da previsao", e);
            throw new SourceUnavailableException("Nao foi possivel iniciar o cache da previsao no banco compartilhado");
        }
    }

    public void load(Lease lease) {
        JdbcTemplate locked = template(lease.connection);
        var run = new ImportRun(lease.connection, locked);
        long started = System.nanoTime();
        try {
            var plan = source.plan(run::exportProgress, run);
            run.planned(plan);
            run.finish();
            locked.update("UPDATE forecast_cache_bootstrap SET phase='analyzing', message='Atualizando estatisticas da previsao' WHERE id");
            locked.execute("ANALYZE forecast_cache");
            transaction(lease.connection, () -> {
                if (!Boolean.TRUE.equals(locked.queryForObject("SELECT planned_units>0 AND planned_units="
                        + "(SELECT count(*) FROM forecast_cache_partition p WHERE p.snapshot_id=b.staging_snapshot)"
                        + " AND NOT EXISTS (SELECT 1 FROM forecast_cache_partition p WHERE p.snapshot_id=b.staging_snapshot"
                        + " AND NOT p.completed) FROM forecast_cache_bootstrap b WHERE b.id", Boolean.class))) {
                    throw new IllegalStateException("plano da previsao incompleto");
                }
                long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started);
                locked.update("UPDATE forecast_cache_bootstrap SET active_snapshot=staging_snapshot,"
                                + " published_units=planned_units, state='ready', phase='ready', checked_at=now(),"
                                + " finalization_completed=finalization_total, loader_pid=NULL, error=NULL, message=? WHERE id",
                        "Previsao mundial pronta em " + (seconds / 60) + " min " + String.format("%02d", seconds % 60)
                                + " s (" + IMPORT_WORKERS + " trabalhadores): todos os arquivos e emissoes confirmados");
                locked.update("DELETE FROM forecast_cache_partition WHERE snapshot_id<>"
                        + "(SELECT active_snapshot FROM forecast_cache_bootstrap WHERE id)");
            });
        } catch (Exception e) {
            boolean interrupted = Thread.interrupted() || e instanceof InterruptedException;
            run.cancel();
            log.error("Carga da previsao falhou; publicacao anterior e arquivos confirmados preservados", e);
            try {
                locked.update("UPDATE forecast_cache_bootstrap SET state='failed', loader_pid=NULL, message=?, error=? WHERE id",
                        "Nao foi possivel concluir a previsao; tente novamente para retomar", safeError(e));
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        } finally {
            run.close();
            lease.close();
        }
    }

    /** A publicacao anterior continua valida durante carga/falha da proxima versao. */
    public void requireReady(JdbcTemplate transaction) {
        Boolean ready = transaction.queryForObject("""
                SELECT active_snapshot IS NOT NULL AND published_units>0 AND published_units=
                    (SELECT count(*) FROM forecast_cache_partition p WHERE p.snapshot_id=b.active_snapshot)
                    AND NOT EXISTS (SELECT 1 FROM forecast_cache_partition p
                        WHERE p.snapshot_id=b.active_snapshot AND NOT p.completed)
                FROM forecast_cache_bootstrap b WHERE b.id
                """, Boolean.class);
        if (!Boolean.TRUE.equals(ready)) {
            throw new SourceUnavailableException("Cache mundial da previsao nao esta pronto; use POST /api/v2/bootstrap/forecast");
        }
    }

    private final class ImportRun implements AnnualCacheExports.Listener, AutoCloseable {
        private record Queued(AnnualCacheSource.Partition partition, long order) {}
        private final Connection lease;
        private final JdbcTemplate locked;
        private final PriorityBlockingQueue<Queued> queue = new PriorityBlockingQueue<>(128,
                Comparator.comparing(Queued::partition, LARGEST_FIRST).thenComparingLong(Queued::order));
        private final Map<String, AnnualCacheSource.Partition> submitted = new HashMap<>();
        private final AtomicReference<Exception> failure = new AtomicReference<>();
        private final AtomicReference<String> exportStage = new AtomicReference<>();
        private final AtomicReference<String> importStage = new AtomicReference<>();
        private final AtomicInteger active = new AtomicInteger();
        private ExecutorService workers;
        private ScheduledExecutorService publisher;
        private PgBinaryCopy encoder;
        private volatile boolean closed;
        private String snapshot;
        private boolean snapshotPublished;
        private long order;
        private String published;

        ImportRun(Connection lease, JdbcTemplate locked) { this.lease = lease; this.locked = locked; }
        void exportProgress(String message) { exportStage.set(message); }

        @Override public void snapshot(String id) {
            if (id == null || id.isBlank()) throw new IllegalStateException("snapshot da previsao ausente");
            if (snapshot != null) {
                if (!snapshot.equals(id)) throw new IllegalStateException("snapshot da previsao mudou durante a carga");
                return;
            }
            try {
                transaction(lease, () -> {
                    locked.update("DELETE FROM forecast_cache_partition WHERE snapshot_id<>? AND snapshot_id IS DISTINCT FROM"
                            + " (SELECT active_snapshot FROM forecast_cache_bootstrap WHERE id)", id);
                    locked.update("UPDATE forecast_cache_bootstrap SET staging_snapshot=?, phase='importing',"
                            + " finalization_total=1 WHERE id", id);
                });
                snapshot = id;
                snapshotPublished = Boolean.TRUE.equals(locked.queryForObject(
                        "SELECT active_snapshot=? FROM forecast_cache_bootstrap WHERE id", Boolean.class, snapshot));
                var columns = new ArrayList<String>();
                columns.add("snapshot_id");
                columns.addAll(ForecastCacheSource.FORECAST_COLUMNS);
                columns.add("partition_id");
                encoder = PgBinaryCopy.forTable(locked, "forecast_cache", columns);
                workers = Executors.newFixedThreadPool(IMPORT_WORKERS, runnable -> new Thread(runnable, "forecast-cache-import"));
                for (int i = 0; i < IMPORT_WORKERS; i++) workers.submit(this::work);
                publisher = Executors.newSingleThreadScheduledExecutor(runnable -> {
                    var thread = new Thread(runnable, "forecast-cache-progress");
                    thread.setDaemon(true);
                    return thread;
                });
                publisher.scheduleWithFixedDelay(this::publish, 1, 1, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw unchecked(e);
            }
        }

        @Override public void exported(List<AnnualCacheSource.Partition> partitions) {
            if (snapshot == null) throw new IllegalStateException("exportacao anterior ao snapshot da previsao");
            partitions.stream().sorted(LARGEST_FIRST).forEach(this::submit);
        }

        void planned(ForecastCacheSource.Plan plan) throws Exception {
            snapshot(plan.snapshot());
            if (plan.partitions().isEmpty()) throw new IllegalStateException("plano da previsao sem particao explicita");
            String[] keys = plan.partitions().stream().map(AnnualCacheSource.Partition::key).toArray(String[]::new);
            var keySet = new HashSet<>(List.of(keys));
            if (keys.length != keySet.size() || !keySet.containsAll(submitted.keySet())) {
                throw new IllegalStateException("plano da previsao divergente das exportacoes");
            }
            if (snapshotPublished && !keySet.equals(new HashSet<>(locked.queryForList(
                    "SELECT source_key FROM forecast_cache_partition WHERE snapshot_id=?", String.class, snapshot)))) {
                throw new IllegalStateException("plano publicado mudou sob a mesma identidade");
            }
            transaction(lease, () -> {
                locked.update("DELETE FROM forecast_cache_partition WHERE snapshot_id=? AND NOT (source_key=ANY (?))",
                        snapshot, lease.createArrayOf("text", keys));
                locked.update("UPDATE forecast_cache_bootstrap SET planned_units=?, forecast_version=?, places_version=? WHERE id",
                        keys.length, plan.forecastVersion(), plan.placesVersion());
            });
            plan.partitions().stream().sorted(LARGEST_FIRST).forEach(this::submit);
            exportStage.set(null);
        }

        private void submit(AnnualCacheSource.Partition partition) {
            checkCancellation();
            if (!partition.kind().equals("FORECAST") || partition.resolution() != 6) {
                throw new IllegalStateException("particao fora do plano da previsao nativa r6");
            }
            var previous = submitted.putIfAbsent(partition.key(), partition);
            if (previous != null) {
                if (!previous.equals(partition)) throw new IllegalStateException("metadados divergentes da mesma exportacao");
                return;
            }
            // Nunca substituir linhas de uma publicacao valida sob a mesma identidade imutavel.
            boolean changed = Boolean.TRUE.equals(locked.queryForObject("SELECT EXISTS (SELECT 1 FROM forecast_cache_partition"
                            + " WHERE snapshot_id=? AND source_key=? AND (file_size IS DISTINCT FROM CAST(? AS bigint)"
                            + " OR expected_rows IS DISTINCT FROM CAST(? AS bigint) OR empty_partition<>?))",
                    Boolean.class, snapshot, partition.key(), partition.size(), partition.expectedRows(), partition.empty()));
            if (snapshotPublished && (changed || !Boolean.TRUE.equals(locked.queryForObject(
                    "SELECT EXISTS (SELECT 1 FROM forecast_cache_partition WHERE snapshot_id=? AND source_key=? AND completed)",
                    Boolean.class, snapshot, partition.key())))) {
                throw new IllegalStateException("exportacao publicada mudou sob a mesma identidade");
            }
            if (changed) {
                locked.update("DELETE FROM forecast_cache_partition WHERE snapshot_id=? AND source_key=?", snapshot, partition.key());
            }
            locked.update("INSERT INTO forecast_cache_partition (snapshot_id,source_key,kind,resolution,file_size,expected_rows,empty_partition)"
                            + " VALUES (?,?,'FORECAST',6,?,?,?) ON CONFLICT (snapshot_id,source_key) DO NOTHING",
                    snapshot, partition.key(), partition.size(), partition.expectedRows(), partition.empty());
            if (!Boolean.TRUE.equals(locked.queryForObject("SELECT completed FROM forecast_cache_partition"
                    + " WHERE snapshot_id=? AND source_key=?", Boolean.class, snapshot, partition.key()))) {
                queue.add(new Queued(partition, order++));
            }
        }

        private void work() {
            try {
                while (failure.get() == null && !Thread.currentThread().isInterrupted()) {
                    Queued next = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (next == null) {
                        if (closed && queue.isEmpty()) return;
                        continue;
                    }
                    importPartition(next.partition());
                }
            } catch (Exception e) {
                if (failure.compareAndSet(null, e)) workers.shutdownNow();
            }
        }

        private void importPartition(AnnualCacheSource.Partition partition) throws Exception {
            try (Connection connection = dataSource.getConnection()) {
                JdbcTemplate worker = template(connection);
                active.incrementAndGet();
                try {
                    checkCancellation();
                    importStage.set("baixando arquivo da previsao");
                    transaction(connection, () -> {
                        Long id = worker.queryForObject("SELECT id FROM forecast_cache_partition WHERE snapshot_id=? AND source_key=?",
                                Long.class, snapshot, partition.key());
                        CopyIn[] copy = {null};
                        var buffer = new Buffer();
                        var out = new DataOutputStream(buffer);
                        long[] written = {0};
                        long rows;
                        try {
                            rows = source.stream(partition, batch -> {
                                checkCancellation();
                                if (batch.isEmpty()) return;
                                try {
                                    buffer.reset();
                                    if (copy[0] == null) {
                                        copy[0] = connection.unwrap(PGConnection.class).getCopyAPI().copyIn(encoder.sql("forecast_cache"));
                                        out.write(PgBinaryCopy.HEADER);
                                    }
                                    for (Object[] values : batch) {
                                        if (values.length != ForecastCacheSource.FORECAST_COLUMNS.size()) {
                                            throw new IllegalStateException("projecao da previsao diverge do COPY");
                                        }
                                        encoder.beginRow(out);
                                        encoder.field(out, 0, snapshot);
                                        int column = 1;
                                        for (Object value : values) encoder.field(out, column++, value);
                                        encoder.field(out, column, id);
                                    }
                                    out.flush();
                                    copy[0].writeToCopy(buffer.array(), 0, buffer.size());
                                } catch (IOException | SQLException e) {
                                    throw new IllegalStateException("COPY binario da previsao falhou", e);
                                }
                                written[0] += batch.size();
                                importStage.set(written[0] + " linhas nativas gravadas; aguardando commit do arquivo");
                            });
                            if (rows != written[0] || (partition.expectedRows() != null && rows != partition.expectedRows())
                                    || (partition.empty() && rows != 0)) {
                                throw new IllegalStateException("contagem da previsao diverge do arquivo exportado");
                            }
                            if (copy[0] != null) {
                                copy[0].writeToCopy(PgBinaryCopy.TRAILER, 0, PgBinaryCopy.TRAILER.length);
                                copy[0].endCopy();
                            }
                        } finally {
                            if (copy[0] != null && copy[0].isActive()) copy[0].cancelCopy();
                        }
                        worker.update("UPDATE forecast_cache_partition SET completed=true, rows_loaded=?,"
                                + " completed_at=clock_timestamp() WHERE id=? AND NOT completed", rows, id);
                    });
                    importStage.set("arquivo da previsao confirmado");
                } finally {
                    active.decrementAndGet();
                }
            }
        }

        void finish() throws Exception {
            closed = true;
            if (workers != null) {
                workers.shutdown();
                awaitWorkers();
            }
            Exception error = failure.get();
            if (error != null) throw error;
        }

        void cancel() {
            failure.compareAndSet(null, new CancellationException("Carga da previsao interrompida"));
            closed = true;
            if (workers != null) {
                workers.shutdownNow();
                awaitWorkers();
            }
        }

        private void awaitWorkers() {
            boolean interrupted = false;
            while (!workers.isTerminated()) {
                try {
                    workers.awaitTermination(1, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    interrupted = true;
                    failure.compareAndSet(null, e);
                    workers.shutdownNow();
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }

        private void publish() {
            try {
                String exporting = exportStage.get();
                String importing = importStage.get();
                if (exporting == null && importing == null) return;
                String message = (exporting == null ? "" : exporting + "; ") + active.get() + "/" + IMPORT_WORKERS
                        + " trabalhadores ativos" + (importing == null ? "; aguardando arquivos" : "; " + importing);
                if (message.equals(published)) return;
                jdbc.update("UPDATE forecast_cache_bootstrap SET message=? WHERE id AND state='loading' AND phase='importing'", message);
                published = message;
            } catch (RuntimeException e) {
                log.warn("Falha publicando progresso da previsao", e);
            }
        }

        private void checkCancellation() {
            if (Thread.currentThread().isInterrupted() || failure.get() != null) {
                throw new CancellationException("Carga da previsao interrompida");
            }
        }

        @Override public void close() {
            if (workers != null && !workers.isTerminated()) cancel();
            if (publisher == null) return;
            publisher.shutdownNow();
            try { publisher.awaitTermination(5, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    private static final class Buffer extends ByteArrayOutputStream {
        Buffer() { super(1 << 16); }
        byte[] array() { return buf; }
    }

    private static JdbcTemplate template(Connection connection) {
        return new JdbcTemplate(new SingleConnectionDataSource(connection, true));
    }

    private static RuntimeException unchecked(Exception e) {
        return e instanceof RuntimeException runtime ? runtime : new IllegalStateException(e.getMessage(), e);
    }

    @FunctionalInterface private interface Work { void run() throws Exception; }

    private static void transaction(Connection connection, Work action) throws Exception {
        connection.setAutoCommit(false);
        try {
            action.run();
            connection.commit();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private static String safeError(Exception error) {
        Throwable cause = error;
        while (cause.getCause() != null) cause = cause.getCause();
        if (cause instanceof AwsServiceException aws && aws.awsErrorDetails() != null) {
            return "Fonte AWS: " + aws.awsErrorDetails().errorCode() + " (HTTP " + aws.statusCode() + ")";
        }
        if (cause instanceof SQLException sql) return "Persistencia/leitura: " + cause.getClass().getSimpleName()
                + " (SQLSTATE " + sql.getSQLState() + ")";
        if (cause instanceof IOException) return "Leitura da fonte: " + cause.getClass().getSimpleName() + "; detalhes no log do servidor";
        return "Carga interrompida por " + cause.getClass().getSimpleName() + "; detalhes no log do servidor";
    }
}
