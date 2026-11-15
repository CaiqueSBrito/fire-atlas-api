package com.fireatlas.gold.bootstrap.repository;

import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.common.policy.SourceUnavailableException;
import com.fireatlas.lake.integration.AnnualCacheSource;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Plano, lotes, commits e exclusao mutua pertencem ao banco compartilhado, nao ao visitante/JVM. */
@Repository
public class BootstrapRepository {
    private static final Logger log = LoggerFactory.getLogger(BootstrapRepository.class);
    private static final int LOCK_NAMESPACE = 1178686550;
    private static final int YEAR = AnnualCacheSource.YEAR;
    private static final String GRID_INSERT = insertSql(true);
    private static final String PLACES_INSERT = insertSql(false);
    private static final String GRID_PUBLISH = publishSql(true);
    private static final String PLACES_PUBLISH = publishSql(false);

    public static final class Lease implements AutoCloseable {
        private final Connection connection;
        private Lease(Connection connection) { this.connection = connection; }
        @Override public void close() {
            try {
                try (var statement = connection.prepareStatement("SELECT pg_advisory_unlock(?, ?)")) {
                    statement.setInt(1, LOCK_NAMESPACE);
                    statement.setInt(2, YEAR);
                    statement.execute();
                }
            } catch (SQLException e) {
                log.warn("Falha liberando lock de bootstrap", e);
            } finally {
                try { connection.close(); } catch (SQLException e) { log.warn("Falha fechando conexao de bootstrap", e); }
            }
        }
    }

    private final DataSource dataSource;
    private final JdbcTemplate jdbc;
    private final AnnualCacheSource source;
    private final long refreshHours;
    private final int importWorkers;

    public BootstrapRepository(DataSource dataSource, JdbcTemplate jdbc, AnnualCacheSource source,
                               @Value("${fireatlas.bootstrap.refresh-hours:6}") long refreshHours,
                               @Value("${fireatlas.bootstrap.import-workers:2}") int importWorkers) {
        if (refreshHours < 1) throw new IllegalArgumentException("bootstrap.refresh-hours deve ser >= 1");
        if (importWorkers < 1 || importWorkers > 4) {
            throw new IllegalArgumentException("bootstrap.import-workers deve estar entre 1 e 4");
        }
        if (dataSource instanceof HikariDataSource pool && pool.getMaximumPoolSize() > 0
                && pool.getMaximumPoolSize() < importWorkers + 3) {
            throw new IllegalArgumentException("Pool JDBC deve reservar bootstrap.import-workers + 3 conexoes");
        }
        this.dataSource = dataSource;
        this.jdbc = jdbc;
        this.source = source;
        this.refreshHours = refreshHours;
        this.importWorkers = importWorkers;
    }

    public BootstrapStatus status() { return status(jdbc); }

    private BootstrapStatus status(JdbcTemplate jdbc) {
        return jdbc.queryForObject("""
                SELECT b.state, b.message, b.error,
                    (SELECT count(*) FROM annual_grid_partition) AS total,
                    (SELECT count(*) FROM annual_grid_partition WHERE completed) AS completed,
                    (SELECT coalesce(sum(rows_loaded), 0) FROM annual_grid_partition WHERE completed AND kind='GRID') AS rows,
                    EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=? AND objid=?
                        AND objsubid=2 AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database())) AS active
                FROM annual_grid_bootstrap b WHERE b.year=?
                """, (rs, row) -> {
            String state = rs.getString("state");
            String message = rs.getString("message");
            String error = rs.getString("error");
            long total = rs.getLong("total");
            long completed = rs.getLong("completed");
            if (state.equals("loading") && !rs.getBoolean("active")) {
                state = "failed";
                message = "Carga interrompida; tente novamente para retomar os arquivos confirmados";
                error = "O processo anterior terminou antes de concluir a carga";
            } else if (state.equals("ready") && (total == 0 || completed != total)) {
                state = "failed";
                message = "Cache anual incompleto";
                error = "O plano duravel ainda possui particoes pendentes";
            }
            return new BootstrapStatus(state, completed, total, rs.getLong("rows"), message, error);
        }, LOCK_NAMESPACE, YEAR, YEAR);
    }

    /** Chamado somente pelo POST; GET e os repositorios de consulta nunca iniciam trabalho. */
    public Lease claim() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            JdbcTemplate locked = template(connection);
            if (!Boolean.TRUE.equals(locked.queryForObject("SELECT pg_try_advisory_lock(?, ?)", Boolean.class,
                    LOCK_NAMESPACE, YEAR))) {
                connection.close();
                return null;
            }
            Lease lease = new Lease(connection);
            var checked = locked.queryForMap("SELECT state, checked_at FROM annual_grid_bootstrap WHERE year=?", YEAR);
            var timestamp = (java.sql.Timestamp) checked.get("checked_at");
            if ("ready".equals(checked.get("state")) && status(locked).state().equals("ready") && timestamp != null
                    && timestamp.toInstant().isAfter(Instant.now().minus(refreshHours, ChronoUnit.HOURS))) {
                lease.close();
                return null;
            }
            locked.update("UPDATE annual_grid_bootstrap SET state='loading', error=NULL, message=? WHERE year=?",
                    "Lendo manifestos Delta da cobertura mundial de 2026", YEAR);
            return lease;
        } catch (SQLException | RuntimeException e) {
            if (connection != null) new Lease(connection).close();
            log.error("Nao foi possivel reivindicar a carga anual compartilhada", e);
            throw new SourceUnavailableException("Nao foi possivel iniciar o cache anual no banco compartilhado");
        }
    }

    public void load(Lease lease) {
        JdbcTemplate locked = template(lease.connection);
        try {
            var plan = source.plan(message -> jdbc.update(
                    "UPDATE annual_grid_bootstrap SET message=? WHERE year=? AND state='loading'", message, YEAR));
            transaction(lease.connection, () -> {
                locked.update("DELETE FROM annual_map_grid_stage");
                locked.update("DELETE FROM annual_h3_places_stage");
                String[] keys = plan.partitions().stream().map(AnnualCacheSource.Partition::key).toArray(String[]::new);
                locked.update("DELETE FROM annual_grid_partition WHERE NOT (source_key = ANY (?))",
                        ps -> ps.setArray(1, lease.connection.createArrayOf("text", keys)));
                for (var partition : plan.partitions()) {
                    locked.update("DELETE FROM annual_grid_partition WHERE source_key=? AND (file_size IS DISTINCT FROM CAST(? AS bigint)"
                                    + " OR expected_rows IS DISTINCT FROM CAST(? AS bigint) OR kind<>?"
                                    + " OR resolution<>? OR empty_partition<>?)",
                            partition.key(), partition.size(), partition.expectedRows(), partition.kind(),
                            partition.resolution(), partition.empty());
                    locked.update("INSERT INTO annual_grid_partition"
                                    + " (source_key, kind, resolution, file_size, expected_rows, empty_partition)"
                                    + " VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (source_key) DO NOTHING",
                            partition.key(), partition.kind(), partition.resolution(), partition.size(),
                            partition.expectedRows(), partition.empty());
                }
                locked.update("UPDATE annual_grid_bootstrap SET grid_version=?, places_version=?, message=? WHERE year=?",
                        plan.gridVersion(), plan.placesVersion(), "Carregando arquivos ativos da grade e dos lugares", YEAR);
            });
            importPending(locked, plan);
            transaction(lease.connection, () -> {
                Long pending = locked.queryForObject("SELECT count(*) FROM annual_grid_partition WHERE NOT completed", Long.class);
                if (pending == null || pending != 0) throw new IllegalStateException("particoes pendentes");
                locked.update("UPDATE annual_grid_bootstrap SET state='ready', checked_at=now(), error=NULL, message=? WHERE year=?",
                        "Cobertura mundial de 2026 pronta: todos os arquivos e resolucoes confirmados", YEAR);
            });
        } catch (Exception e) {
            boolean interrupted = Thread.interrupted() || e instanceof InterruptedException;
            log.error("Carga anual de 2026 falhou; particoes confirmadas serao reutilizadas", e);
            try {
                locked.update("UPDATE annual_grid_bootstrap SET state='failed', message=?, error=? WHERE year=?",
                        "Nao foi possivel concluir a carga; tente novamente para retomar", safeError(e), YEAR);
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        } finally {
            lease.close();
        }
    }

    /** Apenas os arquivos em execucao entram no executor; o coordenador mantem o lock ate o ultimo encerrar. */
    private void importPending(JdbcTemplate locked, AnnualCacheSource.Plan plan) throws Exception {
        Iterator<AnnualCacheSource.Partition> partitions = plan.partitions().iterator();
        AtomicInteger active = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ExecutorService workers = Executors.newFixedThreadPool(importWorkers, runnable ->
                new Thread(runnable, "annual-cache-import-2026"));
        var completions = new ExecutorCompletionService<Void>(workers);
        Set<Future<Void>> running = new HashSet<>();
        Exception error = null;
        boolean interrupted = false;
        try {
            while (true) {
                checkCancellation(failure);
                while (running.size() < importWorkers) {
                    var partition = nextPending(locked, partitions);
                    if (partition == null) break;
                    checkCancellation(failure);
                    running.add(completions.submit(() -> {
                        try {
                            importPartition(partition, active, failure);
                            return null;
                        } catch (Exception e) {
                            failure.compareAndSet(null, e);
                            throw e;
                        }
                    }));
                }
                if (running.isEmpty()) break;
                Future<Void> finished = completions.take();
                running.remove(finished);
                finished.get();
            }
        } catch (InterruptedException e) {
            interrupted = true;
            error = e;
        } catch (ExecutionException e) {
            error = failure.get() != null ? failure.get() : e.getCause() instanceof Exception cause ? cause : e;
        } catch (Exception e) {
            error = failure.get() == null ? e : failure.get();
        } finally {
            if (error == null) workers.shutdown();
            else {
                for (var future : running) future.cancel(true);
                workers.shutdownNow();
            }
            // Future.cancel() conclui o Future antes do writer sair. Somente termination autoriza liberar o lock.
            while (!workers.isTerminated()) {
                try {
                    workers.awaitTermination(1, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    interrupted = true;
                    if (error == null) error = e;
                    for (var future : running) future.cancel(true);
                    workers.shutdownNow();
                }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
        if (error != null) throw error;
    }

    private static AnnualCacheSource.Partition nextPending(JdbcTemplate locked,
                                                            Iterator<AnnualCacheSource.Partition> partitions) {
        while (partitions.hasNext()) {
            var partition = partitions.next();
            if (!Boolean.TRUE.equals(locked.queryForObject(
                    "SELECT completed FROM annual_grid_partition WHERE source_key=?", Boolean.class, partition.key()))) {
                return partition;
            }
        }
        return null;
    }

    private void importPartition(AnnualCacheSource.Partition partition, AtomicInteger active,
                                 AtomicReference<Exception> failure) throws Exception {
        // Nunca emprestar a conexao do advisory lock nem compartilhar um JdbcTemplate entre workers.
        try (Connection connection = dataSource.getConnection()) {
            JdbcTemplate worker = template(connection);
            String table = partition.kind().equals("GRID") ? "annual_map_grid" : "annual_h3_places";
            active.incrementAndGet();
            boolean published = false;
            try {
                checkCancellation(failure);
                worker.update("DELETE FROM " + table + "_stage WHERE source_key=?", partition.key());
                progress(worker, active, failure, "baixando " + partition.kind() + ", H3 " + partition.resolution());
                long[] staged = {0};
                long rows = source.stream(partition, batch -> {
                    checkCancellation(failure);
                    persistBatch(worker, partition, batch);
                    staged[0] += batch.size();
                    progress(worker, active, failure, partition.kind() + ", H3 " + partition.resolution()
                            + ": " + staged[0] + " linhas em lotes temporarios");
                });
                checkCancellation(failure);
                transaction(connection, () -> {
                    worker.update("DELETE FROM " + table + " WHERE source_key=?", partition.key());
                    worker.update(partition.kind().equals("GRID") ? GRID_PUBLISH : PLACES_PUBLISH, partition.key());
                    worker.update("UPDATE annual_grid_partition SET completed=true, rows_loaded=?, completed_at=now()"
                                    + " WHERE source_key=? AND NOT completed", rows, partition.key());
                    worker.update("DELETE FROM " + table + "_stage WHERE source_key=?", partition.key());
                });
                published = true;
            } catch (Exception e) {
                failure.compareAndSet(null, e);
                throw e;
            } finally {
                active.decrementAndGet();
                if (!published) worker.update("DELETE FROM " + table + "_stage WHERE source_key=?", partition.key());
                progress(worker, active, failure, "arquivo " + partition.kind() + ", H3 " + partition.resolution() + " confirmado");
            }
        }
    }

    private void progress(JdbcTemplate worker, AtomicInteger active, AtomicReference<Exception> failure, String stage) {
        String message = failure.get() == null
                ? active.get() + "/" + importWorkers + " trabalhadores ativos; ultimo estagio: " + stage
                : "Encerrando trabalhadores apos falha; " + active.get() + " ainda ativos";
        worker.update("UPDATE annual_grid_bootstrap SET message=? WHERE year=? AND state='loading'", message, YEAR);
    }

    private static void checkCancellation(AtomicReference<Exception> failure) {
        if (Thread.currentThread().isInterrupted() || failure.get() != null) {
            throw new CancellationException("Carga anual interrompida");
        }
    }

    public void requireReady(JdbcTemplate transaction) {
        BootstrapStatus status = status(transaction);
        if (!status.state().equals("ready")) {
            throw new SourceUnavailableException("Cache mundial de 2026 nao esta pronto; use POST /api/v2/bootstrap/2026"
                    + (status.error() == null ? "" : ": " + status.error()));
        }
    }

    private static JdbcTemplate template(Connection connection) {
        return new JdbcTemplate(new SingleConnectionDataSource(connection, true));
    }

    private static List<String> columns(boolean grid) {
        List<String> columns = new ArrayList<>();
        columns.add("resolution");
        if (grid) columns.add("year");
        columns.addAll(grid ? AnnualCacheSource.GRID_COLUMNS : AnnualCacheSource.PLACE_COLUMNS);
        columns.add("source_key");
        return columns;
    }

    private static String insertSql(boolean grid) {
        List<String> columns = columns(grid);
        return "INSERT INTO " + (grid ? "annual_map_grid" : "annual_h3_places") + "_stage ("
                + String.join(",", columns) + ") VALUES ("
                + String.join(",", java.util.Collections.nCopies(columns.size(), "?")) + ")";
    }

    private static String publishSql(boolean grid) {
        String table = grid ? "annual_map_grid" : "annual_h3_places";
        String names = String.join(",", columns(grid));
        // A geometria PostGIS e gerada pelo banco, nunca copiada do staging.
        return "INSERT INTO " + table + " (" + names + ") SELECT " + names
                + " FROM " + table + "_stage WHERE source_key=?";
    }

    private static void persistBatch(JdbcTemplate jdbc, AnnualCacheSource.Partition partition, List<Object[]> batch) {
        boolean grid = partition.kind().equals("GRID");
        jdbc.batchUpdate(grid ? GRID_INSERT : PLACES_INSERT, batch, AnnualCacheSource.BATCH_SIZE, (ps, values) -> {
            int column = 1;
            ps.setInt(column++, partition.resolution());
            if (grid) ps.setInt(column++, YEAR);
            for (Object value : values) ps.setObject(column++, value);
            ps.setString(column, partition.key());
        });
    }

    private static void transaction(Connection connection, Runnable action) throws SQLException {
        connection.setAutoCommit(false);
        try {
            action.run();
            connection.commit();
        } catch (RuntimeException | SQLException e) {
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
        if (cause instanceof java.io.IOException) return "Leitura da fonte: "
                + cause.getClass().getSimpleName() + "; detalhes no log do servidor";
        return "Carga interrompida por " + cause.getClass().getSimpleName() + "; detalhes no log do servidor";
    }
}
