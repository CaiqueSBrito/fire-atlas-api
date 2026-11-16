package com.fireatlas.gold.bootstrap.repository;

import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.common.policy.SourceUnavailableException;
import com.fireatlas.lake.integration.AnnualCacheExports;
import com.fireatlas.lake.integration.AnnualCacheSource;
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
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Plano, lotes, commits e exclusao mutua pertencem ao banco compartilhado, nao ao visitante/JVM. */
@Repository
public class BootstrapRepository {
    private static final Logger log = LoggerFactory.getLogger(BootstrapRepository.class);
    private static final int LOCK_NAMESPACE = 1178686550;
    private static final int YEAR = AnnualCacheSource.YEAR;
    private static final int LOAD_LOCK = 0;
    private static final String GRID_TABLE = "annual_map_grid";
    private static final String PLACES_TABLE = "annual_h3_places";
    private static final List<String> CACHE_TABLES = List.of(GRID_TABLE, PLACES_TABLE);
    private static final List<String> HISTORICAL_TABLES = List.of(GRID_TABLE);
    private static final long PROGRESS_INTERVAL_MS = 1000;
    /** Conexoes alem dos trabalhadores: coordenador (lock), progresso e status/requisicoes concorrentes. */
    static final int RESERVED_CONNECTIONS = 4;

    /**
     * Maiores primeiro: o ultimo arquivo da fila e o que sobra sozinho no fim da carga. Linhas esperadas
     * desconhecidas (partes de lugares, exportacoes em varios arquivos) vao antes, por seguranca.
     */
    private static final Comparator<AnnualCacheSource.Partition> LARGEST_FIRST = Comparator.comparing(
            AnnualCacheSource.Partition::expectedRows, Comparator.nullsFirst(Comparator.reverseOrder()));

    public static final class Lease implements AutoCloseable {
        private final Connection connection;
        private final int year;
        private Lease(Connection connection, int year) { this.connection = connection; this.year = year; }
        public int year() { return year; }
        @Override public void close() {
            try {
                try (var statement = connection.prepareStatement(
                        "SELECT pg_advisory_unlock(?, ?), pg_advisory_unlock(?, ?)")) {
                    statement.setInt(1, LOCK_NAMESPACE);
                    statement.setInt(2, year);
                    statement.setInt(3, LOCK_NAMESPACE);
                    statement.setInt(4, LOAD_LOCK);
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
        if (importWorkers < 1 || importWorkers > 12) {
            throw new IllegalArgumentException("bootstrap.import-workers deve estar entre 1 e 12");
        }
        if (dataSource instanceof HikariDataSource pool && pool.getMaximumPoolSize() > 0
                && pool.getMaximumPoolSize() < importWorkers + RESERVED_CONNECTIONS) {
            throw new IllegalArgumentException("Pool JDBC deve reservar bootstrap.import-workers + "
                    + RESERVED_CONNECTIONS + " conexoes");
        }
        this.dataSource = dataSource;
        this.jdbc = jdbc;
        this.source = source;
        this.refreshHours = refreshHours;
        this.importWorkers = importWorkers;
    }

    public BootstrapStatus status(int year) {
        validateYear(year);
        return status(jdbc, year);
    }

    private BootstrapStatus status(JdbcTemplate jdbc, int year) {
        var statuses = jdbc.query("""
                SELECT b.state, b.message, b.error, b.phase, b.finalization_completed, b.finalization_total, b.planned_units,
                    (SELECT count(*) FROM annual_grid_partition WHERE year=b.year) AS total,
                    (SELECT count(*) FROM annual_grid_partition WHERE year=b.year AND completed) AS completed,
                    (SELECT coalesce(sum(rows_loaded), 0) FROM annual_grid_partition
                        WHERE year=b.year AND completed AND kind='GRID') AS rows,
                    EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=? AND objid=CAST(b.year AS oid)
                        AND pid=b.loader_pid AND objsubid=2 AND granted
                        AND database=(SELECT oid FROM pg_database WHERE datname=current_database())) AS active
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
            } else if (state.equals("ready") && (total == 0 || total != rs.getLong("planned_units") || completed != total
                    || rs.getLong("finalization_completed") != rs.getLong("finalization_total"))) {
                state = "failed";
                message = "Cache anual incompleto";
                error = "O plano duravel ainda possui particoes pendentes";
            }
            return new BootstrapStatus(state, completed, total, rs.getLong("rows"), message, error,
                    rs.getString("phase"), rs.getLong("finalization_completed"), rs.getLong("finalization_total"));
        }, LOCK_NAMESPACE, year);
        return statuses.isEmpty()
                ? new BootstrapStatus("idle", 0, 0, 0, "Aguardando carga mundial de " + year, null, "preparing", 0, 0)
                : statuses.getFirst();
    }

    /** O servico e o unico iniciador: requisicoes de consulta nunca iniciam trabalho. */
    public Lease claim(int year) {
        validateYear(year);
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            JdbcTemplate locked = template(connection);
            if (!Boolean.TRUE.equals(locked.queryForObject("SELECT pg_try_advisory_lock(?, ?)", Boolean.class,
                    LOCK_NAMESPACE, LOAD_LOCK))) {
                connection.close();
                return null;
            }
            // O lock global serializa a carga; este marcador identifica o ano mesmo se o pool reutilizar o PID.
            try (var marker = connection.prepareStatement("SELECT pg_advisory_lock(?, ?)")) {
                marker.setInt(1, LOCK_NAMESPACE);
                marker.setInt(2, year);
                marker.execute();
            }
            Lease lease = new Lease(connection, year);
            locked.update("INSERT INTO annual_grid_bootstrap (year, message) VALUES (?, ?)"
                    + " ON CONFLICT (year) DO NOTHING", year, "Aguardando carga mundial de " + year);
            var checked = locked.queryForMap("SELECT state, checked_at FROM annual_grid_bootstrap WHERE year=?", year);
            var timestamp = (java.sql.Timestamp) checked.get("checked_at");
            if ("ready".equals(checked.get("state")) && status(locked, year).state().equals("ready") && timestamp != null
                    && timestamp.toInstant().isAfter(Instant.now().minus(refreshHours, ChronoUnit.HOURS))) {
                lease.close();
                return null;
            }
            locked.update("UPDATE annual_grid_bootstrap SET state='loading', error=NULL, message=?,"
                            + " phase='preparing', finalization_completed=0, finalization_total=0,"
                            + " planned_units=0, loader_pid=pg_backend_pid() WHERE year=?",
                    "Lendo manifestos Delta da cobertura mundial de " + year, year);
            return lease;
        } catch (SQLException | RuntimeException e) {
            if (connection != null) new Lease(connection, year).close();
            log.error("Nao foi possivel reivindicar a carga anual compartilhada", e);
            throw new SourceUnavailableException("Nao foi possivel iniciar o cache anual no banco compartilhado");
        }
    }

    public List<Integer> historicalYears() throws IOException { return source.historicalYears(); }

    private static void validateYear(int year) {
        if (year < 1 || year > YEAR) throw new IllegalArgumentException("ano invalido para cache: " + year);
    }

    /**
     * Exportacao e importacao correm juntas: cada arquivo entra na fila assim que seu UNLOAD termina. O {@code ready}
     * so e gravado depois de o plano completo voltar (versoes Delta reconferidas), de todos os arquivos confirmados e
     * dos indices recriados.
     */
    public void load(Lease lease) {
        JdbcTemplate locked = template(lease.connection);
        int year = lease.year;
        long started = System.nanoTime();
        var run = new ImportRun(lease.connection, locked, year);
        try {
            var plan = source.plan(year, year == YEAR, run::exportProgress, run);
            run.planned(plan);
            run.finish();
            run.restoreIndexes();
            transaction(lease.connection, () -> {
                Long pending = locked.queryForObject(
                        "SELECT count(*) FROM annual_grid_partition WHERE year=? AND NOT completed", Long.class, year);
                if (pending == null || pending != 0) throw new IllegalStateException("particoes pendentes");
                if (!Boolean.TRUE.equals(locked.queryForObject("SELECT planned_units>0 AND planned_units="
                        + "(SELECT count(*) FROM annual_grid_partition WHERE year=?)"
                        + " AND finalization_completed=finalization_total"
                        + " FROM annual_grid_bootstrap WHERE year=?", Boolean.class, year, year))) {
                    throw new IllegalStateException("finalizacao pendente");
                }
                long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started);
                String elapsed = (seconds / 60) + " min " + String.format("%02d", seconds % 60) + " s";
                // A duracao fica gravada na mensagem persistida; inclui plano, exportacao Athena e importacao.
                locked.update("UPDATE annual_grid_bootstrap SET state='ready', phase='ready', checked_at=now(),"
                                + " loader_pid=NULL, error=NULL, message=? WHERE year=?",
                        "Cobertura mundial de " + year + " pronta em " + elapsed + " (" + importWorkers
                                + " trabalhadores): todos os arquivos e resolucoes confirmados", year);
                log.info("Carga anual de {} concluida em {} com {} trabalhadores", year, elapsed, importWorkers);
            });
        } catch (Exception e) {
            boolean interrupted = Thread.interrupted() || e instanceof InterruptedException;
            // Somente apos todos os escritores sairem o lock pode ser liberado.
            run.cancel();
            log.error("Carga anual de {} falhou; particoes confirmadas serao reutilizadas", year, e);
            try {
                locked.update("UPDATE annual_grid_bootstrap SET state='failed', loader_pid=NULL, message=?, error=? WHERE year=?",
                        "Nao foi possivel concluir a carga; tente novamente para retomar", safeError(e), year);
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        } finally {
            run.close();
            lease.close();
        }
    }

    /** Uma carga: fila de importacao alimentada pela exportacao, trabalhadores, progresso e indices adiados. */
    private final class ImportRun implements AnnualCacheExports.Listener, AutoCloseable {
        private record Queued(AnnualCacheSource.Partition partition, long order) {}

        private final Connection lease;
        private final JdbcTemplate locked;
        private final int year;
        private final PriorityBlockingQueue<Queued> queue = new PriorityBlockingQueue<>(128,
                Comparator.comparing(Queued::partition, LARGEST_FIRST).thenComparingLong(Queued::order));
        /** Somente o coordenador submete: exportacao e plano chamam no mesmo thread. */
        private final Set<String> submitted = new HashSet<>();
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger imported = new AtomicInteger();
        private final AtomicReference<Exception> failure = new AtomicReference<>();
        private final AtomicReference<String> exportStage = new AtomicReference<>();
        private final AtomicReference<String> importStage = new AtomicReference<>();
        private final AtomicReference<String> finalStage = new AtomicReference<>();
        private long order;
        private volatile boolean closed;
        private ExecutorService workers;
        private ScheduledExecutorService publisher;
        private PgBinaryCopy gridCopy;
        private PgBinaryCopy placesCopy;
        private String published;

        ImportRun(Connection lease, JdbcTemplate locked, int year) {
            this.lease = lease;
            this.locked = locked;
            this.year = year;
        }

        void exportProgress(String message) {
            exportStage.set(message);
        }

        /** Antes do primeiro UNLOAD: substitui somente os arquivos pertencentes ao ano desta carga. */
        @Override
        public void snapshot(String id) {
            try {
                transaction(lease, () -> {
                    deletePartitions("strpos(source_key, ?) = 0", id);
                });
                prepare();
            } catch (Exception e) {
                throw unchecked(e);
            }
        }

        @Override
        public void exported(List<AnnualCacheSource.Partition> partitions) {
            try {
                if (workers == null) prepare();
                partitions.stream().sorted(LARGEST_FIRST).forEach(this::submit);
            } catch (Exception e) {
                throw unchecked(e);
            }
        }

        /** Plano completo e reconferido: limpa o que ficou fora dele e submete o que a exportacao nao entregou. */
        void planned(AnnualCacheSource.Plan plan) throws Exception {
            if (plan.partitions().isEmpty()) throw new IllegalStateException("plano anual vazio; faltam particoes explicitas");
            String[] keys = plan.partitions().stream().map(AnnualCacheSource.Partition::key).toArray(String[]::new);
            if (keys.length != new HashSet<>(List.of(keys)).size()) {
                throw new IllegalStateException("plano anual com chaves duplicadas");
            }
            transaction(lease, () -> {
                deletePartitions("NOT (source_key = ANY (?))", lease.createArrayOf("text", keys));
                locked.update("UPDATE annual_grid_bootstrap SET planned_units=? WHERE year=?", keys.length, year);
            });
            if (workers == null) prepare();
            plan.partitions().stream().sorted(LARGEST_FIRST).forEach(this::submit);
            locked.update("UPDATE annual_grid_bootstrap SET grid_version=?, places_version=? WHERE year=?",
                    plan.gridVersion(), plan.placesVersion(), year);
            exportStage.set(null);
        }

        /**
         * Somente a carga inicial pode adiar indices: anos publicados continuam disponiveis durante o historico.
         * A continuacao de uma carga inicial interrompida ainda aproveita as definicoes duraveis.
         */
        private void prepare() throws Exception {
            transaction(lease, () -> {
                boolean unpublished = !Boolean.TRUE.equals(locked.queryForObject(
                        "SELECT EXISTS (SELECT 1 FROM annual_grid_bootstrap WHERE checked_at IS NOT NULL)", Boolean.class));
                if (unpublished) for (String table : tables()) {
                    boolean empty = !Boolean.TRUE.equals(locked.queryForObject(
                            "SELECT EXISTS (SELECT 1 FROM " + table + ")", Boolean.class));
                    if (empty || DeferredIndexes.hasDeferred(locked, table)) DeferredIndexes.defer(locked, table);
                }
            });
            locked.update("UPDATE annual_grid_bootstrap SET phase='importing', finalization_completed=0,"
                    + " finalization_total=? WHERE year=?", DeferredIndexes.finalizationWork(locked) + 1, year);
            gridCopy = PgBinaryCopy.forTable(locked, GRID_TABLE, columns(true));
            if (year == YEAR) placesCopy = PgBinaryCopy.forTable(locked, PLACES_TABLE, columns(false));
            workers = Executors.newFixedThreadPool(importWorkers, runnable -> new Thread(runnable, "annual-cache-import-" + year));
            for (int i = 0; i < importWorkers; i++) workers.submit(this::work);
            publisher = Executors.newSingleThreadScheduledExecutor(runnable -> {
                var thread = new Thread(runnable, "annual-cache-progress-" + year);
                thread.setDaemon(true);
                return thread;
            });
            publisher.scheduleWithFixedDelay(this::publish, PROGRESS_INTERVAL_MS, PROGRESS_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }

        private void submit(AnnualCacheSource.Partition partition) {
            if ((!partition.kind().equals("GRID") && !partition.kind().equals("PLACES"))
                    || (year != YEAR && partition.kind().equals("PLACES"))
                    || partition.resolution() < 0 || partition.resolution() > 5) {
                throw new IllegalStateException("particao fora do plano anual permitido");
            }
            Exception error = failure.get();
            if (error != null) throw new IllegalStateException("importacao do cache falhou", error);
            if (!submitted.add(partition.key())) return;
            // Mesma chave com metadados diferentes e outro arquivo: sai com suas linhas antes de entrar de novo.
            deletePartitions("source_key=? AND (file_size IS DISTINCT FROM CAST(? AS bigint)"
                            + " OR expected_rows IS DISTINCT FROM CAST(? AS bigint) OR kind<>? OR resolution<>? OR empty_partition<>?)",
                    partition.key(), partition.size(), partition.expectedRows(), partition.kind(),
                    partition.resolution(), partition.empty());
            locked.update("INSERT INTO annual_grid_partition"
                            + " (year, source_key, kind, resolution, file_size, expected_rows, empty_partition)"
                            + " VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (year, source_key) DO NOTHING",
                    year, partition.key(), partition.kind(), partition.resolution(), partition.size(),
                    partition.expectedRows(), partition.empty());
            if (Boolean.TRUE.equals(locked.queryForObject(
                    "SELECT completed FROM annual_grid_partition WHERE year=? AND source_key=?",
                    Boolean.class, year, partition.key()))) {
                return;
            }
            queue.add(new Queued(partition, order++));
        }

        /**
         * Com a FK adiada nao ha cascade: as linhas saem explicitamente (so arquivos concluidos tem linhas). Sem indice
         * em partition_id o DELETE varre a tabela, entao so roda quando ha de fato um arquivo concluido a remover.
         */
        private void deletePartitions(String condition, Object... args) {
            Object[] scoped = new Object[args.length + 1];
            scoped[0] = year;
            System.arraycopy(args, 0, scoped, 1, args.length);
            List<Integer> ids = locked.queryForList("SELECT id FROM annual_grid_partition WHERE year=? AND completed AND ("
                    + condition + ")", Integer.class, scoped);
            if (!ids.isEmpty()) {
                Object array = locked.execute((java.sql.Connection c) -> c.createArrayOf("integer", ids.toArray()));
                for (String table : tables()) {
                    locked.update("DELETE FROM " + table + " WHERE partition_id = ANY (?)", array);
                }
            }
            locked.update("DELETE FROM annual_grid_partition WHERE year=? AND (" + condition + ")", scoped);
        }

        private void work() {
            try {
                while (failure.get() == null && !Thread.currentThread().isInterrupted()) {
                    Queued next = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (next == null) {
                        // closed antes de isEmpty: tudo o que foi submetido ja esta na fila.
                        if (closed && queue.isEmpty()) return;
                        continue;
                    }
                    importPartition(next.partition());
                    imported.incrementAndGet();
                }
            } catch (Exception e) {
                if (failure.compareAndSet(null, e)) workers.shutdownNow();
            }
        }

        /** Um arquivo, uma transacao: COPY binario direto na tabela final; o rollback descarta linhas parciais. */
        private void importPartition(AnnualCacheSource.Partition partition) throws Exception {
            // Nunca emprestar a conexao do advisory lock nem compartilhar um JdbcTemplate entre workers.
            try (Connection connection = dataSource.getConnection()) {
                JdbcTemplate worker = template(connection);
                boolean grid = partition.kind().equals("GRID");
                String table = grid ? GRID_TABLE : PLACES_TABLE;
                PgBinaryCopy encoder = grid ? gridCopy : placesCopy;
                String label = partition.kind() + ", H3 " + partition.resolution();
                active.incrementAndGet();
                try {
                    checkCancellation();
                    importStage.set("baixando " + label);
                    transaction(connection, () -> {
                        // Arquivo nao concluido nao tem linhas: linhas e marcador sao gravados na mesma transacao.
                        Integer id = worker.queryForObject("SELECT id FROM annual_grid_partition WHERE year=? AND source_key=?",
                                Integer.class, year, partition.key());
                        CopyIn[] copy = {null};
                        var buffer = new Buffer();
                        var out = new DataOutputStream(buffer);
                        long[] written = {0};
                        long rows;
                        try {
                            rows = source.stream(partition, batch -> {
                                checkCancellation();
                                try {
                                    buffer.reset();
                                    if (copy[0] == null) {
                                        // O COPY so abre com o arquivo ja baixado: o banco nao espera o download.
                                        copy[0] = connection.unwrap(PGConnection.class).getCopyAPI().copyIn(encoder.sql(table));
                                        out.write(PgBinaryCopy.HEADER);
                                    }
                                    for (Object[] values : batch) {
                                        encoder.beginRow(out);
                                        int column = 0;
                                        encoder.field(out, column++, partition.resolution());
                                        if (grid) encoder.field(out, column++, year);
                                        for (Object value : values) encoder.field(out, column++, value);
                                        encoder.field(out, column, id);
                                    }
                                    out.flush();
                                    copy[0].writeToCopy(buffer.array(), 0, buffer.size());
                                } catch (IOException | SQLException e) {
                                    throw new IllegalStateException("COPY binario falhou em " + table, e);
                                }
                                written[0] += batch.size();
                                importStage.set(label + ": " + written[0] + " linhas gravadas (visiveis ao confirmar o arquivo)");
                            });
                            if (copy[0] != null) {
                                copy[0].writeToCopy(PgBinaryCopy.TRAILER, 0, PgBinaryCopy.TRAILER.length);
                                copy[0].endCopy();
                            }
                        } finally {
                            if (copy[0] != null && copy[0].isActive()) copy[0].cancelCopy();
                        }
                        // clock_timestamp(): fim do arquivo. now() seria o inicio da transacao, ou seja, do download.
                        worker.update("UPDATE annual_grid_partition SET completed=true, rows_loaded=?,"
                                + " completed_at=clock_timestamp() WHERE id=? AND NOT completed", rows, id);
                    });
                    importStage.set("arquivo " + label + " confirmado");
                } finally {
                    active.decrementAndGet();
                }
            }
        }

        /** Sem novos arquivos: espera a fila esvaziar e todos os trabalhadores sairem. */
        void finish() throws Exception {
            closed = true;
            if (workers != null) {
                workers.shutdown();
                awaitWorkers();
            }
            Exception error = failure.get();
            if (error != null) throw error;
        }

        /** Falha/interrupcao: cancela e so retorna depois do ultimo escritor encerrar. */
        void cancel() {
            failure.compareAndSet(null, new CancellationException("Carga anual interrompida"));
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

        void restoreIndexes() throws Exception {
            setFinalPhase("indexes", "Recriando indices do cache");
            DeferredIndexes.restore(dataSource, locked, importWorkers, this::setFinalPhase, this::finalizationCompleted);
            if (imported.get() > 0) {
                setFinalPhase("analyzing", "Atualizando estatisticas do cache");
                long started = System.nanoTime();
                locked.execute("ANALYZE " + String.join(", ", tables()));
                finalizationCompleted();
                log.info("ANALYZE do cache em {} ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
            } else {
                // Revalidacao sem arquivos novos nao precisa de ANALYZE; nao conta operacao que nao aconteceu.
                locked.update("UPDATE annual_grid_bootstrap SET finalization_total=finalization_total-1 WHERE year=?", year);
            }
        }

        private void setFinalPhase(String phase, String message) {
            finalStage.set(message);
            locked.update("UPDATE annual_grid_bootstrap SET phase=?, message=? WHERE year=?", phase, message, year);
        }

        private void finalizationCompleted() {
            // Chamado por cada construtor de indice, somente depois do DDL terminar (ou confirmar que ja existe).
            jdbc.update("UPDATE annual_grid_bootstrap SET finalization_completed=finalization_completed+1"
                    + " WHERE year=?", year);
        }

        /** Thread proprio e conexao do pool: trabalhadores nunca escrevem progresso dentro da transacao do arquivo. */
        private void publish() {
            try {
                String message;
                if (failure.get() != null) {
                    message = "Encerrando trabalhadores apos falha; " + active.get() + " ainda ativos";
                } else if (finalStage.get() != null) {
                    message = finalStage.get();
                } else {
                    String export = exportStage.get();
                    String importing = importStage.get();
                    if (export == null && importing == null) return;
                    message = (export == null ? "" : export + "; ") + (importing == null ? "importacao aguardando arquivos"
                            : active.get() + "/" + importWorkers + " trabalhadores ativos; ultimo estagio: " + importing);
                }
                if (message.equals(published)) return;
                jdbc.update("UPDATE annual_grid_bootstrap SET message=? WHERE year=? AND state='loading'", message, year);
                published = message;
            } catch (RuntimeException e) {
                log.warn("Falha publicando progresso da carga anual", e);
            }
        }

        private void checkCancellation() {
            if (Thread.currentThread().isInterrupted() || failure.get() != null) {
                throw new CancellationException("Carga anual interrompida");
            }
        }

        private List<String> tables() {
            return year == YEAR ? CACHE_TABLES : HISTORICAL_TABLES;
        }

        @Override
        public void close() {
            if (publisher == null) return;
            publisher.shutdownNow();
            try {
                publisher.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Expoe o array interno: o lote vai ao COPY sem uma copia extra. */
    private static final class Buffer extends ByteArrayOutputStream {
        Buffer() { super(1 << 16); }
        byte[] array() { return buf; }
    }

    public void requireReady(JdbcTemplate transaction) {
        BootstrapStatus status = status(transaction, YEAR);
        if (!status.state().equals("ready")) {
            throw new SourceUnavailableException("Cache mundial de 2026 nao esta pronto; use POST /api/v2/bootstrap/2026"
                    + (status.error() == null ? "" : ": " + status.error()));
        }
    }

    /** Apenas planos completos e publicados, no mesmo snapshot das linhas consultadas. */
    public List<Integer> readyYears(JdbcTemplate transaction) {
        return transaction.queryForList("""
                SELECT b.year FROM annual_grid_bootstrap b
                WHERE b.state='ready' AND b.planned_units>0
                    AND b.finalization_completed=b.finalization_total
                    AND b.planned_units=(SELECT count(*) FROM annual_grid_partition p WHERE p.year=b.year)
                    AND NOT EXISTS (SELECT 1 FROM annual_grid_partition p WHERE p.year=b.year AND NOT p.completed)
                ORDER BY b.year DESC
                """, Integer.class);
    }

    private static JdbcTemplate template(Connection connection) {
        return new JdbcTemplate(new SingleConnectionDataSource(connection, true));
    }

    /** A geometria PostGIS de lugares e coluna gerada pelo banco, nunca copiada. */
    private static List<String> columns(boolean grid) {
        List<String> columns = new ArrayList<>();
        columns.add("resolution");
        if (grid) columns.add("year");
        columns.addAll(grid ? AnnualCacheSource.GRID_COLUMNS : AnnualCacheSource.PLACE_COLUMNS);
        columns.add("partition_id");
        return columns;
    }

    private static RuntimeException unchecked(Exception e) {
        return e instanceof RuntimeException runtime ? runtime : new IllegalStateException(e.getMessage(), e);
    }

    @FunctionalInterface
    private interface Work {
        void run() throws Exception;
    }

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
        if (cause instanceof java.io.IOException) return "Leitura da fonte: "
                + cause.getClass().getSimpleName() + "; detalhes no log do servidor";
        return "Carga interrompida por " + cause.getClass().getSimpleName() + "; detalhes no log do servidor";
    }
}
