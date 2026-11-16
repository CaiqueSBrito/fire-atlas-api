package com.fireatlas.gold;

import com.fireatlas.gold.bootstrap.repository.BootstrapRepository;
import com.fireatlas.gold.bootstrap.repository.ForecastCacheRepository;
import com.fireatlas.gold.common.policy.SourceUnavailableException;
import com.fireatlas.gold.forecast.mapper.ForecastMapper;
import com.fireatlas.gold.forecast.repository.ForecastRepository;
import com.fireatlas.gold.forecast.service.ForecastService;
import com.fireatlas.gold.h3.policy.H3Index;
import com.fireatlas.gold.region.entity.Region;
import com.fireatlas.lake.integration.AnnualCacheExports;
import com.fireatlas.lake.integration.AnnualCacheSource;
import com.fireatlas.lake.integration.ForecastCacheSource;
import com.fireatlas.lake.integration.AthenaExportStore;
import com.fireatlas.lake.integration.AthenaQuery;
import com.fireatlas.lake.integration.S3LakeStorage;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Apenas schemas efemeros e fonte controlada: COPY, commits, retomada e SQL sao reais. */
@ExtendWith(DbAvailable.class)
@Timeout(30)
public class ForecastCachePostgresTest {
    public static final LocalDate ISSUE = LocalDate.of(2026, 11, 14);
    private DataSource dataSource;
    private JdbcTemplate jdbc;
    private JdbcTemplate admin;
    private String schema;
    private ForecastCacheSource source;
    private ForecastCacheRepository bootstrap;
    private ForecastRepository repository;
    private List<Long> cells;
    private ExecutorService coordinator;
    private final Set<Integer> workerBackends = ConcurrentHashMap.newKeySet();

    @BeforeEach
    void createSchema() {
        admin = new JdbcTemplate(new DriverManagerDataSource(DbAvailable.URL, DbAvailable.USER, DbAvailable.PASSWORD));
        schema = "forecast_test_" + UUID.randomUUID().toString().replace("-", "");
        admin.execute("CREATE SCHEMA " + schema);
        var driver = new DriverManagerDataSource(DbAvailable.URL, DbAvailable.USER, DbAvailable.PASSWORD);
        Properties properties = new Properties();
        properties.setProperty("currentSchema", schema + ",public");
        driver.setConnectionProperties(properties);
        dataSource = new DelegatingDataSource(driver) {
            @Override public Connection getConnection() throws SQLException {
                Connection connection = super.getConnection();
                if (Thread.currentThread().getName().equals("forecast-cache-import")) {
                    try (var statement = connection.createStatement();
                         var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                        result.next();
                        workerBackends.add(result.getInt(1));
                    }
                }
                return connection;
            }
        };
        jdbc = new JdbcTemplate(dataSource);
        migrate(dataSource);
        source = mock(ForecastCacheSource.class);
        bootstrap = new ForecastCacheRepository(dataSource, jdbc, source, 6);
        repository = new ForecastRepository(jdbc, bootstrap);
        long parent = TestH3.cell(-3, -54, 5);
        cells = H3Index.toResolution(List.of(parent), 6, 20).stream().sorted().toList();
        coordinator = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void stopCoordinatorAndDropSchema() throws Exception {
        if (coordinator != null) {
            coordinator.shutdownNow();
            assertTrue(coordinator.awaitTermination(10, TimeUnit.SECONDS), "coordenador nao encerrou");
        }
        if (schema != null) admin.execute("DROP SCHEMA " + schema + " CASCADE");
    }

    /** Reutilizavel pelo smoke HTTP com o mesmo schema isolado, sem resetar o cache da aplicacao. */
    public static void migrate(DataSource dataSource) {
        new ResourceDatabasePopulator(new ClassPathResource("db/migration/V14__annual_grid_cache.sql"),
                new ClassPathResource("db/migration/V15__annual_cache_postgis.sql"),
                new ClassPathResource("db/migration/V16__annual_athena_exports.sql"),
                new ClassPathResource("db/migration/V17__annual_athena_monthly_exports.sql"),
                new ClassPathResource("db/migration/V18__annual_cache_direct_copy.sql"),
                new ClassPathResource("db/migration/V19__annual_athena_export_parts.sql"),
                new ClassPathResource("db/migration/V20__annual_cache_unlogged.sql"),
                new ClassPathResource("db/migration/V21__annual_cache_deferred_indexes.sql"),
                new ClassPathResource("db/migration/V22__annual_cache_finalization_progress.sql"),
                new ClassPathResource("db/migration/V23__annual_cache_historical_years.sql"),
                new ClassPathResource("db/migration/V24__forecast_cache.sql"),
                new ClassPathResource("db/migration/V25__forecast_export_unknown_size.sql"),
                new ClassPathResource("db/migration/V26__forecast_monthly_exports.sql")).execute(dataSource);
    }

    public static AnnualCacheSource.Partition partition(String key, long rows) {
        return new AnnualCacheSource.Partition(key, "FORECAST", 6, rows == 0 ? 0 : -1, rows, rows == 0);
    }

    /** A ordem corresponde exatamente a FORECAST_COLUMNS, inclusive os quatro metadados nativos r6. */
    public static Object[] row(LocalDate issue, long cell, double probability, Boolean active) {
        return new Object[]{issue, cell, H3Index.parentBits(cell, 1), probability, 1.0, 2.0, 3.0, 4.5,
                300.25, 301.5, 5.0, 6.0, 7.0, active, "Brasil", "Santarem", 4.25, -3.0, -54.0,
                "South America", "BRA", "BR", 3389353L};
    }

    public static ForecastCacheSource controlledSource(String snapshot,
                                                       Map<AnnualCacheSource.Partition, List<Object[]>> files) throws Exception {
        ForecastCacheSource source = mock(ForecastCacheSource.class);
        List<AnnualCacheSource.Partition> partitions = List.copyOf(files.keySet());
        doAnswer(invocation -> {
            AnnualCacheExports.Listener listener = invocation.getArgument(1);
            listener.snapshot(snapshot);
            for (var partition : partitions) listener.exported(List.of(partition));
            return new ForecastCacheSource.Plan(1, 1, snapshot, partitions);
        }).when(source).plan(any(), any());
        doAnswer(invocation -> {
            var partition = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            var rows = files.get(partition);
            if (rows == null) throw new IOException("arquivo fora da fonte controlada");
            // Lotes pequenos exercitam varias escritas no mesmo COPY sem materializacao do conjunto real.
            for (var values : rows) consumer.accept(List.<Object[]>of(values));
            return (long) rows.size();
        }).when(source).stream(any(), any());
        return source;
    }

    private void useSource(ForecastCacheSource controlled) {
        source = controlled;
        bootstrap = new ForecastCacheRepository(dataSource, jdbc, source, 6);
        repository = new ForecastRepository(jdbc, bootstrap);
    }

    private Region.Cover cover() { return new Region.Cover(5, List.of(H3Index.parentBits(cells.getFirst(), 5))); }

    private void expire() { jdbc.update("UPDATE forecast_cache_bootstrap SET checked_at=now()-interval '7 hours'"); }

    private void awaitCommitted(String snapshot, long files) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            Long complete = jdbc.queryForObject("SELECT count(*) FROM forecast_cache_partition WHERE snapshot_id=? AND completed",
                    Long.class, snapshot);
            if (complete != null && complete == files) return;
            Thread.sleep(10);
        }
        throw new IOException("COPY nao confirmou durante a exportacao");
    }
    private Future<?> loadAsync() {
        var lease = bootstrap.claim();
        assertNotNull(lease);
        return coordinator.submit(() -> bootstrap.load(lease));
    }

    private void assertLoadLocksHeld() {
        assertEquals(3, jdbc.queryForObject("""
                SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objsubid=2 AND granted
                    AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
                    AND pid=(SELECT loader_pid FROM forecast_cache_bootstrap WHERE id)
                    AND ((classid=1178686550 AND objid=0) OR (classid=1178686551 AND objid IN (0,1)))
                """, Integer.class), "o coordenador conserva o lock anual e ambos os locks da previsao");
    }

    private void assertLoadLocksReleased() throws Exception {
        try (var connection = dataSource.getConnection();
             var statement = connection.createStatement();
             var locks = statement.executeQuery("""
                     SELECT pg_try_advisory_lock(1178686550,0), pg_try_advisory_lock(1178686551,0),
                         pg_try_advisory_lock(1178686551,1)
                     """)) {
            assertTrue(locks.next());
            assertTrue(locks.getBoolean(1), "lock anual ainda retido");
            assertTrue(locks.getBoolean(2), "lock global da previsao ainda retido");
            assertTrue(locks.getBoolean(3), "lock do dono da previsao ainda retido");
        }
    }

    private void awaitProgress(String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (bootstrap.status().message().contains(message)) return;
            Thread.sleep(10);
        }
        throw new IOException("progresso da previsao nao publicou " + message);
    }

    private static void await(CountDownLatch latch) throws InterruptedException, IOException {
        if (!latch.await(10, TimeUnit.SECONDS)) throw new IOException("timeout aguardando sincronizacao do teste");
    }

    private static void awaitIgnoringInterrupts(CountDownLatch latch) throws IOException {
        boolean interrupted = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        try {
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new IOException("timeout aguardando cleanup do teste");
                try {
                    if (!latch.await(remaining, TimeUnit.NANOSECONDS)) throw new IOException("timeout aguardando cleanup do teste");
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }


    @Test
    void passiveStatusRequiresPublicationAndSerializesAnnualAndForecastLeases() {
        assertEquals("idle", bootstrap.status().state());
        assertThrows(SourceUnavailableException.class, repository::findIssues);
        assertThrows(SourceUnavailableException.class,
                () -> repository.read(snapshot -> snapshot.findForecast(new Region.Cover(6, List.of()), ISSUE, 0, false, 10)));
        verifyNoInteractions(source);
        var annual = new BootstrapRepository(dataSource, jdbc, mock(AnnualCacheSource.class), 6, 1);
        var lease = bootstrap.claim();
        assertNotNull(lease);
        try {
            assertNull(new ForecastCacheRepository(dataSource, jdbc, source, 6).claim());
            assertEquals("loading", bootstrap.status().state());
            assertNull(annual.claim(2026), "a previsao deve excluir qualquer carga anual/historica");
            assertLoadLocksHeld();
        } finally {
            lease.close();
        }
        assertEquals("failed", bootstrap.status().state(), "PID sem advisory lock nao e dono ativo");
        try (var annualLease = annual.claim(2026)) {
            assertNotNull(annualLease);
            assertNull(bootstrap.claim(), "a carga anual deve excluir qualquer carga da previsao");
        }
        try (var next = bootstrap.claim()) {
            assertNotNull(next, "os dois tipos de carga liberam seus locks ao fechar");
        }
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_h3_places", Integer.class));
    }
    @Test
    void fourImportersRequireTheirConnectionsAndTheSharedReserve() {
        try (var pool = new HikariDataSource()) {
            pool.setMaximumPoolSize(7);
            assertThrows(IllegalArgumentException.class, () -> new ForecastCacheRepository(pool, jdbc, source, 6));
            pool.setMaximumPoolSize(8);
            assertDoesNotThrow(() -> new ForecastCacheRepository(pool, jdbc, source, 6));
        }
    }

    @Test
    void pooledClaimsReleaseAllLocksOnContentionFailureFreshnessAndRepeatedClose() throws Exception {
        var empty = partition("empty://pooled", 0);
        useSource(controlledSource("pooled", Map.of(empty, List.of())));
        try (var pool = new HikariDataSource()) {
            pool.setDataSource(dataSource);
            pool.setMinimumIdle(0);
            pool.setMaximumPoolSize(8);
            var pooled = new ForecastCacheRepository(pool, new JdbcTemplate(pool), source, 6);
            var annual = new BootstrapRepository(dataSource, jdbc, mock(AnnualCacheSource.class), 6, 1);
            try (var annualLease = annual.claim(2026)) {
                assertNotNull(annualLease);
                assertNull(pooled.claim(), "ano em carga impede a previsao sem esperar pelo pool");
            }
            assertLoadLocksReleased();

            try (var blocker = dataSource.getConnection(); var statement = blocker.createStatement()) {
                statement.execute("SELECT pg_advisory_lock(1178686551,0)");
                assertNull(pooled.claim(), "lock da previsao ocupado libera o lock anual recem-adquirido");
                try (var annualLease = annual.claim(2026)) {
                    assertNotNull(annualLease);
                }
            }
            assertLoadLocksReleased();

            jdbc.execute("ALTER TABLE forecast_cache_bootstrap RENAME TO forecast_cache_bootstrap_unavailable");
            try {
                assertThrows(SourceUnavailableException.class, pooled::claim);
            } finally {
                jdbc.execute("ALTER TABLE forecast_cache_bootstrap_unavailable RENAME TO forecast_cache_bootstrap");
            }
            assertLoadLocksReleased();

            var lease = pooled.claim();
            assertNotNull(lease);
            lease.close();
            // Fechar novamente nao pode desbloquear nem fechar uma sessao ja emprestada a outra carga.
            try (var next = pooled.claim()) {
                assertNotNull(next);
                lease.close();
                assertLoadLocksHeld();
            }
            assertLoadLocksReleased();

            pooled.load(pooled.claim());
            assertEquals("ready", pooled.status().state());
            assertLoadLocksReleased();
            assertNull(pooled.claim(), "publicacao recente nao inicia nova carga");
            assertLoadLocksReleased();
        }
    }

    @Test
    void fourDistinctBackendWorkersRunTogetherAndPublicationWaitsForEveryCommit() throws Exception {
        var first = partition("s3://parallel/a", 1);
        var second = partition("s3://parallel/b", 1);
        var third = partition("s3://parallel/c", 1);
        var last = partition("s3://parallel/d", 1);
        var files = new LinkedHashMap<AnnualCacheSource.Partition, List<Object[]>>();
        int index = 0;
        for (var part : List.of(first, second, third, last)) {
            files.put(part, List.<Object[]>of(row(ISSUE, cells.get(index++), 0.71, true)));
        }
        useSource(controlledSource("parallel", files));
        CountDownLatch entered = new CountDownLatch(4);
        CountDownLatch finishThree = new CountDownLatch(1);
        CountDownLatch finishLast = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        doAnswer(invocation -> {
            var part = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            active.incrementAndGet();
            try {
                consumer.accept(files.get(part));
                entered.countDown();
                await(part.equals(last) ? finishLast : finishThree);
                return 1L;
            } finally {
                active.decrementAndGet();
            }
        }).when(source).stream(any(), any());

        Future<?> loading = loadAsync();
        try {
            await(entered);
            assertEquals(4, active.get());
            assertEquals(4, workerBackends.size(), "quatro COPY simultaneos usam quatro sessoes PostgreSQL distintas");
            assertEquals(4, jdbc.queryForObject("SELECT count(*) FROM pg_stat_activity WHERE pid IN (?,?,?,?)"
                    + " AND datname=current_database() AND xact_start IS NOT NULL", Integer.class, workerBackends.toArray()));
            assertEquals("loading", bootstrap.status().state());
            assertEquals(0, bootstrap.status().completedUnits());
            assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM forecast_cache", Integer.class));
            assertThrows(SourceUnavailableException.class, repository::findIssues);
            assertLoadLocksHeld();
            assertFalse(loading.isDone());
            awaitProgress("4/4 trabalhadores ativos");

            finishThree.countDown();
            awaitCommitted("parallel", 3);
            assertEquals("loading", bootstrap.status().state(), "tres commits nao publicam quatro arquivos");
            assertEquals(3, bootstrap.status().completedUnits());
            assertEquals(3, jdbc.queryForObject("SELECT count(*) FROM forecast_cache", Integer.class));
            assertThrows(SourceUnavailableException.class, repository::findIssues);
            assertFalse(loading.isDone());
        } finally {
            finishThree.countDown();
            finishLast.countDown();
        }
        loading.get(10, TimeUnit.SECONDS);
        assertEquals("ready", bootstrap.status().state());
        assertEquals(4, bootstrap.status().completedUnits());
        assertEquals(4, bootstrap.status().rowsLoaded());
        assertEquals(4, repository.findIssues().getFirst().cells());
        assertLoadLocksReleased();
    }

    @Test
    void failedWorkerJoinsAllOtherWritersBeforeReleasingBothLoadLocksAndResumesOnlyCommits() throws Exception {
        var committed = partition("s3://failure/committed", 1);
        var failed = partition("s3://failure/failed", 1);
        var cancelled = List.of(partition("s3://failure/cancelled-a", 1),
                partition("s3://failure/cancelled-b", 1), partition("s3://failure/cancelled-c", 1));
        var pending = partition("s3://failure/pending", 1);
        var files = new LinkedHashMap<AnnualCacheSource.Partition, List<Object[]>>();
        int index = 0;
        for (var part : List.of(committed, failed, cancelled.get(0), cancelled.get(1), cancelled.get(2), pending)) {
            files.put(part, List.<Object[]>of(row(ISSUE, cells.get(index++), 0.42, false)));
        }
        useSource(controlledSource("failure", files));
        CountDownLatch entered = new CountDownLatch(4);
        CountDownLatch planned = new CountDownLatch(1);
        CountDownLatch failNow = new CountDownLatch(1);
        CountDownLatch cancelledWriters = new CountDownLatch(3);
        CountDownLatch cleanupAllowed = new CountDownLatch(1);
        AtomicBoolean retry = new AtomicBoolean();
        doAnswer(invocation -> {
            AnnualCacheExports.Listener listener = invocation.getArgument(1);
            listener.snapshot("failure");
            listener.exported(List.of(committed));
            awaitCommitted("failure", 1);
            listener.exported(List.of(failed, cancelled.get(0), cancelled.get(1), cancelled.get(2)));
            if (!retry.get()) await(entered);
            listener.exported(List.of(pending));
            planned.countDown();
            return new ForecastCacheSource.Plan(1, 1, "failure", List.copyOf(files.keySet()));
        }).when(source).plan(any(), any());
        doAnswer(invocation -> {
            var part = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(files.get(part));
            if (retry.get() || part.equals(committed) || part.equals(pending)) return 1L;
            entered.countDown();
            if (part.equals(failed)) {
                await(failNow);
                throw new IOException("falha depois do COPY antes do commit");
            }
            try {
                await(new CountDownLatch(1));
                throw new IOException("cancelamento esperado");
            } catch (InterruptedException e) {
                cancelledWriters.countDown();
                awaitIgnoringInterrupts(cleanupAllowed);
                throw new IOException("worker cancelado", e);
            }
        }).when(source).stream(any(), any());

        Future<?> loading = loadAsync();
        try {
            await(planned);
            assertEquals(1, bootstrap.status().completedUnits());
            failNow.countDown();
            await(cancelledWriters);
            assertFalse(loading.isDone(), "cancelar Future nao encerra a transacao dos outros writers");
            assertEquals("loading", bootstrap.status().state());
            assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM forecast_cache", Integer.class));
            assertLoadLocksHeld();
            assertNull(new ForecastCacheRepository(dataSource, jdbc, source, 6).claim());
            assertNull(new BootstrapRepository(dataSource, jdbc, mock(AnnualCacheSource.class), 6, 1).claim(2026));
            verify(source, never()).stream(eq(pending), any());
        } finally {
            failNow.countDown();
            cleanupAllowed.countDown();
        }
        loading.get(10, TimeUnit.SECONDS);
        assertEquals("failed", bootstrap.status().state());
        assertEquals(1, bootstrap.status().completedUnits());
        assertEquals(1, bootstrap.status().rowsLoaded());
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM forecast_cache", Integer.class));
        assertLoadLocksReleased();
        assertThrows(SourceUnavailableException.class, repository::findIssues);

        retry.set(true);
        var restarted = new ForecastCacheRepository(dataSource, jdbc, source, 6);
        var lease = restarted.claim();
        assertNotNull(lease);
        restarted.load(lease);
        assertEquals("ready", restarted.status().state());
        assertEquals(6, restarted.status().completedUnits());
        assertEquals(6, restarted.status().rowsLoaded());
        assertEquals(6, repository.findIssues().getFirst().cells());
        verify(source, times(1)).stream(eq(committed), any());
        verify(source, times(2)).stream(eq(failed), any());
        for (var part : cancelled) verify(source, times(2)).stream(eq(part), any());
        verify(source, times(1)).stream(eq(pending), any());
        assertLoadLocksReleased();
    }


    @Test
    void failedFileRollsBackAndInterruptionResumesOnlyCommittedPartitions() throws Exception {
        var first = partition("s3://forecast/a", 2);
        var second = partition("s3://forecast/b", 1);
        var files = new LinkedHashMap<AnnualCacheSource.Partition, List<Object[]>>();
        files.put(first, List.of(row(ISSUE, cells.get(0), 0.123456789012345, true), row(ISSUE, cells.get(1), 0.8, false)));
        files.put(second, List.<Object[]>of(row(ISSUE, cells.get(2), 0.95, true)));
        useSource(controlledSource("snapshot-a", files));
        AtomicBoolean fail = new AtomicBoolean(true);
        doAnswer(invocation -> {
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            if (fail.getAndSet(false)) {
                awaitCommitted("snapshot-a", 1);
                consumer.accept(files.get(second));
                throw new IOException("interrupcao apos um arquivo confirmado e o primeiro lote do proximo");
            }
            consumer.accept(files.get(second));
            return 1L;
        }).when(source).stream(eq(second), any());

        bootstrap.load(bootstrap.claim());
        assertEquals("failed", bootstrap.status().state());
        assertEquals(1, bootstrap.status().completedUnits());
        assertEquals(2, bootstrap.status().rowsLoaded());
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM forecast_cache", Integer.class));
        assertThrows(SourceUnavailableException.class, repository::findIssues);

        // Um novo processo tambem pode morrer depois de reivindicar: GET detecta a perda do descritor.
        var interrupted = bootstrap.claim();
        assertNotNull(interrupted);
        interrupted.close();
        assertEquals("failed", bootstrap.status().state());
        var restarted = new ForecastCacheRepository(dataSource, jdbc, source, 6);
        restarted.load(restarted.claim());
        assertEquals("ready", restarted.status().state());
        assertEquals(2, restarted.status().completedUnits());
        assertEquals(3, restarted.status().rowsLoaded());
        assertEquals(3, repository.findIssues().getFirst().cells());
        verify(source, times(1)).stream(eq(first), any());
        verify(source, times(2)).stream(eq(second), any());
        assertNull(restarted.claim(), "cache recente nao inicia nova exportacao");
    }

    @Test
    void stagedRefreshNeverReplacesPublishedDataUntilWholePlanIsValidated() throws Exception {
        var old = partition("s3://old/file", 1);
        useSource(controlledSource("old", Map.of(old, List.<Object[]>of(row(ISSUE, cells.get(0), 0.2, true)))));
        bootstrap.load(bootstrap.claim());
        expire();
        var next = partition("s3://new/file", 1);
        doAnswer(invocation -> {
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.<Object[]>of(row(ISSUE, cells.get(0), 0.99999999999999, false)));
            return 1L;
        }).when(source).stream(eq(next), any());
        AtomicBoolean drift = new AtomicBoolean(true);
        AtomicReference<Double> visibleWhilePlanning = new AtomicReference<>();
        doAnswer(invocation -> {
            AnnualCacheExports.Listener listener = invocation.getArgument(1);
            listener.snapshot("new");
            listener.exported(List.of(next));
            awaitCommitted("new", 1);
            visibleWhilePlanning.set(repository.read(snapshot -> snapshot.findForecast(cover(), ISSUE, 0, false, 10)).getFirst().probability7d());
            if (drift.getAndSet(false)) throw new IOException("versao Delta mudou depois do UNLOAD");
            return new ForecastCacheSource.Plan(2, 2, "new", List.of(next));
        }).when(source).plan(any(), any());

        bootstrap.load(bootstrap.claim());
        assertEquals(0.2, visibleWhilePlanning.get());
        assertEquals("failed", bootstrap.status().state());
        assertEquals(0.2, repository.read(snapshot -> snapshot.findForecast(cover(), ISSUE, 0, false, 10)).getFirst().probability7d());
        assertEquals("old", jdbc.queryForObject("SELECT active_snapshot FROM forecast_cache_bootstrap", String.class));
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM forecast_cache", Integer.class));

        bootstrap.load(bootstrap.claim());
        assertEquals("ready", bootstrap.status().state());
        assertEquals(0.99999999999999, repository.read(snapshot -> snapshot.findForecast(cover(), ISSUE, 0, false, 10)).getFirst().probability7d());
        assertEquals("new", jdbc.queryForObject("SELECT active_snapshot FROM forecast_cache_bootstrap", String.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM forecast_cache", Integer.class));
        verify(source, times(1)).stream(eq(next), any());
    }

    @Test
    void initialCopyOverlapsExportsButNothingIsVisibleBeforePlanReturns() throws Exception {
        var part = partition("s3://pipeline/file", 1);
        useSource(controlledSource("pipeline", Map.of(part, List.<Object[]>of(row(ISSUE, cells.getFirst(), 0.7, true)))));
        AtomicBoolean unavailableDuringPlan = new AtomicBoolean();
        doAnswer(invocation -> {
            AnnualCacheExports.Listener listener = invocation.getArgument(1);
            listener.snapshot("pipeline");
            listener.exported(List.of(part));
            awaitCommitted("pipeline", 1);
            try { repository.findIssues(); }
            catch (SourceUnavailableException expected) { unavailableDuringPlan.set(true); }
            return new ForecastCacheSource.Plan(1, 1, "pipeline", List.of(part));
        }).when(source).plan(any(), any());
        bootstrap.load(bootstrap.claim());
        assertTrue(unavailableDuringPlan.get());
        assertEquals("ready", bootstrap.status().state());
        assertEquals(1, bootstrap.status().finalizationCompleted());
        assertEquals(1, repository.findIssues().getFirst().cells());
    }

    @Test
    void binaryCopyPreservesEveryMetricNullDateBooleanTextAndLakeCoordinate() throws Exception {
        var part = partition("s3://typed/file", 2);
        Object[] precise = row(ISSUE, cells.get(0), 0.12345678901234566, true);
        precise[14] = "Costa do \"Marfim\"";
        precise[15] = "Cidade d'Agua\nLinha 2";
        Object[] nullable = row(ISSUE.minusYears(1), cells.get(1), 0.01, null);
        nullable[2] = null;
        for (int i = 4; i <= 12; i++) nullable[i] = null;
        for (int i = 14; i < nullable.length; i++) nullable[i] = null;
        useSource(controlledSource("typed", Map.of(part, List.of(precise, nullable))));
        bootstrap.load(bootstrap.claim());
        assertEquals("ready", bootstrap.status().state());
        var preserved = repository.read(snapshot -> snapshot.findForecast(cover(), ISSUE, 0, false, 10)).getFirst();
        assertEquals(Double.doubleToLongBits((Double) precise[3]), Double.doubleToLongBits(preserved.probability7d()));
        assertEquals(1.0, preserved.detections24h());
        assertEquals(2.0, preserved.detections3d());
        assertEquals(3.0, preserved.detections7d());
        assertEquals(4.5, preserved.frpMean7d());
        assertEquals(300.25, preserved.modisBrightnessMean7d());
        assertEquals(301.5, preserved.viirsBrightnessMean7d());
        assertEquals(5.0, preserved.ring1Detections24h());
        assertEquals(6.0, preserved.ring1Detections7d());
        assertEquals(7.0, preserved.ring2Detections7d());
        assertEquals("Costa do \"Marfim\"", preserved.countryName());
        assertEquals("Cidade d'Agua\nLinha 2", preserved.cityName());
        assertEquals(4.25, preserved.cityDistanceKm());
        var geometry = jdbc.queryForMap("SELECT latitude,longitude,ST_X(geom) AS x,ST_Y(geom) AS y FROM forecast_cache WHERE issue_date=?", ISSUE);
        assertEquals(-3.0, geometry.get("latitude"));
        assertEquals(-54.0, geometry.get("longitude"));
        assertEquals(-54.0, geometry.get("x"));
        assertEquals(-3.0, geometry.get("y"));
        assertNull(jdbc.queryForObject("SELECT geom FROM forecast_cache WHERE issue_date=?", Object.class, ISSUE.minusYears(1)));
        var absent = repository.read(snapshot -> snapshot.findForecast(cover(), ISSUE.minusYears(1), 0, false, 10)).getFirst();
        assertNull(absent.h3Region());
        assertNull(absent.detections24h());
        assertNull(absent.frpMean7d());
        assertNull(absent.active());
        assertNull(absent.countryName());
        assertNull(absent.cityDistanceKm());
        assertEquals(0, repository.read(snapshot -> snapshot.findForecast(cover(), ISSUE.minusYears(1), 0, true, 10)).size());
    }

    @Test
    void issuesListsSummaryAndTruncationKeepNativeProbabilitiesAndAllIssueYears() throws Exception {
        var part = partition("s3://issues/file", 5);
        var rows = List.of(row(ISSUE, cells.get(0), 0.9, false), row(ISSUE, cells.get(1), 0.9, true),
                row(ISSUE, cells.get(2), 0.4, true), row(ISSUE.minusYears(1), cells.get(0), 0.1, true),
                row(ISSUE.minusYears(2), cells.get(0), 0.8, false));
        useSource(controlledSource("issues", Map.of(part, rows)));
        bootstrap.load(bootstrap.claim());
        assertEquals(List.of(ISSUE, ISSUE.minusYears(1), ISSUE.minusYears(2)),
                repository.findIssues().stream().map(issue -> issue.issueDate()).toList());
        assertEquals(3, repository.findIssues().getFirst().cells());
        assertEquals(2, repository.findIssues().getFirst().activeCells());
        var limited = repository.read(snapshot -> snapshot.findForecast(cover(), ISSUE, 0.5, false, 1));
        assertEquals(2, limited.size(), "limit+1 necessario para detectar truncamento");
        assertEquals(List.of(cells.get(0), cells.get(1)), limited.stream().map(value -> value.h3()).toList());
        assertEquals(List.of(cells.get(1)), repository.read(snapshot -> snapshot.findForecast(cover(), ISSUE, 0.5, true, 10)).stream()
                .map(value -> value.h3()).toList());
        var summary = repository.read(snapshot -> snapshot.findSummary(cover(), ISSUE));
        assertEquals(3, summary.cells());
        assertEquals(2, summary.activeCells());
        assertEquals(0.9, summary.maxProbability());
        assertEquals((0.9 + 0.9 + 0.4) / 3, summary.meanProbability(), 1e-15);
        for (int resolution = 0; resolution <= 6; resolution++) {
            var parent = new Region.Cover(resolution, List.of(H3Index.parentBits(cells.get(0), resolution)));
            assertEquals(resolution == 6 ? 1 : 3, repository.read(snapshot -> snapshot.findSummary(parent, ISSUE)).cells(), "parent H3 r" + resolution);
        }
        var service = new ForecastService(repository, new ForecastMapper());
        Region region = Region.of(null, H3Index.toHex(H3Index.parentBits(cells.getFirst(), 5)));
        var response = service.forecast(region, null, 0, false, 1);
        assertEquals(ISSUE, response.issueDate());
        assertEquals(ISSUE, response.latestIssueDate());
        assertTrue(response.truncated());
        assertEquals(1, response.count());
        assertEquals(0.9, response.items().getFirst().probability7d());
        assertEquals(3, response.summary().cells());
        assertEquals(0, repository.read(snapshot -> snapshot.findSummary(cover(), ISSUE.minusDays(1))).cells());
        assertNull(repository.read(snapshot -> snapshot.findSummary(cover(), ISSUE.minusDays(1))).maxProbability());
    }

    @Test
    void placeFiltersUseSharedParentsOnlyBelowSixAndNativeMetadataAtSix() throws Exception {
        var part = partition("s3://places/file", 3);
        Object[] brazil = row(ISSUE, cells.get(0), 0.21, true);
        Object[] foreign = row(ISSUE, cells.get(1), 0.78, false);
        foreign[14] = "Peru";
        foreign[19] = "South America";
        foreign[20] = "PER";
        foreign[21] = "PE";
        foreign[22] = 3936456L;
        Object[] distantCity = row(ISSUE, cells.get(2), 0.57, true);
        distantCity[16] = 100.0;
        useSource(controlledSource("places", Map.of(part, List.of(brazil, foreign, distantCity))));
        bootstrap.load(bootstrap.claim());
        Long partitionId = jdbc.queryForObject("INSERT INTO annual_grid_partition"
                + " (year,source_key,kind,resolution,file_size,completed) VALUES (2026,'places-parent','PLACES',5,1,true) RETURNING id", Long.class);
        jdbc.update("INSERT INTO annual_h3_places (resolution,h3_cell,country_iso_a2,country_code,continent,city_geonameid,city_distance_km,partition_id)"
                        + " VALUES (5,?,'BR','BRA','South America',3389353,4.25,?)",
                H3Index.parentBits(cells.get(0), 5), partitionId);
        var country = new Region.Place(Region.Kind.COUNTRY, "BR", 0);
        var iso3 = new Region.Place(Region.Kind.COUNTRY, "BRA", 0);
        var continent = new Region.Place(Region.Kind.CONTINENT, "South America", 0);
        var city = new Region.Place(Region.Kind.CITY, "3389353", 25);
        assertEquals(3, repository.read(snapshot -> snapshot.findSummary(new Region.Cover(5, List.of(), country), ISSUE)).cells());
        assertEquals(2, repository.read(snapshot -> snapshot.findSummary(new Region.Cover(6, List.of(), country), ISSUE)).cells());
        assertEquals(2, repository.read(snapshot -> snapshot.findSummary(new Region.Cover(6, List.of(), iso3), ISSUE)).cells());
        assertEquals(3, repository.read(snapshot -> snapshot.findSummary(new Region.Cover(6, List.of(), continent), ISSUE)).cells());
        assertEquals(3, repository.read(snapshot -> snapshot.findSummary(new Region.Cover(5, List.of(), city), ISSUE)).cells());
        var nativeCity = repository.read(snapshot -> snapshot.findForecast(new Region.Cover(6, List.of(), city), ISSUE, 0, false, 10));
        assertEquals(1, nativeCity.size());
        assertEquals(cells.get(0), nativeCity.getFirst().h3());
        assertEquals(0.21, nativeCity.getFirst().probability7d(), "nao somar/mediar probabilidades no ancestral");
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_h3_places WHERE resolution=6", Integer.class));
    }

    @Test
    void emptySourcePublishesExplicitPartitionButAnEmptyPlanNeverDoes() throws Exception {
        var empty = partition("empty://forecast", 0);
        useSource(controlledSource("empty", Map.of(empty, List.of())));
        bootstrap.load(bootstrap.claim());
        assertEquals("ready", bootstrap.status().state());
        assertEquals(1, bootstrap.status().completedUnits());
        assertEquals(0, bootstrap.status().rowsLoaded());
        assertTrue(repository.findIssues().isEmpty());
        expire();
        doAnswer(invocation -> new ForecastCacheSource.Plan(2, 2, "invalid-empty", List.of()))
                .when(source).plan(any(), any());
        bootstrap.load(bootstrap.claim());
        assertEquals("failed", bootstrap.status().state());
        assertEquals("empty", jdbc.queryForObject("SELECT active_snapshot FROM forecast_cache_bootstrap", String.class));
        assertTrue(repository.findIssues().isEmpty(), "publicacao vazia valida anterior continua consultavel");
    }

    @Test
    void immutablePublishedSnapshotCannotAcceptNewFilesBeforeARefreshCompletes() throws Exception {
        var first = partition("s3://immutable/a", 1);
        useSource(controlledSource("immutable", Map.of(first, List.<Object[]>of(row(ISSUE, cells.get(0), 0.3, true)))));
        bootstrap.load(bootstrap.claim());
        expire();
        var unexpected = partition("s3://immutable/b", 1);
        doAnswer(invocation -> {
            AnnualCacheExports.Listener listener = invocation.getArgument(1);
            listener.snapshot("immutable");
            listener.exported(List.of(unexpected));
            return new ForecastCacheSource.Plan(1, 1, "immutable", List.of(first, unexpected));
        }).when(source).plan(any(), any());
        bootstrap.load(bootstrap.claim());
        assertEquals("failed", bootstrap.status().state());
        assertEquals(1, repository.read(snapshot -> snapshot.findSummary(cover(), ISSUE)).cells());
        assertEquals(0.3, repository.read(snapshot -> snapshot.findForecast(cover(), ISSUE, 0, false, 10)).getFirst().probability7d());
        verify(source, never()).stream(eq(unexpected), any());
    }

    @Test
    void oneResponseReadsTheSameSnapshotEvenIfPublicationChangesBetweenItsQueries() throws Exception {
        var old = partition("s3://response/old", 1);
        useSource(controlledSource("response-old", Map.of(old, List.<Object[]>of(row(ISSUE, cells.getFirst(), 0.25, true)))));
        bootstrap.load(bootstrap.claim());
        expire();
        var next = partition("s3://response/new", 1);
        useSource(controlledSource("response-new",
                Map.of(next, List.<Object[]>of(row(ISSUE.plusDays(1), cells.getFirst(), 0.99, false)))));
        List<LocalDate> observed = repository.read(snapshot -> {
            LocalDate before = snapshot.findIssues().getFirst().issueDate();
            bootstrap.load(bootstrap.claim());
            LocalDate after = snapshot.findIssues().getFirst().issueDate();
            assertEquals(0.25, snapshot.findForecast(cover(), ISSUE, 0, false, 10).getFirst().probability7d());
            assertEquals(1, snapshot.findSummary(cover(), ISSUE).cells());
            return List.of(before, after);
        });
        assertEquals(List.of(ISSUE, ISSUE), observed);
        assertEquals("ready", bootstrap.status().state());
        assertEquals(ISSUE.plusDays(1), repository.findIssues().getFirst().issueDate());
        assertEquals(0.99, repository.read(snapshot ->
                snapshot.findForecast(cover(), ISSUE.plusDays(1), 0, false, 10)).getFirst().probability7d());
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM forecast_cache WHERE snapshot_id='response-old'", Integer.class));
    }

    @Test
    void monthlyUnloadAndUnknownFileSizeReachPublicationAndRefreshWithoutExportingAgain() throws Exception {
        var lake = mock(S3LakeStorage.class);
        var athena = mock(AthenaQuery.class);
        var providers = new StaticListableBeanFactory();
        providers.addBean("lake", lake);
        providers.addBean("athena", athena);
        var logs = Map.of(
                "gold/forecast/_delta_log/00000000000000000000.json",
                """
                {"add":{"path":"year=2026/month=11/part.parquet","partitionValues":{"year":"2026","month":"11"},"size":1,"stats":"{\\"numRecords\\":1}"}}
                """.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "gold/h3_places/_delta_log/00000000000000000000.json", new byte[0]);
        when(lake.list(anyString())).thenAnswer(call -> {
            String prefix = call.getArgument(0);
            return logs.keySet().stream().filter(key -> key.startsWith(prefix)).toList();
        });
        when(lake.read(anyString())).thenAnswer(call -> logs.get(call.getArgument(0)));
        when(lake.location("gold/")).thenReturn("s3://lake/gold/");
        when(athena.sourceIdentity()).thenReturn("controlled-source");
        when(athena.workgroupOutputLocation()).thenReturn("s3://results/queries/");
        var destinations = new java.util.concurrent.ConcurrentHashMap<String, String>();
        when(athena.startCommand(anyString(), anyString())).thenAnswer(call -> {
            String sql = call.getArgument(0), token = call.getArgument(1);
            int start = sql.indexOf(") TO '") + 6;
            destinations.put(token, sql.substring(start, sql.indexOf("'", start)));
            return token;
        });
        when(athena.awaitCommand(anyString(), anyLong())).thenAnswer(call ->
                new AthenaQuery.Command(call.getArgument(0), "s3://manifest/" + call.getArgument(0)));
        when(lake.readLocation(anyString())).thenAnswer(call -> {
            String manifest = call.getArgument(0);
            String token = manifest.substring("s3://manifest/".length());
            return (destinations.get(token) + "part.parquet\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        });
        var integrated = spy(new ForecastCacheSource(providers.getBeanProvider(S3LakeStorage.class),
                providers.getBeanProvider(AthenaQuery.class), new AthenaExportStore(jdbc), 2, 300000, ""));
        doAnswer(call -> {
            Consumer<List<Object[]>> consumer = call.getArgument(1);
            consumer.accept(List.<Object[]>of(row(ISSUE, cells.getFirst(), 0.73123456789, true)));
            return 1L;
        }).when(integrated).stream(any(), any());
        useSource(integrated);
        bootstrap.load(bootstrap.claim());
        assertEquals("ready", bootstrap.status().state());
        assertEquals(0.73123456789, repository.read(snapshot ->
                snapshot.findForecast(cover(), ISSUE, 0, false, 10)).getFirst().probability7d());
        expire();
        bootstrap.load(bootstrap.claim());
        assertEquals("ready", bootstrap.status().state());
        assertEquals(1, bootstrap.status().rowsLoaded());
        verify(athena, times(1)).startCommand(anyString(), anyString());
        verify(integrated, times(1)).stream(any(), any());
    }
}
