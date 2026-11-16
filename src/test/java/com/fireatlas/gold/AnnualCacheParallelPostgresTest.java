package com.fireatlas.gold;

import com.fireatlas.gold.bootstrap.repository.BootstrapRepository;
import com.fireatlas.gold.bootstrap.service.BootstrapService;
import com.fireatlas.gold.common.policy.SourceUnavailableException;
import com.fireatlas.lake.integration.AnnualCacheSource;
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
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
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
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(DbAvailable.class)
@Timeout(30)
class AnnualCacheParallelPostgresTest {
    private DataSource dataSource;
    private JdbcTemplate jdbc;
    private JdbcTemplate admin;
    private String schema;
    private AnnualCacheSource source;
    private BootstrapRepository bootstrap;
    private ExecutorService coordinator;
    private final Set<Integer> workerBackends = ConcurrentHashMap.newKeySet();

    @BeforeEach
    void createSchema() throws Exception {
        admin = new JdbcTemplate(new DriverManagerDataSource(DbAvailable.URL, DbAvailable.USER, DbAvailable.PASSWORD));
        schema = "annual_parallel_test_" + UUID.randomUUID().toString().replace("-", "");
        admin.execute("CREATE SCHEMA " + schema);
        var driver = new DriverManagerDataSource(DbAvailable.URL, DbAvailable.USER, DbAvailable.PASSWORD);
        Properties properties = new Properties();
        properties.setProperty("currentSchema", schema + ",public");
        driver.setConnectionProperties(properties);
        dataSource = new DelegatingDataSource(driver) {
            @Override public Connection getConnection() throws SQLException {
                Connection connection = super.getConnection();
                if (Thread.currentThread().getName().startsWith("annual-cache-import-")) {
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
        new ResourceDatabasePopulator(new ClassPathResource("db/migration/V14__annual_grid_cache.sql"),
                new ClassPathResource("db/migration/V15__annual_cache_postgis.sql"),
                new ClassPathResource("db/migration/V16__annual_athena_exports.sql"),
                new ClassPathResource("db/migration/V17__annual_athena_monthly_exports.sql"),
                new ClassPathResource("db/migration/V18__annual_cache_direct_copy.sql"),
                new ClassPathResource("db/migration/V19__annual_athena_export_parts.sql"),
                new ClassPathResource("db/migration/V20__annual_cache_unlogged.sql"),
                new ClassPathResource("db/migration/V21__annual_cache_deferred_indexes.sql"),
                new ClassPathResource("db/migration/V22__annual_cache_finalization_progress.sql"),
                new ClassPathResource("db/migration/V23__annual_cache_historical_years.sql")).execute(dataSource);
        source = mock(AnnualCacheSource.class);
        doAnswer(invocation -> source.plan(invocation.getArgument(0), invocation.getArgument(1)))
                .when(source).plan(anyInt(), anyBoolean(), any(), any());
        bootstrap = repository();
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

    @Test
    void distinctPartitionsUseTwoBackendConnectionsAndReadyWaitsForBothCommits() throws Exception {
        var first = partition("grid-a");
        var second = partition("grid-b");
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(first, second)));
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        doAnswer(invocation -> {
            var partition = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            active.incrementAndGet();
            try {
                consumer.accept(List.<Object[]>of(row(partition.key())));
                entered.countDown();
                await(finish);
                return 1L;
            } finally {
                active.decrementAndGet();
            }
        }).when(source).stream(any(), any());

        Future<?> loading = loadAsync();
        try {
            await(entered);
            assertEquals(2, active.get());
            assertEquals(2, workerBackends.size(), "cada importador deve possuir uma sessao PostgreSQL distinta");
            assertEquals("loading", bootstrap.status(2026).state());
            assertEquals(0, bootstrap.status(2026).completedUnits());
            // Linhas ja enviadas pelo COPY ficam invisiveis ate o commit do arquivo.
            assertEquals(0, count("annual_map_grid"));
            assertNull(repository().claim(2026));
            assertFalse(loading.isDone());
        } finally {
            finish.countDown();
        }
        loading.get(10, TimeUnit.SECONDS);
        assertEquals("ready", bootstrap.status(2026).state());
        assertEquals(2, bootstrap.status(2026).completedUnits());
        assertEquals(2, bootstrap.status(2026).rowsLoaded());
        assertEquals(2, count("annual_grid_partition"));
        assertEquals(2, count("annual_map_grid"));
        verify(source, times(1)).plan(eq(2026), eq(true), any(), any());
        verify(source, times(1)).stream(eq(first), any());
        verify(source, times(1)).stream(eq(second), any());
    }

    @Test
    void failedWorkerRetainsCommittedFilesAndLockUntilOtherWriterHasActuallyExited() throws Exception {
        var committed = partition("grid-a");
        var failed = partition("grid-b");
        var cancelled = partition("grid-c");
        var pending = partition("grid-d");
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(committed, failed, cancelled, pending)));
        CountDownLatch slowEntered = new CountDownLatch(1);
        CountDownLatch failNow = new CountDownLatch(1);
        CountDownLatch slowCancelled = new CountDownLatch(1);
        CountDownLatch cleanupAllowed = new CountDownLatch(1);
        AtomicBoolean retry = new AtomicBoolean();
        doAnswer(invocation -> {
            var partition = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.<Object[]>of(row(partition.key())));
            if (retry.get() || partition.equals(committed) || partition.equals(pending)) return 1L;
            if (partition.equals(failed)) {
                await(failNow);
                throw new IOException("s3://private-bucket/secret-export simulated failure");
            }
            slowEntered.countDown();
            try {
                await(new CountDownLatch(1));
                throw new IOException("cancelamento esperado");
            } catch (InterruptedException e) {
                slowCancelled.countDown();
                awaitIgnoringInterrupts(cleanupAllowed);
                Thread.currentThread().interrupt();
                throw new IOException("worker cancelado", e);
            }
        }).when(source).stream(any(), any());

        Future<?> loading = loadAsync();
        try {
            // O terceiro arquivo so comeca depois de o primeiro ter publicado e fechado sua conexao.
            await(slowEntered);
            assertEquals(1, bootstrap.status(2026).completedUnits());
            failNow.countDown();
            await(slowCancelled);
            assertFalse(loading.isDone(), "Future cancelado nao significa writer encerrado");
            assertNull(repository().claim(2026), "lock deve permanecer com coordenador durante cleanup do writer");
            assertEquals("loading", bootstrap.status(2026).state());
            assertEquals(1, count("annual_map_grid"));
            verify(source, never()).stream(eq(pending), any());
        } finally {
            failNow.countDown();
            cleanupAllowed.countDown();
        }
        loading.get(10, TimeUnit.SECONDS);
        assertEquals("failed", bootstrap.status(2026).state());
        assertEquals(1, bootstrap.status(2026).completedUnits());
        assertFalse(bootstrap.status(2026).error().contains("secret-export"));
        assertEquals(1, count("annual_map_grid"));

        retry.set(true);
        BootstrapRepository restarted = repository();
        var lease = restarted.claim(2026);
        assertNotNull(lease);
        restarted.load(lease);
        assertEquals("ready", restarted.status(2026).state());
        assertEquals(4, restarted.status(2026).completedUnits());
        assertEquals(4, restarted.status(2026).rowsLoaded());
        assertEquals(4, count("annual_grid_partition"));
        assertEquals(4, count("annual_map_grid"));
        verify(source, times(1)).stream(eq(committed), any());
        verify(source, times(2)).stream(eq(failed), any());
        verify(source, times(2)).stream(eq(cancelled), any());
        verify(source, times(1)).stream(eq(pending), any());
    }

    @Test
    void interruptedServiceShutdownJoinsEveryWriterBeforeReturningAndReleasingLock() throws Exception {
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1,
                List.of(partition("grid-a"), partition("grid-b"))));
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch cancelled = new CountDownLatch(2);
        CountDownLatch cleanupAllowed = new CountDownLatch(1);
        AtomicBoolean retry = new AtomicBoolean();
        doAnswer(invocation -> {
            var partition = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.<Object[]>of(row(partition.key())));
            if (retry.get()) return 1L;
            entered.countDown();
            try {
                await(new CountDownLatch(1));
                throw new IOException("cancelamento esperado");
            } catch (InterruptedException e) {
                cancelled.countDown();
                awaitIgnoringInterrupts(cleanupAllowed);
                Thread.currentThread().interrupt();
                throw new IOException("shutdown", e);
            }
        }).when(source).stream(any(), any());
        var service = new BootstrapService(bootstrap, mock(com.fireatlas.gold.bootstrap.service.ForecastCacheService.class));
        AtomicBoolean restoredInterrupt = new AtomicBoolean();
        Thread closing = new Thread(() -> {
            Thread.currentThread().interrupt();
            service.close();
            restoredInterrupt.set(Thread.currentThread().isInterrupted());
        }, "annual-cache-test-close");
        try {
            service.start();
            await(entered);
            closing.start();
            await(cancelled);
            assertTrue(closing.isAlive());
            assertNull(repository().claim(2026));
            assertEquals("loading", bootstrap.status(2026).state());
            assertEquals(0, bootstrap.status(2026).completedUnits());
        } finally {
            cleanupAllowed.countDown();
            if (closing.getState() != Thread.State.NEW) closing.join(10_000);
            service.close();
        }
        assertFalse(closing.isAlive());
        assertTrue(restoredInterrupt.get());
        assertEquals("failed", bootstrap.status(2026).state());
        assertEquals(0, count("annual_map_grid"));
        assertThrows(SourceUnavailableException.class, service::start);
        retry.set(true);
        var lease = bootstrap.claim(2026);
        assertNotNull(lease);
        bootstrap.load(lease);
        assertEquals("ready", bootstrap.status(2026).state());
        assertEquals(2, bootstrap.status(2026).completedUnits());
    }

    private BootstrapRepository repository() {
        return new BootstrapRepository(dataSource, jdbc, source, 6, 2);
    }

    private Future<?> loadAsync() {
        var lease = bootstrap.claim(2026);
        assertNotNull(lease);
        return coordinator.submit(() -> bootstrap.load(lease));
    }

    private int count(String table) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class);
    }

    private static AnnualCacheSource.Partition partition(String key) {
        return new AnnualCacheSource.Partition(key, "GRID", 5, 100, 1L, false);
    }

    private static Object[] row(String key) {
        long cell = switch (key) {
            case "grid-a" -> 10L;
            case "grid-b" -> 20L;
            case "grid-c" -> 30L;
            default -> 40L;
        };
        return new Object[]{Date.valueOf("2026-01-01"), "VIIRS", cell, 1L, 2.0, 3.0, 0L, 1L, 0L, null};
    }

    private static void await(CountDownLatch latch) throws InterruptedException, IOException {
        if (!latch.await(8, TimeUnit.SECONDS)) throw new IOException("timeout aguardando sincronizacao do teste");
    }

    private static void awaitIgnoringInterrupts(CountDownLatch latch) throws IOException {
        boolean interrupted = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
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
}
