package com.fireatlas.lake.integration;

import com.fireatlas.gold.common.policy.SourceUnavailableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AnnualCacheExportsTest {
    private AthenaQuery query;
    private S3LakeStorage lake;
    private MemoryStore store;

    @BeforeEach
    void setUp() {
        query = mock(AthenaQuery.class);
        lake = mock(S3LakeStorage.class);
        store = new MemoryStore();
        when(query.sourceIdentity()).thenReturn("fireatlas_gold/fireatlas-api");
        when(query.workgroupOutputLocation()).thenReturn("s3://results/athena-results/");
        when(lake.location("gold/")).thenReturn("s3://lake/gold/");
        when(query.startCommand(anyString(), anyString())).thenAnswer(invocation -> invocation.getArgument(1));
        when(query.awaitCommand(anyString(), anyLong())).thenAnswer(invocation -> command(invocation.getArgument(0)));
        when(lake.readLocation(anyString())).thenAnswer(invocation -> manifestFor(invocation.getArgument(0)));
    }

    @Test
    void unloadProjectsExactlyConsumedColumnsBeforeTransferAndUsesExactPartitions() {
        String grid = AnnualCacheExports.unloadSql(2026, AnnualCacheExports.Request.grid(5, 3, null), "s3://results/output/");
        assertTrue(grid.startsWith("UNLOAD (SELECT \"day\", \"sensor\", \"h3_cell\", \"detections\""));
        assertTrue(grid.contains("FROM map_grid WHERE resolution = 5 AND year = 2026"
                + " AND day >= DATE '2026-03-01' AND day < DATE '2026-04-01')"));
        assertFalse(grid.contains("month"));
        assertTrue(AnnualCacheExports.unloadSql(2026, AnnualCacheExports.Request.grid(5, 12, null), "s3://results/output/")
                .contains("AND day >= DATE '2026-12-01' AND day < DATE '2027-01-01')"));
        assertTrue(grid.endsWith("WITH (format='PARQUET', compression='ZSTD')"));
        assertFalse(grid.contains("SELECT *"));
        assertFalse(grid.contains("platforms"));
        assertFalse(grid.contains("frp_mean_mw"));
        assertFalse(grid.contains("brightness_max_k"));
        for (String column : AnnualCacheSource.GRID_COLUMNS) assertTrue(grid.contains("\"" + column + "\""));
        String places = AnnualCacheExports.unloadSql(2026, AnnualCacheExports.Request.places(5, null), "s3://results/output/");
        assertTrue(places.contains("FROM h3_places WHERE resolution = 5)"));
        assertFalse(places.contains("year ="));
        assertFalse(places.contains("month ="));
        assertTrue(places.contains("\"latitude\", \"longitude\""));
        assertFalse(places.contains("SELECT *"));
        assertThrows(IllegalArgumentException.class, () -> AnnualCacheExports.Request.grid(5, 0, null));
        assertThrows(IllegalArgumentException.class, () -> AnnualCacheExports.Request.grid(5, 13, null));
        assertThrows(IllegalArgumentException.class, () -> new AnnualCacheExports.Request("PLACES", 5, 1, 0, 0, null));
        assertThrows(IllegalArgumentException.class, () -> AnnualCacheExports.Request.placesPart(5, 9, 8));
        assertThrows(IllegalArgumentException.class, () -> new AnnualCacheExports.Request("GRID", 5, 1, 1, 2, null));
    }

    @Test
    void largePlaceResolutionIsExportedAsDisjointHashPartsWithSeparateKeysAndDestinations() throws Exception {
        String part = AnnualCacheExports.unloadSql(2026, AnnualCacheExports.Request.placesPart(5, 3, 8), "s3://results/output/");
        assertTrue(part.contains("FROM h3_places WHERE resolution = 5 AND mod(bitwise_and(from_big_endian_64("
                + "xxhash64(to_big_endian_64(h3_cell))), 9223372036854775807), 8) = 2)"));
        List<AnnualCacheExports.Request> requests = new ArrayList<>();
        for (int p = 1; p <= 8; p++) requests.add(AnnualCacheExports.Request.placesPart(5, p, 8));
        var result = coordinator().export(2026, 1, 1, requests, message -> {});
        assertEquals(8, result.size());
        assertEquals(8, store.rows.size());
        for (var export : store.rows.values()) {
            assertTrue(export.prefix().contains("/places/r=5/p=" + export.key().part() + "-8/"));
        }
        // Linhas por parte sao desconhecidas: nenhuma verificacao de contagem falsa no streaming.
        assertTrue(result.stream().allMatch(p -> p.expectedRows() == null));
    }

    @Test
    void twelveMonthsOfAResolutionRunInParallelEachWithItsOwnUnloadAndDestination() throws Exception {
        var entered = new CountDownLatch(12);
        var active = new AtomicInteger();
        var maximum = new AtomicInteger();
        when(query.awaitCommand(anyString(), anyLong())).thenAnswer(invocation -> {
            maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
            try {
                entered.countDown();
                assertTrue(entered.await(10, TimeUnit.SECONDS), "os 12 meses devem estar em execucao ao mesmo tempo");
                return command(invocation.getArgument(0));
            } finally {
                active.decrementAndGet();
            }
        });
        List<AnnualCacheExports.Request> requests = new ArrayList<>();
        for (int month = 1; month <= 12; month++) requests.add(AnnualCacheExports.Request.grid(4, month, 1L));
        var result = new AnnualCacheExports(provider(query), provider(lake), store, 12, 300000, "")
                .export(2026, 1, 1, requests, message -> {});
        assertEquals(12, maximum.get());
        assertEquals(12, result.size());
        assertEquals(12, store.rows.size());
        assertEquals(12, store.rows.values().stream().map(AthenaExportStore.Export::prefix).distinct().count());
        for (var export : store.rows.values()) {
            assertTrue(export.prefix().contains("/grid/r=4/m=" + export.key().month() + "/"));
            verify(query).startCommand(AnnualCacheExports.unloadSql(2026, 
                    AnnualCacheExports.Request.grid(4, export.key().month(), 1L), export.prefix()), export.token());
        }
        verify(query, times(12)).startCommand(anyString(), anyString());
    }

    @Test
    void completedManifestAndObjectKeysSurviveCoordinatorRestartAndChangedSnapshotDoesNotReuseThem() throws Exception {
        var requests = List.of(AnnualCacheExports.Request.grid(5, 1, 2L));
        var first = coordinator().export(2026, 5, 7, requests, message -> {});
        var restarted = coordinator().export(2026, 5, 7, requests, message -> {});
        assertEquals(first, restarted);
        assertTrue(first.getFirst().key().startsWith("s3://results/athena-results/bootstrap/2026/"));
        assertEquals(2L, first.getFirst().expectedRows());
        assertEquals(-1L, first.getFirst().size());
        verify(query, times(1)).startCommand(anyString(), anyString());
        verify(lake, times(1)).readLocation(anyString());
        var refreshed = coordinator().export(2026, 6, 7, requests, message -> {});
        assertNotEquals(first.getFirst().key(), refreshed.getFirst().key());
        verify(query, times(2)).startCommand(anyString(), anyString());
        verify(query, never()).run(anyString());
        verify(lake, never()).download(anyString());
    }

    @Test
    void failedManifestReadResumesSameSuccessfulQueryRatherThanReexporting() throws Exception {
        var fail = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (fail.getAndSet(false)) throw new IllegalStateException("s3:GetObject temporariamente negado");
            return manifestFor(invocation.getArgument(0));
        }).when(lake).readLocation(anyString());
        var requests = List.of(AnnualCacheExports.Request.grid(4, 1, 1L));
        assertThrows(IllegalStateException.class, () -> coordinator().export(2026, 2, 3, requests, message -> {}));
        var interrupted = store.only();
        assertEquals("running", interrupted.state());
        assertNotNull(interrupted.queryId());
        var resumed = coordinator().export(2026, 2, 3, requests, message -> {});
        assertTrue(resumed.getFirst().key().startsWith(interrupted.prefix()));
        verify(query, times(1)).startCommand(anyString(), anyString());
        verify(query, times(2)).awaitCommand(interrupted.queryId(), 300000);
        assertEquals("complete", store.only().state());
    }

    @Test
    void crashAfterSubmissionBeforeSavingQueryIdResubmitsSameSqlAndIdempotencyToken() throws Exception {
        store = spy(store);
        doThrow(new IllegalStateException("queda antes de salvar query_id")).doCallRealMethod()
                .when(store).started(any(), anyString());
        var requests = List.of(AnnualCacheExports.Request.grid(3, 1, 1L));
        assertThrows(IllegalStateException.class, () -> coordinator().export(2026, 1, 1, requests, message -> {}));
        var pending = store.only();
        assertEquals("pending", pending.state());
        assertNull(pending.queryId());
        var resumed = coordinator().export(2026, 1, 1, requests, message -> {});
        assertTrue(resumed.getFirst().key().startsWith(pending.prefix()));
        verify(query, times(2)).startCommand(
                AnnualCacheExports.unloadSql(2026, requests.getFirst(), pending.prefix()), pending.token());
        verify(query, times(1)).awaitCommand(pending.token(), 300000);
    }

    @Test
    void terminalQueryFailureCreatesNewEmptyAttemptPrefixButDoesNotDeleteS3Objects() throws Exception {
        when(query.awaitCommand(anyString(), anyLong()))
                .thenThrow(new AthenaQuery.QueryFailedException("Athena FAILED: s3:PutObject denied"))
                .thenAnswer(invocation -> command(invocation.getArgument(0)));
        var requests = List.of(AnnualCacheExports.Request.places(5, 1L));
        assertThrows(AthenaQuery.QueryFailedException.class, () -> coordinator().export(2026, 1, 1, requests, message -> {}));
        String oldPrefix = store.only().prefix();
        assertEquals("failed", store.only().state());
        var retry = coordinator().export(2026, 1, 1, requests, message -> {});
        assertNotEquals(oldPrefix, store.only().prefix());
        assertFalse(retry.getFirst().key().startsWith(oldPrefix));
        verify(query, times(2)).startCommand(anyString(), anyString());
    }

    @Test
    void emptyManifestIsTruthfulAndCannotHideRowsDeclaredByDelta() throws Exception {
        doReturn(new byte[0]).when(lake).readLocation(anyString());
        var unknown = coordinator().export(2026, 1, 1,
                List.of(AnnualCacheExports.Request.places(0, null)), message -> {});
        assertTrue(unknown.getFirst().empty());
        assertEquals(0L, unknown.getFirst().expectedRows());
        assertEquals("complete", store.only().state());
        assertThrows(IOException.class, () -> coordinator().export(2026, 1, 1,
                List.of(AnnualCacheExports.Request.grid(5, 1, 10L)), message -> {}));
        assertEquals(1, store.rows.values().stream().filter(export -> export.state().equals("complete")).count());
    }

    @Test
    void emptyActiveDeltaPartitionNeedsNoQueryAndIsHandedToTheListenerAfterTheSnapshot() throws Exception {
        List<String> messages = new ArrayList<>();
        List<String> events = new ArrayList<>();
        var result = coordinator().export(2026, 1, 1, List.of(AnnualCacheExports.Request.grid(0, 1, 0L)), messages::add,
                new AnnualCacheExports.Listener() {
                    @Override public void snapshot(String id) { events.add("snapshot:" + id); }
                    @Override public void exported(List<AnnualCacheSource.Partition> partitions) {
                        partitions.forEach(p -> events.add("exported:" + p.key()));
                    }
                });
        assertTrue(result.getFirst().empty());
        assertTrue(messages.getLast().contains("1/1"));
        // A identidade chega antes de qualquer particao, e as chaves deste snapshot a contem.
        assertEquals(2, events.size());
        String snapshot = events.getFirst().substring("snapshot:".length());
        assertEquals("exported:" + result.getFirst().key(), events.getLast());
        assertTrue(result.getFirst().key().contains(snapshot));
        verify(query, never()).startCommand(anyString(), anyString());
        assertTrue(store.rows.isEmpty());
    }

    @Test
    void exportsAreActuallyConcurrentButBoundedToTwo() throws Exception {
        var entered = new CountDownLatch(2);
        var active = new AtomicInteger();
        var maximum = new AtomicInteger();
        when(query.awaitCommand(anyString(), anyLong())).thenAnswer(invocation -> {
            int running = active.incrementAndGet();
            maximum.accumulateAndGet(running, Math::max);
            try {
                entered.countDown();
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                return command(invocation.getArgument(0));
            } finally {
                active.decrementAndGet();
            }
        });
        var requests = List.of(AnnualCacheExports.Request.grid(0, 1, 1L),
                AnnualCacheExports.Request.grid(1, 1, 1L), AnnualCacheExports.Request.places(5, 1L));
        var result = coordinator().export(2026, 1, 1, requests, message -> {});
        assertEquals(3, result.size());
        assertEquals(2, maximum.get());
        assertEquals(0, active.get());
    }

    @Test
    void failureDoesNotReturnUntilOtherExportWorkersHaveTerminated() throws Exception {
        var entered = new CountDownLatch(2);
        var terminated = new CountDownLatch(1);
        when(query.awaitCommand(anyString(), anyLong())).thenAnswer(invocation -> {
            String id = invocation.getArgument(0);
            var job = store.forToken(id);
            entered.countDown();
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            if (job.key().resolution() == 0) throw new AthenaQuery.QueryFailedException("FAILED");
            try {
                Thread.sleep(30000);
                throw new AssertionError("worker nao foi cancelado");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SourceUnavailableException("interrompido");
            } finally {
                terminated.countDown();
            }
        });
        var requests = List.of(AnnualCacheExports.Request.grid(0, 1, 1L), AnnualCacheExports.Request.grid(1, 1, 1L));
        assertThrows(AthenaQuery.QueryFailedException.class, () -> coordinator().export(2026, 1, 1, requests, message -> {}));
        assertEquals(0, terminated.getCount());
        assertTrue(store.rows.values().stream().noneMatch(export -> export.state().equals("complete")));
    }

    @Test
    void manifestAcceptsActualAthenaShapeButRejectsOriginalOrDuplicateObjects() throws Exception {
        String prefix = "s3://results/exports/attempt/";
        assertEquals(List.of(prefix + "part"), AnnualCacheExports.manifest(bytes(prefix + "part\n"), prefix));
        assertEquals(List.of(prefix + "part,000"), AnnualCacheExports.manifest(bytes("\"" + prefix + "part,000\"\r\n"), prefix));
        assertEquals(List.of(), AnnualCacheExports.manifest(new byte[0], prefix));
        assertThrows(IOException.class, () -> AnnualCacheExports.manifest(bytes("s3://lake/gold/map_grid/raw.parquet\n"), prefix));
        assertThrows(IOException.class, () -> AnnualCacheExports.manifest(bytes(prefix + "part\n" + prefix + "part\n"), prefix));
        assertThrows(IOException.class, () -> AnnualCacheExports.manifest(bytes(prefix + "part,unquoted\n"), prefix));
    }

    @Test
    void historicalExportsUseRequestedYearInSqlPrefixAndDurableIdentity() throws Exception {
        var requests = List.of(AnnualCacheExports.Request.grid(5, 1, 1L),
                AnnualCacheExports.Request.grid(5, 12, 1L));
        var first = coordinator().export(2025, 1, -1, requests, message -> {});
        assertEquals(first, coordinator().export(2025, 1, -1, requests, message -> {}));
        var older = coordinator().export(2024, 1, -1, requests, message -> {});
        assertTrue(first.stream().allMatch(p -> p.key().contains("/bootstrap/2025/")));
        assertTrue(older.stream().allMatch(p -> p.key().contains("/bootstrap/2024/")));
        assertEquals(4, store.rows.size());
        assertNotEquals(AnnualCacheExports.fingerprint(2025, "lake", "context", "prefix", 1, -1),
                AnnualCacheExports.fingerprint(2024, "lake", "context", "prefix", 1, -1));
        String january = AnnualCacheExports.unloadSql(2025, requests.getFirst(), "s3://results/output/");
        String december = AnnualCacheExports.unloadSql(2025, requests.getLast(), "s3://results/output/");
        assertTrue(january.contains("year = 2025 AND day >= DATE '2025-01-01' AND day < DATE '2025-02-01'"));
        assertTrue(december.contains("year = 2025 AND day >= DATE '2025-12-01' AND day < DATE '2026-01-01'"));
        assertThrows(IllegalArgumentException.class, () -> AnnualCacheExports.Request.grid(6, 1, 1L));
        assertThrows(IllegalArgumentException.class, () -> coordinator().export(2025, 1, -1,
                List.of(AnnualCacheExports.Request.places(5, 1L)), message -> {}));
        assertThrows(IllegalArgumentException.class, () -> coordinator().export(2027, 1, -1, requests, message -> {}));
    }

    private AnnualCacheExports coordinator() {
        return new AnnualCacheExports(provider(query), provider(lake), store, 2, 300000, "");
    }

    private static AthenaQuery.Command command(String id) {
        return new AthenaQuery.Command(id, "s3://results/" + id + "-manifest.csv");
    }

    private byte[] manifestFor(String location) {
        String token = location.substring("s3://results/".length(), location.length() - "-manifest.csv".length());
        return bytes(store.forToken(token).prefix() + "part-000\n");
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    private static <T> ObjectProvider<T> provider(T object) {
        ObjectProvider<T> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(object);
        return provider;
    }

    private static final class MemoryStore extends AthenaExportStore {
        private final Map<Key, Export> rows = new ConcurrentHashMap<>();
        MemoryStore() { super(new JdbcTemplate()); }
        @Override public Export find(Key key) { return rows.get(key); }
        @Override public Export begin(Key key, String token, String prefix) {
            var export = new Export(key, token, prefix, null, "pending", List.of());
            rows.put(key, export);
            return export;
        }
        @Override public void started(Export export, String id) {
            rows.put(export.key(), new Export(export.key(), export.token(), export.prefix(), id, "running", List.of()));
        }
        @Override public void complete(Export export, String manifest, List<String> files) {
            var current = rows.get(export.key());
            rows.put(export.key(), new Export(export.key(), export.token(), export.prefix(), current.queryId(), "complete", files));
        }
        @Override public void failed(Export export) {
            var current = rows.get(export.key());
            rows.put(export.key(), new Export(export.key(), export.token(), export.prefix(), current.queryId(), "failed", List.of()));
        }
        Export only() { assertEquals(1, rows.size()); return rows.values().iterator().next(); }
        Export forToken(String token) {
            return rows.values().stream().filter(export -> export.token().equals(token)).findFirst().orElseThrow();
        }
    }
}
