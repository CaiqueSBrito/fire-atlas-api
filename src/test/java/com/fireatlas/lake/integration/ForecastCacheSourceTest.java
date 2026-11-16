package com.fireatlas.lake.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.time.YearMonth;
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

class ForecastCacheSourceTest {
    @TempDir Path root;
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Map<String, byte[]> logs = new ConcurrentHashMap<>();
    private S3LakeStorage lake;
    private AthenaQuery query;
    private MemoryStore store;

    @BeforeEach
    void setUp() throws Exception {
        lake = mock(S3LakeStorage.class);
        query = mock(AthenaQuery.class);
        store = new MemoryStore();
        log("forecast", 0, add(2026, 11, 1L));
        log("h3_places", 0, "");
        when(lake.list(anyString())).thenAnswer(invocation -> {
            String prefix = invocation.getArgument(0);
            return logs.keySet().stream().filter(key -> key.startsWith(prefix)).sorted().toList();
        });
        when(lake.read(anyString())).thenAnswer(invocation -> logs.get(invocation.getArgument(0)));
        when(lake.location("gold/")).thenReturn("s3://lake/gold/");
        when(query.sourceIdentity()).thenReturn("fireatlas_gold/fireatlas-api");
        when(query.workgroupOutputLocation()).thenReturn("s3://results/athena-results/");
        when(query.startCommand(anyString(), anyString())).thenAnswer(invocation -> invocation.getArgument(1));
        when(query.awaitCommand(anyString(), anyLong())).thenAnswer(invocation -> command(invocation.getArgument(0)));
        when(lake.readLocation(anyString())).thenAnswer(invocation -> manifest(invocation.getArgument(0)));
    }

    @Test
    void plansOnlyActiveActualMonthsAcrossAllYearsAndKeepsCrossYearDurableKeysSeparate() throws Exception {
        log("forecast", 0, add(2024, 1, 1L) + add(2027, 1, 2L) + add(2027, 12, null));
        // Removed data must not produce an export, even when its year/month once existed.
        log("forecast", 1, "{\"remove\":{\"path\":\"year=2024/month=1/part.parquet\"}}\n" + add(2025, 1, 3L));
        List<String> events = new ArrayList<>();
        var plan = source().plan(message -> {}, new AnnualCacheExports.Listener() {
            @Override public void snapshot(String id) { events.add("snapshot:" + id); }
            @Override public void exported(List<AnnualCacheSource.Partition> partitions) {
                partitions.forEach(partition -> events.add(partition.key()));
            }
        });
        assertEquals(1, plan.forecastVersion());
        assertEquals(0, plan.placesVersion());
        assertEquals(3, plan.partitions().size());
        assertEquals("snapshot:" + plan.snapshot(), events.getFirst());
        assertTrue(plan.partitions().stream().allMatch(partition -> partition.kind().equals("FORECAST")
                && partition.resolution() == 6 && partition.key().contains(plan.snapshot())));
        var januaries = store.rows.values().stream().filter(export -> export.key().month() == 1).toList();
        assertEquals(2, januaries.size());
        assertNotEquals(januaries.get(0).key().snapshot(), januaries.get(1).key().snapshot());
        assertTrue(store.rows.values().stream().anyMatch(export -> export.prefix().contains("/year=2025/")));
        assertTrue(store.rows.values().stream().anyMatch(export -> export.prefix().contains("/year=2027/")));
        verify(query, times(3)).startCommand(anyString(), anyString());
        verify(query, never()).run(anyString());
        verify(lake, never()).list(startsWith("gold/map_grid/"));
        verify(lake, never()).download(anyString());
        assertThrows(UnsupportedOperationException.class, () -> plan.partitions().clear());
    }

    @Test
    void fingerprintSeparatesSourceContextProjectionSnapshotsYearsAndMonths() {
        var months = List.of(YearMonth.of(2026, 11));
        String original = ForecastCacheSource.fingerprint("lake", "context", "prefix", 1, 2, months);
        assertEquals(original, ForecastCacheSource.fingerprint("lake", "context", "prefix", 1, 2, months));
        assertNotEquals(original, ForecastCacheSource.fingerprint("other-lake", "context", "prefix", 1, 2, months));
        assertNotEquals(original, ForecastCacheSource.fingerprint("lake", "other-context", "prefix", 1, 2, months));
        assertNotEquals(original, ForecastCacheSource.fingerprint("lake", "context", "other-prefix", 1, 2, months));
        assertNotEquals(original, ForecastCacheSource.fingerprint("lake", "context", "prefix", 3, 2, months));
        assertNotEquals(original, ForecastCacheSource.fingerprint("lake", "context", "prefix", 1, 3, months));
        assertNotEquals(original, ForecastCacheSource.fingerprint("lake", "context", "prefix", 1, 2, List.of(YearMonth.of(2025, 11))));
        assertNotEquals(original, ForecastCacheSource.fingerprint("lake", "context", "prefix", 1, 2, List.of(YearMonth.of(2026, 12))));
    }

    @Test
    void refusesPublicationWhenEitherDeltaSnapshotChangesDuringExport() throws Exception {
        for (String table : List.of("forecast", "h3_places")) {
            when(query.awaitCommand(anyString(), anyLong())).thenAnswer(invocation -> {
                log(table, 1, "");
                return command(invocation.getArgument(0));
            });
            var error = assertThrows(IOException.class, () -> source().plan(message -> {}, AnnualCacheExports.Listener.NONE));
            assertTrue(error.getMessage().contains("snapshot Delta mudou"));
            logs.remove(key(table, 1));
            store.rows.clear();
        }
    }

    @Test
    void completedManifestSurvivesRestartAndNewSnapshotExportsFreshObjects() throws Exception {
        var first = plan();
        assertEquals(first, plan());
        verify(query, times(1)).startCommand(anyString(), anyString());
        verify(lake, times(1)).readLocation(anyString());
        log("h3_places", 1, "");
        var refreshed = plan();
        assertNotEquals(first.snapshot(), refreshed.snapshot());
        assertNotEquals(first.partitions().getFirst().key(), refreshed.partitions().getFirst().key());
        verify(query, times(2)).startCommand(anyString(), anyString());
    }

    @Test
    void temporaryManifestFailureResumesSuccessfulCommandAndPersistsIdBeforeWaiting() throws Exception {
        var fail = new AtomicBoolean(true);
        when(query.awaitCommand(anyString(), anyLong())).thenAnswer(invocation -> {
            String id = invocation.getArgument(0);
            assertEquals(id, store.forToken(id).queryId());
            assertEquals("running", store.forToken(id).state());
            return command(id);
        });
        doAnswer(invocation -> {
            if (fail.getAndSet(false)) throw new IOException("manifesto temporariamente indisponivel");
            return manifest(invocation.getArgument(0));
        }).when(lake).readLocation(anyString());
        assertThrows(IOException.class, this::plan);
        var running = store.only();
        assertEquals("running", running.state());
        assertNotNull(running.queryId());
        assertTrue(plan().partitions().getFirst().key().startsWith(running.prefix()));
        verify(query, times(1)).startCommand(anyString(), anyString());
        verify(query, times(2)).awaitCommand(running.queryId(), 300000);
        assertEquals("complete", store.only().state());
    }

    @Test
    void multiObjectManifestEmitsOnlyListedFilesAndDoesNotApplyWholeMonthCountToEachFile() throws Exception {
        log("forecast", 0, add(2026, 11, 100L));
        doAnswer(invocation -> {
            String prefix = store.only().prefix();
            return bytes("\"" + prefix + "part-000\"\r\n\"" + prefix + "part-001\"\r\n");
        }).when(lake).readLocation(anyString());
        var partitions = plan().partitions();
        assertEquals(List.of(store.only().prefix() + "part-000", store.only().prefix() + "part-001"),
                partitions.stream().map(AnnualCacheSource.Partition::key).toList());
        assertTrue(partitions.stream().allMatch(partition -> partition.expectedRows() == null && !partition.empty()));
        verify(lake, never()).download(anyString());
        verify(lake, never()).downloadLocation(anyString());
    }

    @Test
    void submissionCrashReusesIdempotencyTokenAndSqlRatherThanChoosingNewPrefix() throws Exception {
        store = spy(store);
        doThrow(new IllegalStateException("queda antes de salvar query_id")).doCallRealMethod()
                .when(store).started(any(), anyString());
        assertThrows(IllegalStateException.class, this::plan);
        var pending = store.only();
        assertEquals("pending", pending.state());
        assertNull(pending.queryId());
        plan();
        verify(query, times(2)).startCommand(ForecastCacheSource.unloadSql(YearMonth.of(2026, 11), pending.prefix()), pending.token());
        verify(query, times(1)).awaitCommand(pending.token(), 300000);
    }

    @Test
    void onlyTerminalQueryFailureGetsNewEmptyAttemptPrefix() throws Exception {
        when(query.awaitCommand(anyString(), anyLong()))
                .thenThrow(new AthenaQuery.QueryFailedException("Athena FAILED"))
                .thenAnswer(invocation -> command(invocation.getArgument(0)));
        assertThrows(AthenaQuery.QueryFailedException.class, this::plan);
        var failed = store.only();
        assertEquals("failed", failed.state());
        var retry = plan();
        assertNotEquals(failed.prefix(), store.only().prefix());
        assertFalse(retry.partitions().getFirst().key().startsWith(failed.prefix()));
        verify(query, times(2)).startCommand(anyString(), anyString());
    }

    @Test
    void confirmedEmptySourceAndZeroRecordMonthEmitExplicitEmptyPartitionsWithoutUnload() throws Exception {
        log("forecast", 0, "");
        List<String> events = new ArrayList<>();
        var empty = source().plan(message -> {}, new AnnualCacheExports.Listener() {
            @Override public void snapshot(String id) { events.add(id); }
            @Override public void exported(List<AnnualCacheSource.Partition> partitions) {
                assertEquals(1, events.size());
                events.add(partitions.getFirst().key());
            }
        });
        assertEquals(1, empty.partitions().size());
        assertTrue(empty.partitions().getFirst().empty());
        assertEquals(0L, empty.partitions().getFirst().expectedRows());
        assertTrue(events.getLast().contains(empty.snapshot()));
        assertEquals(0, source().stream(empty.partitions().getFirst(), batch -> fail("nenhuma linha inventada")));
        log("forecast", 0, add(2026, 11, 0L));
        assertTrue(plan().partitions().getFirst().empty());
        verify(query, never()).startCommand(anyString(), anyString());
        verify(lake, never()).downloadLocation(anyString());
        assertTrue(store.rows.isEmpty());
    }

    @Test
    void emptyManifestConfirmsUnknownRowsButNeverHidesKnownRows() throws Exception {
        log("forecast", 0, add(2026, 11, null));
        doReturn(new byte[0]).when(lake).readLocation(anyString());
        var empty = plan();
        assertTrue(empty.partitions().getFirst().empty());
        assertEquals("complete", store.only().state());
        store.rows.clear();
        log("forecast", 0, add(2026, 11, 2L));
        assertThrows(IOException.class, this::plan);
        assertEquals("running", store.only().state());
    }

    @Test
    void missingManifestIsNotAnEmptyPartitionAndDoesNotRepeatSuccessfulUnload() throws Exception {
        when(query.awaitCommand(anyString(), anyLong())).thenAnswer(invocation -> new AthenaQuery.Command(invocation.getArgument(0), null));
        assertThrows(IOException.class, this::plan);
        var running = store.only();
        assertEquals("running", running.state());
        verify(lake, never()).readLocation(anyString());
        when(query.awaitCommand(anyString(), anyLong())).thenAnswer(invocation -> command(invocation.getArgument(0)));
        plan();
        verify(query, times(1)).startCommand(anyString(), anyString());
        verify(query, times(2)).awaitCommand(running.queryId(), 300000);
    }

    @Test
    void manifestsRejectOriginalDuplicateAndPersistedOutOfDestinationObjects() throws Exception {
        doReturn(bytes("s3://lake/gold/forecast/raw.parquet\n")).when(lake).readLocation(anyString());
        assertThrows(IOException.class, this::plan);
        var running = store.only();
        doReturn(bytes(running.prefix() + "part\n" + running.prefix() + "part\n")).when(lake).readLocation(anyString());
        assertThrows(IOException.class, this::plan);
        doAnswer(invocation -> manifest(invocation.getArgument(0))).when(lake).readLocation(anyString());
        plan();
        var complete = store.only();
        store.rows.put(complete.key(), new AthenaExportStore.Export(complete.key(), complete.token(), complete.prefix(),
                complete.queryId(), "complete", List.of("s3://lake/gold/forecast/raw.parquet")));
        assertThrows(IOException.class, this::plan);
        verify(query, times(1)).startCommand(anyString(), anyString());
        verify(lake, never()).download(anyString());
        verify(lake, never()).downloadLocation(anyString());
    }

    @Test
    void workersAreConcurrentBoundedAndUseConfiguredTimeout() throws Exception {
        log("forecast", 0, add(2026, 1, 1L) + add(2026, 2, 1L) + add(2026, 3, 1L));
        var entered = new CountDownLatch(2);
        var active = new AtomicInteger();
        var maximum = new AtomicInteger();
        when(query.awaitCommand(anyString(), eq(12345L))).thenAnswer(invocation -> {
            maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
            try {
                entered.countDown();
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                return command(invocation.getArgument(0));
            } finally {
                active.decrementAndGet();
            }
        });
        var source = new ForecastCacheSource(provider(lake), provider(query), store, 2, 12345, "s3://results/custom/");
        assertEquals(3, source.plan(message -> {}, AnnualCacheExports.Listener.NONE).partitions().size());
        assertEquals(2, maximum.get());
        assertEquals(0, active.get());
        verify(query, times(3)).awaitCommand(anyString(), eq(12345L));
        assertTrue(store.rows.values().stream().allMatch(export -> export.prefix().startsWith("s3://results/custom/forecast/")));
    }

    @Test
    void monthlyProjectionPreservesNativeProbabilityDateNullableMetricsCoordinatesAndOneToOneJoin() throws Exception {
        Path projected = root.resolve("projected.parquet");
        try (var connection = DriverManager.getConnection("jdbc:duckdb:"); var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE forecast AS SELECT DATE '2026-11-15' issue_date, 123::BIGINT h3_r6, "
                    + "7::BIGINT h3_r1, 0.73123456789::DOUBLE prob_fire_7d, NULL::DOUBLE det_24h, 3::DOUBLE det_3d, "
                    + "7::DOUBLE det_7d, NULL::DOUBLE frp_mean_7d, 300::DOUBLE modis_brightness_mean_7d, "
                    + "NULL::DOUBLE viirs_brightness_mean_7d, 1::DOUBLE ring1_det_24h, 2::DOUBLE ring1_det_7d, "
                    + "3::DOUBLE ring2_det_7d, true active, 2026 AS \"year\", 11 AS \"month\"");
            statement.execute("INSERT INTO forecast SELECT issue_date, 456, h3_r1, prob_fire_7d, det_24h, det_3d, det_7d, "
                    + "frp_mean_7d, modis_brightness_mean_7d, viirs_brightness_mean_7d, ring1_det_24h, ring1_det_7d, "
                    + "ring2_det_7d, active, year, month FROM forecast");
            statement.execute("CREATE TABLE h3_places AS SELECT 123::BIGINT h3_cell, 'Brasil' country_name_pt, 'Manaus' city_name, "
                    + "7.25::DOUBLE city_distance_km, -3.14::DOUBLE latitude, -59.91::DOUBLE longitude, 'SA' continent, "
                    + "'BRA' country_code, 'BR' country_iso_a2, 456::BIGINT city_geonameid, 6 resolution");
            statement.execute("INSERT INTO h3_places SELECT * FROM h3_places");
            String unload = ForecastCacheSource.unloadSql(YearMonth.of(2026, 11), "s3://ignored/");
            String select = unload.substring("UNLOAD (".length(), unload.indexOf(") TO '"));
            statement.execute("COPY (" + select + ") TO '" + literal(projected) + "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }
        downloads(projected);
        List<Object[]> rows = new ArrayList<>();
        var partition = partition(2L);
        assertEquals(2, source().stream(partition, rows::addAll));
        assertEquals(2, rows.size());
        for (Object[] row : rows) {
            assertEquals(ForecastCacheSource.FORECAST_COLUMNS.size(), row.length);
            assertEquals(Date.valueOf("2026-11-15"), row[0]);
            assertEquals(0.73123456789, row[3]);
            assertNull(row[4]);
            assertNull(row[7]);
            assertNull(row[9]);
            if (row[1].equals(123L)) {
                assertEquals("Manaus", row[15]);
                assertEquals(-3.14, row[17]);
                assertEquals(-59.91, row[18]);
                assertEquals("BR", row[21]);
                assertEquals(456L, row[22]);
            } else {
                assertEquals(456L, row[1]);
                for (int index = 14; index < row.length; index++) assertNull(row[index]);
            }
        }
        verify(lake).downloadLocation(partition.key());
        verify(lake).release(any(Path.class));
        verify(lake, never()).download(anyString());
    }

    @Test
    void streamsOnlyExportedObjectInBoundedBatchesAndReleasesOnFailureOrCountMismatch() throws Exception {
        Path projected = parquet(2501, false);
        downloads(projected);
        List<Integer> sizes = new ArrayList<>();
        assertEquals(2501, source().stream(partition(2501L), batch -> sizes.add(batch.size())));
        assertEquals(List.of(1000, 1000, 501), sizes);
        assertThrows(IllegalStateException.class, () -> source().stream(partition(2501L), batch -> {
            throw new IllegalStateException("falha COPY");
        }));
        assertThrows(IOException.class, () -> source().stream(partition(2500L), batch -> {}));
        verify(lake, times(3)).release(any(Path.class));
        verify(lake, never()).download(anyString());
    }

    @Test
    void refusesMissingContractColumnsAndOriginalOrNonForecastPartitions() throws Exception {
        downloads(parquet(1, true));
        assertThrows(IOException.class, () -> source().stream(partition(1L), batch -> {}));
        verify(lake).release(any(Path.class));
        assertThrows(IOException.class, () -> source().stream(new AnnualCacheSource.Partition(
                "gold/forecast/raw.parquet", "FORECAST", 6, 1, 1L, false), batch -> {}));
        assertThrows(IOException.class, () -> source().stream(new AnnualCacheSource.Partition(
                "s3://results/export.parquet", "GRID", 5, 1, 1L, false), batch -> {}));
        verify(lake, times(1)).downloadLocation(anyString());
        verify(lake, never()).download(anyString());
    }

    @Test
    void invalidForecastPartitionMetadataIsNotTreatedAsEmpty() throws Exception {
        log("forecast", 0, JSON.writeValueAsString(Map.of("add", Map.of("path", "bad.parquet", "partitionValues",
                Map.of("year", "2026", "month", "13"), "size", 1))) + "\n");
        assertThrows(IOException.class, this::plan);
        verify(query, never()).startCommand(anyString(), anyString());
    }

    private Path parquet(int count, boolean missingColumn) throws Exception {
        Path file = root.resolve("projected-" + count + "-" + missingColumn + ".parquet");
        List<String> selection = new ArrayList<>();
        for (String column : ForecastCacheSource.FORECAST_COLUMNS) {
            if (missingColumn && column.equals("prob_fire_7d")) continue;
            String value = switch (column) {
                case "issue_date" -> "DATE '2026-11-15'";
                case "h3_r6", "h3_r1", "city_geonameid" -> "i::BIGINT";
                case "active" -> "true";
                case "country_name_pt", "city_name", "continent", "country_code", "country_iso_a2" -> "NULL::VARCHAR";
                default -> "NULL::DOUBLE";
            };
            selection.add(value + " AS \"" + column + "\"");
        }
        try (var connection = DriverManager.getConnection("jdbc:duckdb:"); var statement = connection.createStatement()) {
            statement.execute("COPY (SELECT " + String.join(", ", selection) + " FROM range(1, " + (count + 1)
                    + ") t(i)) TO '" + literal(file) + "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }
        return file;
    }

    private void downloads(Path projected) throws Exception {
        when(lake.downloadLocation(anyString())).thenAnswer(invocation -> {
            Path temporary = Files.createTempFile(root, "download-", ".parquet");
            Files.copy(projected, temporary, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            return temporary;
        });
        doAnswer(invocation -> { Files.delete(invocation.getArgument(0)); return null; }).when(lake).release(any(Path.class));
    }

    private AnnualCacheSource.Partition partition(Long expected) {
        return new AnnualCacheSource.Partition("s3://results/exports/forecast/projected.parquet", "FORECAST", 6, -1, expected, false);
    }

    private ForecastCacheSource.Plan plan() throws IOException {
        return source().plan(message -> {}, AnnualCacheExports.Listener.NONE);
    }

    private ForecastCacheSource source() {
        return new ForecastCacheSource(provider(lake), provider(query), store, 2, 300000, "");
    }

    private void log(String table, int version, String body) {
        logs.put(key(table, version), bytes("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n" + body));
    }

    private static String key(String table, int version) {
        return "gold/" + table + "/_delta_log/" + String.format("%020d", version) + ".json";
    }

    private static String add(int year, int month, Long records) throws Exception {
        Map<String, Object> fields = new java.util.LinkedHashMap<>();
        fields.put("path", "year=" + year + "/month=" + month + "/part.parquet");
        fields.put("partitionValues", Map.of("year", String.valueOf(year), "month", String.valueOf(month)));
        fields.put("size", 1);
        if (records != null) fields.put("stats", JSON.writeValueAsString(Map.of("numRecords", records)));
        return JSON.writeValueAsString(Map.of("add", fields)) + "\n";
    }

    private static AthenaQuery.Command command(String token) {
        return new AthenaQuery.Command(token, "s3://results/" + token + "-manifest.csv");
    }

    private byte[] manifest(String location) {
        String token = location.substring("s3://results/".length(), location.length() - "-manifest.csv".length());
        return bytes(store.forToken(token).prefix() + "part-000\n");
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String literal(Path file) { return file.toAbsolutePath().toString().replace("\\", "/").replace("'", "''"); }

    @SuppressWarnings("unchecked")
    private static <T> ObjectProvider<T> provider(T value) {
        ObjectProvider<T> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(value);
        return provider;
    }

    private static class MemoryStore extends AthenaExportStore {
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
