package com.fireatlas.gold;

import com.fireatlas.lake.integration.AnnualCacheExports;
import com.fireatlas.lake.integration.AnnualCacheSource;
import com.fireatlas.lake.integration.DeltaTable;
import com.fireatlas.lake.integration.S3LakeStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.ObjectProvider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AnnualCacheSourceTest {
    @TempDir Path root;

    @Test
    void planProjectsOnlyActive2026SnapshotAndConfirmsEmptyResolutionsBeforeReturning() throws Exception {
        var table = GoldFixtures.table(root, "gold/map_grid/");
        table.append(List.of(GoldFixtures.Row.viirs(5, 1, "2025-12-31", 5, 2),
                GoldFixtures.Row.viirs(5, 1, "2026-01-01", 3, 2)));
        table.overwrite(5, 2026, List.of(GoldFixtures.Row.viirs(5, 2, "2026-11-13", 7, 4)));
        emptyPlaces();
        var projected = projectGrid(5, "exports/grid/part.parquet", 1);
        var storage = mockStorage();
        var exports = mock(AnnualCacheExports.class);
        when(exports.export(eq(2026), eq(1L), eq(0L), anyList(), any(), any())).thenAnswer(invocation -> {
            List<AnnualCacheExports.Request> requests = invocation.getArgument(3);
            // 6 resolucoes x (12 meses da grade + 1 de lugares)
            assertEquals(78, requests.size());
            for (int res = 0; res <= 5; res++) {
                int r = res;
                assertEquals(java.util.stream.IntStream.rangeClosed(1, 12).boxed().toList(), requests.stream()
                        .filter(q -> q.kind().equals("GRID") && q.resolution() == r).map(q -> q.month()).sorted().toList());
            }
            // O unico mes com linhas sai primeiro: a fila de importacao, que comeca durante a exportacao, recebe-o antes.
            var first = requests.getFirst();
            assertEquals(List.of("GRID", 5, 11, 1L), List.of(first.kind(), first.resolution(), first.month(), first.expectedRows()));
            assertEquals(77, requests.stream().filter(r -> Long.valueOf(0).equals(r.expectedRows())).count());
            return List.of(projected);
        });
        AnnualCacheSource source = source(storage, exports);
        var plan = source.plan(2026, true);
        assertEquals(List.of(projected), plan.partitions());
        assertThrows(UnsupportedOperationException.class, () -> plan.partitions().clear());
        List<Object[]> rows = new ArrayList<>();
        assertEquals(1, source.stream(projected, rows::addAll));
        assertEquals(2L, rows.getFirst()[2]);
        assertEquals(7L, rows.getFirst()[3]);
        assertEquals(10, rows.getFirst().length);
        verify(storage, never()).download(anyString());
        verify(storage).downloadLocation(projected.key());
    }

    @Test
    void refusesSnapshotPublishedWhileAthenaWasExporting() throws Exception {
        var table = GoldFixtures.table(root, "gold/map_grid/");
        table.append(List.of(GoldFixtures.Row.viirs(5, 1, "2026-01-01", 3, 2)));
        emptyPlaces();
        var exports = mock(AnnualCacheExports.class);
        when(exports.export(eq(2026), anyLong(), anyLong(), anyList(), any(), any())).thenAnswer(invocation -> {
            table.overwrite(5, 2026, List.of(GoldFixtures.Row.viirs(5, 2, "2026-11-13", 7, 4)));
            return List.of();
        });
        var error = assertThrows(java.io.IOException.class, () -> source(mockStorage(), exports).plan(2026, true));
        assertTrue(error.getMessage().contains("snapshot Delta mudou"));
    }

    @Test
    void largePlaceResolutionsAreSplitIntoHashPartsAndSmallOnesStayWhole() throws Exception {
        GoldFixtures.table(root, "gold/map_grid/").append(List.of(GoldFixtures.Row.viirs(5, 1, "2026-01-01", 3, 2)));
        Path log = root.resolve("gold/h3_places/_delta_log");
        Files.createDirectories(log);
        Files.writeString(log.resolve("00000000000000000000.json"),
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n" + placesAdd(3, 300_000)
                        + placesAdd(4, 425_905) + placesAdd(5, 2_234_335));
        var exports = mock(AnnualCacheExports.class);
        when(exports.export(eq(2026), anyLong(), anyLong(), anyList(), any(), any())).thenAnswer(invocation -> {
            List<AnnualCacheExports.Request> requests = invocation.getArgument(3);
            var places = requests.stream().filter(r -> r.kind().equals("PLACES")).toList();
            assertEquals(List.of(AnnualCacheExports.Request.places(3, 300_000L)),
                    places.stream().filter(r -> r.resolution() == 3).toList());
            assertEquals(List.of(AnnualCacheExports.Request.placesPart(4, 1, 2), AnnualCacheExports.Request.placesPart(4, 2, 2)),
                    places.stream().filter(r -> r.resolution() == 4).toList());
            assertEquals(java.util.stream.IntStream.rangeClosed(1, 8).mapToObj(p -> AnnualCacheExports.Request.placesPart(5, p, 8)).toList(),
                    places.stream().filter(r -> r.resolution() == 5).toList());
            return List.of();
        });
        source(mockStorage(), exports).plan(2026, true);
        verify(exports).export(eq(2026), anyLong(), anyLong(), anyList(), any(), any());
    }

    @Test
    void historicalYearsComeFromActiveManifestAndHistoricalPlanNeverReadsPlaces() throws Exception {
        var table = GoldFixtures.table(root, "gold/map_grid/");
        table.append(List.of(GoldFixtures.Row.viirs(5, 1, "2025-01-01", 3, 2),
                GoldFixtures.Row.viirs(4, 2, "2025-12-31", 4, 2),
                GoldFixtures.Row.viirs(3, 3, "2023-06-01", 5, 2),
                GoldFixtures.Row.viirs(2, 4, "1999-01-01", 6, 2),
                GoldFixtures.Row.viirs(5, 5, "2026-01-01", 7, 2),
                GoldFixtures.Row.viirs(6, 6, "1998-01-01", 8, 2)));
        var storage = mockStorage();
        var exports = mock(AnnualCacheExports.class);
        when(exports.export(eq(2025), eq(0L), eq(-1L), anyList(), any(), any())).thenAnswer(invocation -> {
            List<AnnualCacheExports.Request> requests = invocation.getArgument(3);
            assertEquals(72, requests.size());
            assertTrue(requests.stream().allMatch(r -> r.kind().equals("GRID") && r.resolution() <= 5));
            assertEquals(2L, requests.stream().map(AnnualCacheExports.Request::expectedRows).mapToLong(Long::longValue).sum());
            return List.of();
        });
        var source = source(storage, exports);
        assertEquals(List.of(2025, 2023, 1999), source.historicalYears());
        assertEquals(-1L, source.plan(2025, false).placesVersion());
        verify(storage, never()).list(startsWith("gold/h3_places/"));
        verify(storage, never()).read(startsWith("gold/h3_places/"));
        assertThrows(IllegalArgumentException.class, () -> source.plan(2025, true));
        assertThrows(IllegalArgumentException.class, () -> source.plan(0, false));
        assertThrows(IllegalArgumentException.class, () -> source.plan(2027, false));
    }

    private static String placesAdd(int resolution, long rows) {
        return "{\"add\":{\"path\":\"resolution=" + resolution + "/part.parquet\",\"partitionValues\":{\"resolution\":\""
                + resolution + "\"},\"size\":1,\"modificationTime\":0,\"dataChange\":true,\"stats\":\"{\\\"numRecords\\\":"
                + rows + "}\"}}\n";
    }

    @Test
    void streamsBoundedProjectedBatchesAndReleasesFileOnConsumerFailure() throws Exception {
        List<GoldFixtures.Row> rows = new ArrayList<>();
        for (long i = 1; i <= 2501; i++) rows.add(GoldFixtures.Row.viirs(5, i, "2026-11-13", 1, 2));
        GoldFixtures.table(root, "gold/map_grid/").append(rows);
        var projected = projectGrid(5, "exports/grid/part.parquet", 2501);
        var storage = mockStorage();
        AnnualCacheSource source = source(storage, mock(AnnualCacheExports.class));
        List<Integer> sizes = new ArrayList<>();
        assertEquals(2501, source.stream(projected, batch -> sizes.add(batch.size())));
        assertEquals(List.of(1000, 1000, 501), sizes);
        assertThrows(IllegalStateException.class, () -> source.stream(projected, batch -> {
            throw new IllegalStateException("falha de persistencia");
        }));
        verify(storage, times(2)).release(any(Path.class));
        verify(storage, never()).download(anyString());
    }

    @Test
    void independentWorkersReadDifferentProjectedFilesAndCleanDistinctTemporaries() throws Exception {
        List<GoldFixtures.Row> rows = new ArrayList<>();
        for (int res : List.of(4, 5)) {
            for (long cell = 1; cell <= 1001; cell++) rows.add(GoldFixtures.Row.viirs(res, cell, "2026-11-13", 1, 2));
        }
        GoldFixtures.table(root, "gold/map_grid/").append(rows);
        var first = projectGrid(4, "exports/a/part.parquet", 1001);
        var second = projectGrid(5, "exports/b/part.parquet", 1001);
        var storage = mockStorage();
        var downloaded = new ConcurrentLinkedQueue<Path>();
        var local = new GoldFixtures.LocalStorage(root);
        doAnswer(invocation -> {
            String uri = invocation.getArgument(0);
            Path file = local.download(uri.substring("s3://results/".length()));
            downloaded.add(file);
            return file;
        }).when(storage).downloadLocation(anyString());
        AnnualCacheSource source = source(storage, mock(AnnualCacheExports.class));
        var entered = new CountDownLatch(2);
        Consumer<List<Object[]>> consumer = batch -> {
            if (batch.size() == 1000) {
                entered.countDown();
                try {
                    assertTrue(entered.await(10, TimeUnit.SECONDS), "dois streams devem coexistir");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        };
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> source.stream(first, consumer));
            var b = executor.submit(() -> source.stream(second, consumer));
            assertEquals(1001L, a.get(15, TimeUnit.SECONDS));
            assertEquals(1001L, b.get(15, TimeUnit.SECONDS));
        }
        assertEquals(2, downloaded.size());
        assertEquals(2, downloaded.stream().map(Path::getParent).distinct().count());
        assertTrue(downloaded.stream().noneMatch(Files::exists));
        verify(storage, times(2)).release(any(Path.class));
    }

    @Test
    void keepsPublishedPlaceCoordinatesAndNeverRecomputesThem() throws Exception {
        Path file = root.resolve("exports/places/part.parquet");
        Files.createDirectories(file.getParent());
        try (var c = DriverManager.getConnection("jdbc:duckdb:"); var statement = c.createStatement()) {
            statement.execute("COPY (SELECT 123::BIGINT AS h3_cell, 'SA' AS continent, 'BRA' AS country_code, "
                    + "'BR' AS country_iso_a2, 'Brazil' AS country_name, 'Brasil' AS country_name_pt, "
                    + "456::BIGINT AS city_geonameid, 'Manaus' AS city_name, 'BR' AS city_country_code, "
                    + "7.25::DOUBLE AS city_distance_km, -3.14::DOUBLE AS latitude, -59.91::DOUBLE AS longitude) "
                    + "TO '" + literal(file) + "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }
        var partition = new AnnualCacheSource.Partition("s3://results/exports/places/part.parquet", "PLACES", 5, -1, 1L, false);
        List<Object[]> rows = new ArrayList<>();
        assertEquals(1, source(mockStorage(), mock(AnnualCacheExports.class)).stream(partition, rows::addAll));
        assertEquals(12, rows.getFirst().length);
        assertEquals(-3.14, rows.getFirst()[10]);
        assertEquals(-59.91, rows.getFirst()[11]);
    }

    @Test
    void originalParquetKeysAreRejectedRatherThanSilentlyFallingBack() throws Exception {
        var storage = mockStorage();
        var raw = new AnnualCacheSource.Partition("gold/map_grid/part.parquet", "GRID", 5, 100, 1L, false);
        assertThrows(java.io.IOException.class, () -> source(storage, mock(AnnualCacheExports.class)).stream(raw, batch -> {}));
        verify(storage, never()).download(anyString());
        verify(storage, never()).downloadLocation(anyString());
    }

    private AnnualCacheSource.Partition projectGrid(int resolution, String output, long rows) throws Exception {
        var snapshot = DeltaTable.load(new GoldFixtures.LocalStorage(root), "gold/map_grid/");
        var source = snapshot.files().stream().filter(f -> f.year() == 2026 && f.resolution() == resolution)
                .findFirst().orElseThrow();
        Path file = root.resolve(output);
        Files.createDirectories(file.getParent());
        try (var c = DriverManager.getConnection("jdbc:duckdb:"); var statement = c.createStatement()) {
            statement.execute("COPY (SELECT " + String.join(",", AnnualCacheSource.GRID_COLUMNS)
                    + " FROM read_parquet('" + literal(root.resolve(source.key())) + "', hive_partitioning=false)) TO '"
                    + literal(file) + "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }
        return new AnnualCacheSource.Partition("s3://results/" + output, "GRID", resolution, Files.size(file), rows, false);
    }

    private static String literal(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "/").replace("'", "''");
    }

    private void emptyPlaces() throws Exception {
        Path log = root.resolve("gold/h3_places/_delta_log");
        Files.createDirectories(log);
        Files.writeString(log.resolve("00000000000000000000.json"),
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n");
    }

    private S3LakeStorage mockStorage() throws Exception {
        var local = new GoldFixtures.LocalStorage(root);
        var storage = mock(S3LakeStorage.class);
        when(storage.list(anyString())).thenAnswer(invocation -> local.list(invocation.getArgument(0)));
        when(storage.read(anyString())).thenAnswer(invocation -> local.read(invocation.getArgument(0)));
        when(storage.download(anyString())).thenAnswer(invocation -> local.download(invocation.getArgument(0)));
        when(storage.downloadLocation(anyString())).thenAnswer(invocation -> {
            String uri = invocation.getArgument(0);
            return local.download(uri.substring("s3://results/".length()));
        });
        doAnswer(invocation -> { local.release(invocation.getArgument(0)); return null; })
                .when(storage).release(any(Path.class));
        return storage;
    }

    @SuppressWarnings("unchecked")
    private AnnualCacheSource source(S3LakeStorage storage, AnnualCacheExports exports) {
        ObjectProvider<S3LakeStorage> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(storage);
        return new AnnualCacheSource(provider, exports);
    }
}
