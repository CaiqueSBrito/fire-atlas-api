package com.fireatlas.gold;

import com.fireatlas.gold.bootstrap.repository.AnnualCacheQuery;
import com.fireatlas.gold.bootstrap.repository.BootstrapRepository;
import com.fireatlas.gold.common.policy.SourceUnavailableException;
import com.fireatlas.gold.detection.repository.DetectionRepository;
import com.fireatlas.gold.region.entity.Region;
import com.fireatlas.lake.integration.AnnualCacheExports;
import com.fireatlas.lake.integration.AnnualCacheSource;
import com.fireatlas.lake.integration.AthenaQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(DbAvailable.class)
class AnnualCachePostgresTest {
    private DriverManagerDataSource dataSource;
    private JdbcTemplate jdbc;
    private JdbcTemplate admin;
    private String schema;
    private AnnualCacheSource source;
    private BootstrapRepository bootstrap;

    @BeforeEach
    void createSchema() throws Exception {
        admin = new JdbcTemplate(new DriverManagerDataSource(DbAvailable.URL, DbAvailable.USER, DbAvailable.PASSWORD));
        schema = "annual_test_" + UUID.randomUUID().toString().replace("-", "");
        admin.execute("CREATE SCHEMA " + schema);
        dataSource = new DriverManagerDataSource(DbAvailable.URL, DbAvailable.USER, DbAvailable.PASSWORD);
        Properties properties = new Properties();
        properties.setProperty("currentSchema", schema + ",public");
        dataSource.setConnectionProperties(properties);
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
        bootstrap = new BootstrapRepository(dataSource, jdbc, source, 6, 2);
    }

    @AfterEach
    void dropSchema() { if (schema != null) admin.execute("DROP SCHEMA " + schema + " CASCADE"); }

    @Test
    void lockRecoveryRetryAndRefreshReuseOnlyCommittedFiles() throws Exception {
        bootstrap = new BootstrapRepository(dataSource, jdbc, source, 6, 1);
        var first = partition("grid-a", "GRID", 1);
        var second = partition("grid-b", "GRID", 1);
        var plan = new AnnualCacheSource.Plan(1, 1, List.of(first, second));
        when(source.plan(2026, true)).thenReturn(plan);
        AtomicBoolean fail = new AtomicBoolean(true);
        doAnswer(invocation -> {
            var partition = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.<Object[]>of(gridRow(partition.key().equals("grid-a") ? 10 : 20, "VIIRS", 3)));
            if (partition.key().equals("grid-b") && fail.getAndSet(false)) throw new IOException("falha de leitura simulada");
            return 1L;
        }).when(source).stream(any(), any());

        assertEquals("idle", bootstrap.status(2026).state());
        var lease = bootstrap.claim(2026);
        assertNotNull(lease);
        var restarted = new BootstrapRepository(dataSource, jdbc, source, 6, 1);
        assertNull(restarted.claim(2026));
        assertEquals("loading", restarted.status(2026).state());
        bootstrap.load(lease);
        assertEquals("failed", bootstrap.status(2026).state());
        assertEquals(1, bootstrap.status(2026).completedUnits());
        assertEquals(1, bootstrap.status(2026).rowsLoaded());
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid", Integer.class));

        restarted.load(restarted.claim(2026));
        assertEquals("ready", restarted.status(2026).state());
        assertEquals(2, restarted.status(2026).completedUnits());
        assertEquals(2, restarted.status(2026).rowsLoaded());
        verify(source, times(1)).stream(eq(first), any());
        verify(source, times(2)).stream(eq(second), any());
        assertNull(restarted.claim(2026));
        verify(source, times(2)).plan(2026, true);

        // Simula processo morto: status GET nao escreve nem baixa; POST recupera o plano persistido.
        jdbc.update("UPDATE annual_grid_bootstrap SET state='loading'");
        assertEquals("failed", restarted.status(2026).state());
        restarted.load(restarted.claim(2026));
        assertEquals("ready", restarted.status(2026).state());
        // Depois da primeira publicacao nem a tabela vazia de lugares perde seus indices.
        assertEquals(0, restarted.status(2026).finalizationTotal());
        assertEquals(0, restarted.status(2026).finalizationCompleted());
        verify(source, times(1)).stream(eq(first), any());

        // Nova versao remove um arquivo e conserva outro; somente o arquivo novo e transferido.
        jdbc.update("UPDATE annual_grid_bootstrap SET checked_at=now()-interval '7 hours'");
        var replacement = partition("grid-c", "GRID", 1);
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(2, 1, List.of(first, replacement)));
        restarted.load(restarted.claim(2026));
        assertEquals("ready", restarted.status(2026).state());
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_grid_partition WHERE source_key='grid-b'", Integer.class));
        // Linhas do arquivo removido do plano saem em cascata; o novo arquivo publica as suas.
        assertEquals(1, jdbc.queryForObject("""
                SELECT count(*) FROM annual_map_grid g JOIN annual_grid_partition p ON p.id = g.partition_id
                WHERE p.source_key = 'grid-c'
                """, Integer.class));
        verify(source, times(1)).stream(eq(first), any());
        verify(source, times(1)).stream(eq(replacement), any());
    }

    @Test
    void binaryCopyPreservesTextExactlyIncludingQuotesLineBreaksEmptyStringsAndNulls() throws Exception {
        var places = partition("places", "PLACES", 2);
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(places)));
        doAnswer(invocation -> {
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.of(
                    new Object[]{10L, "Africa", "CIV", "CI", "Côte d'Ivoire", "Costa do \"Marfim\"", 7L,
                            "Bonaire, Sint Eustatius\nand Saba", "", 1.5, -10.0, -50.0},
                    new Object[]{20L, null, null, null, null, null, null, null, null, null, 0.0, 0.0}));
            return 2L;
        }).when(source).stream(any(), any());
        bootstrap.load(bootstrap.claim(2026));
        assertEquals("ready", bootstrap.status(2026).state());
        var row = jdbc.queryForMap("SELECT * FROM annual_h3_places WHERE h3_cell=10");
        assertEquals("Côte d'Ivoire", row.get("country_name"));
        assertEquals("Costa do \"Marfim\"", row.get("country_name_pt"));
        assertEquals("Bonaire, Sint Eustatius\nand Saba", row.get("city_name"));
        assertEquals("", row.get("city_country_code"));
        assertEquals(1.5, row.get("city_distance_km"));
        var empty = jdbc.queryForMap("SELECT * FROM annual_h3_places WHERE h3_cell=20");
        assertNull(empty.get("continent"));
        assertNull(empty.get("city_geonameid"));
        assertNull(empty.get("city_distance_km"));
    }

    @Test
    void importStartsWhileExportStillRunsAndOtherSnapshotsLeaveBeforeTheFirstUnload() throws Exception {
        // Cache de outro snapshot ja publicado: precisa sair antes que algo deste snapshot seja importado.
        var old = partition("s3://old-snapshot/grid/part", "GRID", 1);
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(old)));
        doAnswer(invocation -> {
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.<Object[]>of(gridRow(10, "VIIRS", 1)));
            return 1L;
        }).when(source).stream(eq(old), any());
        bootstrap.load(bootstrap.claim(2026));
        assertEquals("ready", bootstrap.status(2026).state());
        jdbc.update("UPDATE annual_grid_bootstrap SET checked_at=now()-interval '7 hours'");

        var exported = partition("s3://results/snap-2/grid/part", "GRID", 1);
        var imported = new CountDownLatch(1);
        doAnswer(invocation -> {
            AnnualCacheExports.Listener listener = invocation.getArgument(3);
            listener.snapshot("snap-2");
            assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid", Integer.class),
                    "linhas de outro snapshot saem antes do primeiro UNLOAD");
            listener.exported(List.of(exported));
            // Sem sobreposicao isto esgotaria o tempo: o plano so volta depois do arquivo ja ter sido importado.
            assertTrue(imported.await(10, TimeUnit.SECONDS), "importacao deve comecar durante a exportacao");
            return new AnnualCacheSource.Plan(2, 1, List.of(exported));
        }).when(source).plan(eq(2026), eq(true), any(), any());
        doAnswer(invocation -> {
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.<Object[]>of(gridRow(10, "VIIRS", 1)));
            imported.countDown();
            return 1L;
        }).when(source).stream(eq(exported), any());
        bootstrap.load(bootstrap.claim(2026));
        assertEquals("ready", bootstrap.status(2026).state());
        assertEquals(1, bootstrap.status(2026).completedUnits());
        assertEquals(List.of("s3://results/snap-2/grid/part"),
                jdbc.queryForList("SELECT source_key FROM annual_grid_partition", String.class));
        verify(source, times(1)).stream(eq(exported), any());
    }

    @Test
    void loadFromScratchRestoresIndexesPrimaryAndForeignKeysBeforeReady() throws Exception {
        String indexes = "SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes WHERE tablename IN"
                + " ('annual_map_grid', 'annual_h3_places') AND schemaname = current_schema()";
        String constraints = "SELECT string_agg(conname, ',' ORDER BY conname) FROM pg_constraint"
                + " WHERE conrelid IN ('annual_map_grid'::regclass, 'annual_h3_places'::regclass) AND contype IN ('p', 'f')";
        String before = jdbc.queryForObject(indexes, String.class);
        String keysBefore = jdbc.queryForObject(constraints, String.class);
        long operations = before.split(",").length + keysBefore.split(",").length + 1L; // indices, PK/FK e ANALYZE
        var grid = partition("grid", "GRID", 2);
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(grid)));
        AtomicBoolean droppedDuringCopy = new AtomicBoolean();
        doAnswer(invocation -> {
            // Durante o COPY de uma carga do zero nao sobra nenhum indice nas tabelas do cache (string_agg vazio = NULL).
            String during = jdbc.queryForObject(indexes, String.class);
            droppedDuringCopy.set(during == null);
            var progress = bootstrap.status(2026);
            assertEquals("importing", progress.phase());
            assertEquals(0, progress.finalizationCompleted());
            assertEquals(operations, progress.finalizationTotal());
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.of(gridRow(10, "VIIRS", 1), gridRow(10, "MODIS", 1)));
            return 2L;
        }).when(source).stream(any(), any());
        bootstrap.load(bootstrap.claim(2026));
        assertEquals("ready", bootstrap.status(2026).state());
        assertTrue(droppedDuringCopy.get(), "carga do zero grava sem a chave primaria");
        assertEquals(before, jdbc.queryForObject(indexes, String.class));
        assertEquals(keysBefore, jdbc.queryForObject(constraints, String.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_cache_deferred_index", Integer.class));
        assertEquals("ready", bootstrap.status(2026).phase());
        assertEquals(operations, bootstrap.status(2026).finalizationTotal());
        assertEquals(operations, bootstrap.status(2026).finalizationCompleted());
        // Arquivos completos nao autorizam consultas quando a finalizacao ainda esta pendente.
        jdbc.update("UPDATE annual_grid_bootstrap SET finalization_completed=finalization_total-1");
        assertEquals("failed", bootstrap.status(2026).state());
        assertThrows(SourceUnavailableException.class, () -> bootstrap.requireReady(jdbc));
    }

    @Test
    void rebuildsTargetIndexesEvenWhenFallbackSchemaContainsAnIndexWithTheSameName() throws Exception {
        String fallback = schema + "_fallback";
        admin.execute("CREATE SCHEMA " + fallback);
        try {
            admin.execute("CREATE TABLE " + fallback + ".shadow_places (id integer)");
            admin.execute("CREATE UNIQUE INDEX annual_h3_places_pkey ON " + fallback + ".shadow_places (id)");
            Properties properties = new Properties();
            properties.setProperty("currentSchema", schema + "," + fallback + ",public");
            dataSource.setConnectionProperties(properties);
            when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1,
                    List.of(partition("grid", "GRID", 1))));
            doAnswer(invocation -> {
                Consumer<List<Object[]>> consumer = invocation.getArgument(1);
                consumer.accept(List.<Object[]>of(gridRow(10, "VIIRS", 1)));
                return 1L;
            }).when(source).stream(any(), any());
            bootstrap.load(bootstrap.claim(2026));
            assertEquals("ready", bootstrap.status(2026).state());
            assertEquals(1, jdbc.queryForObject("""
                    SELECT count(*) FROM pg_constraint
                    WHERE conrelid='annual_h3_places'::regclass AND contype='p'
                    """, Integer.class));
            assertTrue(admin.queryForObject("SELECT to_regclass(?) IS NOT NULL", Boolean.class,
                    fallback + ".annual_h3_places_pkey"));
        } finally {
            admin.execute("DROP SCHEMA " + fallback + " CASCADE");
        }
    }

    @Test
    void duplicateRowsAcrossFilesFailWhenThePrimaryKeyIsRebuiltInsteadOfBeingPublished() throws Exception {
        var a = partition("grid-a", "GRID", 1);
        var b = partition("grid-b", "GRID", 1);
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(a, b)));
        doAnswer(invocation -> {
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.<Object[]>of(gridRow(10, "VIIRS", 1)));
            return 1L;
        }).when(source).stream(any(), any());
        bootstrap.load(bootstrap.claim(2026));
        assertEquals("failed", bootstrap.status(2026).state());
        assertTrue(bootstrap.status(2026).error().contains("23505"), bootstrap.status(2026).error());
        assertTrue(bootstrap.status(2026).finalizationCompleted() < bootstrap.status(2026).finalizationTotal(),
                "a chave que falhou nao conta como concluida");
        assertThrows(SourceUnavailableException.class, () -> bootstrap.requireReady(jdbc));
    }

    @Test
    void cachedFiltersStatisticsAndMixedYearRankingUseWholeDataset() throws Exception {
        var grid = partition("grid", "GRID", 4);
        var places = partition("places", "PLACES", 2);
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(grid, places)));
        doAnswer(invocation -> {
            var partition = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            List<Object[]> rows = partition.kind().equals("GRID")
                    ? List.of(gridRow(10, "VIIRS", 5), gridRow(10, "MODIS", 3),
                    gridRow(20, "VIIRS", 2), gridRow(20, "MODIS", 1))
                    : List.of(placeRow(10, "BR", "BRA", 7, 10), placeRow(20, "US", "USA", 8, 30));
            consumer.accept(rows);
            return (long) rows.size();
        }).when(source).stream(any(), any());
        var athena = mock(AthenaQuery.class);
        var queries = new AnnualCacheQuery(athena, jdbc, bootstrap);
        var detections = new DetectionRepository(queries);
        var cover = new Region.Cover(5, List.of(10L, 20L));
        var day = LocalDate.of(2026, 1, 1);
        var sensors = List.of("MODIS", "VIIRS");
        assertThrows(SourceUnavailableException.class, () -> detections.findDailyTotals(cover, day, day, sensors));
        verifyNoInteractions(athena);
        bootstrap.load(bootstrap.claim(2026));
        assertEquals("ready", bootstrap.status(2026).state());
        assertEquals(4326, jdbc.queryForObject("SELECT ST_SRID(geom) FROM annual_h3_places WHERE h3_cell=10", Integer.class));
        assertEquals("POINT(-50 -10)", jdbc.queryForObject("SELECT ST_AsText(geom) FROM annual_h3_places WHERE h3_cell=10", String.class));
        assertEquals(1, jdbc.queryForObject("""
                SELECT count(*) FROM annual_h3_places
                WHERE ST_Covers(ST_MakeEnvelope(-51, -11, -49, -9, 4326), geom)
                """, Integer.class));
        assertEquals(11, detections.findPeriodTotals(cover, day, day, sensors).stream().mapToLong(t -> t.count()).sum());
        assertEquals(4, detections.findCells(cover, day, day, sensors, 10).size());
        assertEquals(2, detections.findDailyTotals(cover, day, day, sensors).size());
        assertEquals(10, detections.findRanking(cover, day, day, sensors, false, 1).getFirst().cell());
        assertEquals(5.5, detections.findCellDistribution(cover, day, day, sensors, .5).percentileValue().doubleValue());
        var stats = detections.findSensorStats(cover, day, day, sensors);
        assertEquals(80.0, stats.stream().filter(s -> s.sensor().equals("MODIS")).findFirst().orElseThrow().confidenceMean().doubleValue());
        var country = new Region.Cover(5, List.of(), new Region.Place(Region.Kind.COUNTRY, "BRA", 0));
        assertEquals(8, detections.findPeriodTotals(country, day, day, sensors).stream().mapToLong(t -> t.count()).sum());
        var city = new Region.Cover(5, List.of(), new Region.Place(Region.Kind.CITY, "8", 25));
        assertTrue(detections.findDailyTotals(city, day, day, sensors).isEmpty());
        assertTrue(detections.findDailyTotals(cover, LocalDate.of(2026, 12, 31), LocalDate.of(2026, 12, 31), sensors).isEmpty());
        verifyNoInteractions(athena);

        doAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            assertTrue(sql.contains("year NOT IN (2026)"));
            Consumer<List<AthenaQuery.Row>> consumer = invocation.getArgument(1);
            consumer.accept(List.of(externalRow(20, "VIIRS", 20), externalRow(10, "MODIS", 1)));
            return 2L;
        }).when(athena).stream(anyString(), any());
        var start = LocalDate.of(2025, 12, 31);
        assertEquals(20, detections.findRanking(cover, start, day, sensors, false, 1).getFirst().cell());
        assertEquals(16.0, detections.findCellDistribution(cover, start, day, sensors, .5).percentileValue().doubleValue());
        assertEquals(32, detections.findPeriodTotals(cover, start, day, sensors).stream().mapToLong(t -> t.count()).sum());
        verify(athena, never()).run(anyString());
    }

    @Test
    void historicalFailureResumeAndBothRefreshesPreserveOtherYearsAndSharedPlaces() throws Exception {
        bootstrap = new BootstrapRepository(dataSource, jdbc, source, 6, 1);
        var current = partition("grid-2026", "GRID", 1);
        var places = partition("places", "PLACES", 1);
        var first = partition("s3://old-2025/grid-a", "GRID", 1);
        var second = partition("s3://old-2025/grid-b", "GRID", 1);
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(current, places)));
        when(source.plan(2025, false)).thenReturn(new AnnualCacheSource.Plan(1, -1, List.of(first, second)));
        AtomicBoolean fail = new AtomicBoolean(true);
        String indexes = "SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes"
                + " WHERE tablename IN ('annual_map_grid', 'annual_h3_places') AND schemaname=current_schema()";
        doAnswer(invocation -> {
            var part = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            if (part.kind().equals("PLACES")) {
                consumer.accept(List.<Object[]>of(placeRow(10, "BR", "BRA", 7, 10)));
            } else {
                int year = part.key().contains("2025") ? 2025 : 2026;
                consumer.accept(List.<Object[]>of(datedGridRow(year, part.equals(second) ? 20 : 10, "VIIRS", 3)));
                if (part.equals(second) && fail.getAndSet(false)) throw new IOException("historico interrompido");
            }
            return 1L;
        }).when(source).stream(any(), any());
        loadYear(2026);
        String publishedIndexes = jdbc.queryForObject(indexes, String.class);

        var lease = bootstrap.claim(2025);
        assertNotNull(lease);
        assertEquals(2025, lease.year());
        assertEquals("loading", bootstrap.status(2025).state());
        assertEquals("ready", bootstrap.status(2026).state());
        assertNull(new BootstrapRepository(dataSource, jdbc, source, 6, 1).claim(2024),
                "o mesmo lock serializa anos distintos");
        bootstrap.load(lease);
        assertEquals("failed", bootstrap.status(2025).state());
        assertEquals(1, bootstrap.status(2025).completedUnits());
        assertEquals(1, bootstrap.status(2025).rowsLoaded());
        assertEquals("ready", bootstrap.status(2026).state());
        assertEquals(2, bootstrap.status(2026).completedUnits());
        assertEquals(List.of(2026), bootstrap.readyYears(jdbc));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid WHERE year=2026", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM annual_h3_places", Integer.class));
        assertEquals(publishedIndexes, jdbc.queryForObject(indexes, String.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_cache_deferred_index", Integer.class));

        bootstrap = new BootstrapRepository(dataSource, jdbc, source, 6, 1);
        loadYear(2025);
        assertEquals("ready", bootstrap.status(2025).state());
        assertEquals(List.of(2026, 2025), bootstrap.readyYears(jdbc));
        assertEquals(3, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid", Integer.class));
        verify(source, times(1)).stream(eq(first), any());
        verify(source, times(2)).stream(eq(second), any());

        // Um snapshot historico novo so remove os arquivos do proprio ano.
        jdbc.update("UPDATE annual_grid_bootstrap SET checked_at=now()-interval '7 hours' WHERE year=2025");
        var replacement = partition("s3://new-2025/grid", "GRID", 1);
        doAnswer(invocation -> {
            AnnualCacheExports.Listener listener = invocation.getArgument(3);
            listener.snapshot("new-2025");
            assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid WHERE year=2025", Integer.class));
            assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid WHERE year=2026", Integer.class));
            assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM annual_h3_places", Integer.class));
            assertEquals("ready", bootstrap.status(2026).state());
            listener.exported(List.of(replacement));
            return new AnnualCacheSource.Plan(2, -1, List.of(replacement));
        }).when(source).plan(eq(2025), eq(false), any(), any());
        loadYear(2025);
        assertEquals(List.of(2026, 2025), bootstrap.readyYears(jdbc));
        assertEquals(1, bootstrap.status(2025).totalUnits());

        // A atualizacao de 2026 tambem nao despeja o historico publicado.
        jdbc.update("UPDATE annual_grid_bootstrap SET checked_at=now()-interval '7 hours' WHERE year=2026");
        var newCurrent = partition("grid-2026-new", "GRID", 1);
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(2, 1, List.of(newCurrent, places)));
        loadYear(2026);
        assertEquals(List.of(2026, 2025), bootstrap.readyYears(jdbc));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid WHERE year=2025", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM annual_h3_places", Integer.class));
        assertEquals("BR", jdbc.queryForObject("SELECT country_iso_a2 FROM annual_h3_places", String.class));
        assertEquals(publishedIndexes, jdbc.queryForObject(indexes, String.class));
        verify(source, times(1)).stream(eq(places), any());
    }

    @Test
    void readyYearsExcludePartialHistoryAndMixedRangesAggregateAllRowsBeforeRanking() throws Exception {
        bootstrap = new BootstrapRepository(dataSource, jdbc, source, 6, 1);
        var current = partition("grid-2026", "GRID", 2);
        var currentCoverage = new AnnualCacheSource.Partition("coverage-2026", "GRID", 0, 100, 2L, false);
        var historical = partition("grid-2025", "GRID", 2);
        var historicalCoverage = new AnnualCacheSource.Partition("coverage-2025", "GRID", 0, 100, 2L, false);
        var partial = new AnnualCacheSource.Partition("partial-2024", "GRID", 0, 100, 1L, false);
        var failed = partition("failed-2024", "GRID", 1);
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(current, currentCoverage)));
        when(source.plan(2025, false)).thenReturn(new AnnualCacheSource.Plan(1, -1, List.of(historical, historicalCoverage)));
        when(source.plan(2024, false)).thenReturn(new AnnualCacheSource.Plan(1, -1, List.of(partial, failed)));
        doAnswer(invocation -> {
            var part = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            if (part.equals(failed)) throw new IOException("particao historica ausente");
            int year = Integer.parseInt(part.key().substring(part.key().length() - 4));
            List<Object[]> rows = year == 2024 ? List.<Object[]>of(datedGridRow(year, 10, "VIIRS", 999))
                    : List.of(datedGridRow(year, 10, "VIIRS", year == 2026 ? 5 : 1),
                    datedGridRow(year, 20, "VIIRS", year == 2026 ? 2 : 20));
            consumer.accept(rows);
            return (long) rows.size();
        }).when(source).stream(any(), any());
        loadYear(2026);
        loadYear(2025);
        loadYear(2024);
        assertEquals("failed", bootstrap.status(2024).state());
        assertEquals(1, bootstrap.status(2024).completedUnits());
        assertEquals(List.of(2026, 2025), bootstrap.readyYears(jdbc));
        var athena = mock(AthenaQuery.class);
        var detections = new DetectionRepository(new AnnualCacheQuery(athena, jdbc, bootstrap));
        var cover = new Region.Cover(5, List.of(10L, 20L));
        var sensors = List.of("VIIRS");
        var end = LocalDate.of(2026, 1, 1);
        assertEquals(28, detections.findPeriodTotals(cover, LocalDate.of(2025, 1, 1), end, sensors)
                .stream().mapToLong(row -> row.count()).sum());
        assertEquals(20, detections.findRanking(cover, LocalDate.of(2025, 1, 1), end, sensors, false, 1).getFirst().cell());
        assertEquals(21, detections.findPeriodTotals(cover, LocalDate.of(2025, 1, 1),
                LocalDate.of(2025, 1, 1), sensors).getFirst().count());
        verifyNoInteractions(athena);
        doAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            assertTrue(sql.contains("year NOT IN (2026, 2025)"), sql);
            assertFalse(sql.contains("LIMIT"), "a fonte externa fornece linhas, nao um ranking ja limitado");
            Consumer<List<AthenaQuery.Row>> consumer = invocation.getArgument(1);
            consumer.accept(List.of(externalRow(2024, 10, "VIIRS", 3), externalRow(2024, 20, "VIIRS", 1)));
            return 2L;
        }).when(athena).stream(anyString(), any());
        var start = LocalDate.of(2024, 1, 1);
        assertEquals(32, detections.findPeriodTotals(cover, start, end, sensors)
                .stream().mapToLong(row -> row.count()).sum());
        assertEquals(20, detections.findRanking(cover, start, end, sensors, false, 1).getFirst().cell());
        assertEquals(16.0, detections.findCellDistribution(cover, start, end, sensors, .5).percentileValue().doubleValue());
        verify(athena, never()).run(anyString());

        when(athena.run(anyString())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            assertTrue(sql.contains("year NOT IN (2026, 2025)"), sql);
            var values = new LinkedHashMap<String, String>();
            values.put("sensor", "VIIRS");
            values.put("min_date", "2024-01-01");
            values.put("max_date", "2024-01-01");
            values.put("n", "4");
            return List.of(new AthenaQuery.Row(values));
        });
        var coverage = detections.findCoverage().getFirst();
        assertEquals(32, coverage.detections(), "cobertura nao inclui as 999 deteccoes da carga parcial");
        assertEquals(start, coverage.minDate());
        assertEquals(end, coverage.maxDate());
    }

    @Test
    void historicalPublicationDuringAnExternalQueryUsesTheSameReadyYearSnapshot() throws Exception {
        var current = partition("grid-2026", "GRID", 1);
        var historical = partition("grid-2025", "GRID", 1);
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(current)));
        when(source.plan(2025, false)).thenReturn(new AnnualCacheSource.Plan(1, -1, List.of(historical)));
        doAnswer(invocation -> {
            var part = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.<Object[]>of(datedGridRow(part.equals(current) ? 2026 : 2025, 10, "VIIRS", 3)));
            return 1L;
        }).when(source).stream(any(), any());
        loadYear(2026);
        var athena = mock(AthenaQuery.class);
        var detections = new DetectionRepository(new AnnualCacheQuery(athena, jdbc, bootstrap));
        doAnswer(invocation -> {
            loadYear(2025);
            Consumer<List<AthenaQuery.Row>> consumer = invocation.getArgument(1);
            consumer.accept(List.of(externalRow(2025, 10, "VIIRS", 3)));
            return 1L;
        }).when(athena).stream(anyString(), any());
        var cover = new Region.Cover(5, List.of(10L));
        assertEquals(6, detections.findPeriodTotals(cover, LocalDate.of(2025, 1, 1),
                LocalDate.of(2026, 1, 1), List.of("VIIRS")).getFirst().count());
        assertEquals(List.of(2026, 2025), bootstrap.readyYears(jdbc));
    }

    @Test
    void interruptedHistoricalLoadKeepsForegroundQueryableUntilEveryWriterExits() throws Exception {
        var current = partition("grid-2026", "GRID", 1);
        var historical = partition("grid-2025", "GRID", 1);
        when(source.plan(2026, true)).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(current)));
        when(source.plan(2025, false)).thenReturn(new AnnualCacheSource.Plan(1, -1, List.of(historical)));
        var entered = new CountDownLatch(1);
        var cancelled = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        AtomicBoolean retry = new AtomicBoolean();
        doAnswer(invocation -> {
            var part = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.<Object[]>of(datedGridRow(part.equals(current) ? 2026 : 2025, 10, "VIIRS", 3)));
            if (part.equals(historical) && !retry.get()) {
                entered.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    cancelled.countDown();
                    assertTrue(release.await(10, TimeUnit.SECONDS));
                    Thread.currentThread().interrupt();
                    throw new IOException("historico cancelado", e);
                }
            }
            return 1L;
        }).when(source).stream(any(), any());
        loadYear(2026);
        var executor = Executors.newSingleThreadExecutor();
        var lease = bootstrap.claim(2025);
        assertNotNull(lease);
        executor.submit(() -> bootstrap.load(lease));
        try {
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            executor.shutdownNow();
            assertTrue(cancelled.await(10, TimeUnit.SECONDS));
            assertEquals("loading", bootstrap.status(2025).state());
            assertEquals("ready", bootstrap.status(2026).state());
            assertNull(bootstrap.claim(2024));
            var athena = mock(AthenaQuery.class);
            var detections = new DetectionRepository(new AnnualCacheQuery(athena, jdbc, bootstrap));
            assertEquals(3, detections.findPeriodTotals(new Region.Cover(5, List.of(10L)),
                    LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 1), List.of("VIIRS")).getFirst().count());
            verifyNoInteractions(athena);
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertEquals("failed", bootstrap.status(2025).state());
        assertEquals("ready", bootstrap.status(2026).state());
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid WHERE year=2025", Integer.class));
        retry.set(true);
        loadYear(2025);
        assertEquals(List.of(2026, 2025), bootstrap.readyYears(jdbc));
    }

    @Test
    void absentEmptyAndDamagedYearPlansAreNeverMistakenForCompletePublishedData() throws Exception {
        assertEquals("idle", bootstrap.status(2025).state());
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM annual_grid_bootstrap", Integer.class),
                "consultar um ano ausente nao cria trabalho");
        when(source.plan(2025, false)).thenReturn(new AnnualCacheSource.Plan(1, -1, List.of()));
        loadYear(2025);
        assertEquals("failed", bootstrap.status(2025).state());
        assertTrue(bootstrap.readyYears(jdbc).isEmpty());
        var empty = new AnnualCacheSource.Partition("empty-2024", "GRID", 0, 0, 0L, true);
        when(source.plan(2024, false)).thenReturn(new AnnualCacheSource.Plan(1, -1, List.of(empty)));
        when(source.stream(eq(empty), any())).thenReturn(0L);
        loadYear(2024);
        assertEquals("ready", bootstrap.status(2024).state());
        assertEquals(0, bootstrap.status(2024).rowsLoaded());
        assertEquals(List.of(2024), bootstrap.readyYears(jdbc));
        jdbc.update("DELETE FROM annual_grid_partition WHERE year=2024");
        assertEquals("failed", bootstrap.status(2024).state());
        assertTrue(bootstrap.readyYears(jdbc).isEmpty());

        // O lock de outro ano nao ressuscita um marcador loading de um processo morto.
        jdbc.update("UPDATE annual_grid_bootstrap SET state='loading', loader_pid=NULL WHERE year=2026");
        var lease = bootstrap.claim(2023);
        assertNotNull(lease);
        try {
            // Reproduz um PID fisico reutilizado pelo pool, mas com o marcador de propriedade de outro ano.
            jdbc.update("UPDATE annual_grid_bootstrap SET loader_pid="
                    + "(SELECT loader_pid FROM annual_grid_bootstrap WHERE year=2023) WHERE year=2026");
            assertEquals("loading", bootstrap.status(2023).state());
            assertEquals("failed", bootstrap.status(2026).state());
        } finally {
            lease.close();
        }
        assertEquals("failed", bootstrap.status(2023).state());
    }

    private void loadYear(int year) {
        var lease = bootstrap.claim(year);
        assertNotNull(lease, "ano deve poder reivindicar a carga: " + year);
        bootstrap.load(lease);
    }

    private static AnnualCacheSource.Partition partition(String key, String kind, long count) {
        return new AnnualCacheSource.Partition(key, kind, 5, 100, count, false);
    }

    private static Object[] gridRow(long cell, String sensor, long count) {
        return new Object[]{Date.valueOf("2026-01-01"), sensor, cell, count, count * 2.0, 3.0,
                0L, sensor.equals("VIIRS") ? count : 0L, 0L, sensor.equals("MODIS") ? 80.0 : null};
    }

    private static Object[] datedGridRow(int year, long cell, String sensor, long count) {
        Object[] row = gridRow(cell, sensor, count);
        row[0] = Date.valueOf(LocalDate.of(year, 1, 1));
        return row;
    }

    private static Object[] placeRow(long cell, String iso2, String iso3, long city, double distance) {
        return new Object[]{cell, "South America", iso3, iso2, iso2, iso2, city, "City", iso2, distance,
                cell == 10 ? -10.0 : 40.0, cell == 10 ? -50.0 : -100.0};
    }

    private static AthenaQuery.Row externalRow(long cell, String sensor, long count) {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        values.put("resolution", "5");
        values.put("year", "2025");
        Object[] row = gridRow(cell, sensor, count);
        for (int i = 0; i < AnnualCacheSource.GRID_COLUMNS.size(); i++) {
            values.put(AnnualCacheSource.GRID_COLUMNS.get(i), row[i] == null ? null : row[i].toString());
        }
        values.put("day", "2025-12-31");
        return new AthenaQuery.Row(values);
    }

    private static AthenaQuery.Row externalRow(int year, long cell, String sensor, long count) {
        var values = new LinkedHashMap<String, String>();
        values.put("resolution", "5");
        values.put("year", Integer.toString(year));
        Object[] row = datedGridRow(year, cell, sensor, count);
        for (int i = 0; i < AnnualCacheSource.GRID_COLUMNS.size(); i++) {
            values.put(AnnualCacheSource.GRID_COLUMNS.get(i), row[i] == null ? null : row[i].toString());
        }
        return new AthenaQuery.Row(values);
    }
}
