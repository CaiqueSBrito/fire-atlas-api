package com.fireatlas.gold;

import com.fireatlas.gold.bootstrap.repository.AnnualCacheQuery;
import com.fireatlas.gold.bootstrap.repository.BootstrapRepository;
import com.fireatlas.gold.common.policy.SourceUnavailableException;
import com.fireatlas.gold.detection.repository.DetectionRepository;
import com.fireatlas.gold.region.entity.Region;
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
                new ClassPathResource("db/migration/V16__annual_athena_exports.sql")).execute(dataSource);
        source = mock(AnnualCacheSource.class);
        doAnswer(invocation -> source.plan()).when(source).plan(any());
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
        when(source.plan()).thenReturn(plan);
        AtomicBoolean fail = new AtomicBoolean(true);
        doAnswer(invocation -> {
            var partition = invocation.getArgument(0, AnnualCacheSource.Partition.class);
            Consumer<List<Object[]>> consumer = invocation.getArgument(1);
            consumer.accept(List.<Object[]>of(gridRow(partition.key().equals("grid-a") ? 10 : 20, "VIIRS", 3)));
            if (partition.key().equals("grid-b") && fail.getAndSet(false)) throw new IOException("falha de leitura simulada");
            return 1L;
        }).when(source).stream(any(), any());

        assertEquals("idle", bootstrap.status().state());
        var lease = bootstrap.claim();
        assertNotNull(lease);
        var restarted = new BootstrapRepository(dataSource, jdbc, source, 6, 1);
        assertNull(restarted.claim());
        assertEquals("loading", restarted.status().state());
        bootstrap.load(lease);
        assertEquals("failed", bootstrap.status().state());
        assertEquals(1, bootstrap.status().completedUnits());
        assertEquals(1, bootstrap.status().rowsLoaded());
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid_stage", Integer.class));

        restarted.load(restarted.claim());
        assertEquals("ready", restarted.status().state());
        assertEquals(2, restarted.status().completedUnits());
        assertEquals(2, restarted.status().rowsLoaded());
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid_stage", Integer.class));
        verify(source, times(1)).stream(eq(first), any());
        verify(source, times(2)).stream(eq(second), any());
        assertNull(restarted.claim());
        verify(source, times(2)).plan();

        // Simula processo morto: status GET nao escreve nem baixa; POST recupera o plano persistido.
        jdbc.update("UPDATE annual_grid_bootstrap SET state='loading'");
        assertEquals("failed", restarted.status().state());
        restarted.load(restarted.claim());
        assertEquals("ready", restarted.status().state());
        verify(source, times(1)).stream(eq(first), any());

        // Nova versao remove um arquivo e conserva outro; somente o arquivo novo e transferido.
        jdbc.update("UPDATE annual_grid_bootstrap SET checked_at=now()-interval '7 hours'");
        var replacement = partition("grid-c", "GRID", 1);
        when(source.plan()).thenReturn(new AnnualCacheSource.Plan(2, 1, List.of(first, replacement)));
        restarted.load(restarted.claim());
        assertEquals("ready", restarted.status().state());
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM annual_map_grid", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM annual_grid_partition WHERE source_key='grid-b'", Integer.class));
        verify(source, times(1)).stream(eq(first), any());
        verify(source, times(1)).stream(eq(replacement), any());
    }

    @Test
    void cachedFiltersStatisticsAndMixedYearRankingUseWholeDataset() throws Exception {
        var grid = partition("grid", "GRID", 4);
        var places = partition("places", "PLACES", 2);
        when(source.plan()).thenReturn(new AnnualCacheSource.Plan(1, 1, List.of(grid, places)));
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
        var cover = new Region.Cover(6, List.of(10L, 20L));
        var day = LocalDate.of(2026, 1, 1);
        var sensors = List.of("MODIS", "VIIRS");
        assertThrows(SourceUnavailableException.class, () -> detections.findDailyTotals(cover, day, day, sensors));
        verifyNoInteractions(athena);
        bootstrap.load(bootstrap.claim());
        assertEquals("ready", bootstrap.status().state());
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
        var country = new Region.Cover(6, List.of(), new Region.Place(Region.Kind.COUNTRY, "BRA", 0));
        assertEquals(8, detections.findPeriodTotals(country, day, day, sensors).stream().mapToLong(t -> t.count()).sum());
        var city = new Region.Cover(6, List.of(), new Region.Place(Region.Kind.CITY, "8", 25));
        assertTrue(detections.findDailyTotals(city, day, day, sensors).isEmpty());
        assertTrue(detections.findDailyTotals(cover, LocalDate.of(2026, 12, 31), LocalDate.of(2026, 12, 31), sensors).isEmpty());
        verifyNoInteractions(athena);

        doAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            assertTrue(sql.contains("year <> 2026"));
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

    private static AnnualCacheSource.Partition partition(String key, String kind, long count) {
        return new AnnualCacheSource.Partition(key, kind, 6, 100, count, false);
    }

    private static Object[] gridRow(long cell, String sensor, long count) {
        return new Object[]{Date.valueOf("2026-01-01"), sensor, cell, count, count * 2.0, 3.0,
                0L, sensor.equals("VIIRS") ? count : 0L, 0L, sensor.equals("MODIS") ? 80.0 : null};
    }

    private static Object[] placeRow(long cell, String iso2, String iso3, long city, double distance) {
        return new Object[]{cell, "South America", iso3, iso2, iso2, iso2, city, "City", iso2, distance,
                cell == 10 ? -10.0 : 40.0, cell == 10 ? -50.0 : -100.0};
    }

    private static AthenaQuery.Row externalRow(long cell, String sensor, long count) {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        values.put("resolution", "6");
        values.put("year", "2025");
        Object[] row = gridRow(cell, sensor, count);
        for (int i = 0; i < AnnualCacheSource.GRID_COLUMNS.size(); i++) {
            values.put(AnnualCacheSource.GRID_COLUMNS.get(i), row[i] == null ? null : row[i].toString());
        }
        values.put("day", "2025-12-31");
        return new AthenaQuery.Row(values);
    }
}
