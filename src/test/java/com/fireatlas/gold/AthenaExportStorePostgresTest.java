package com.fireatlas.gold;

import com.fireatlas.lake.integration.AthenaExportStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(DbAvailable.class)
class AthenaExportStorePostgresTest {
    private JdbcTemplate admin;
    private JdbcTemplate jdbc;
    private String schema;

    @BeforeEach
    void createSchema() {
        admin = new JdbcTemplate(new DriverManagerDataSource(DbAvailable.URL, DbAvailable.USER, DbAvailable.PASSWORD));
        schema = "export_test_" + UUID.randomUUID().toString().replace("-", "");
        admin.execute("CREATE SCHEMA " + schema);
        var dataSource = new DriverManagerDataSource(DbAvailable.URL, DbAvailable.USER, DbAvailable.PASSWORD);
        Properties properties = new Properties();
        properties.setProperty("currentSchema", schema + ",public");
        dataSource.setConnectionProperties(properties);
        jdbc = new JdbcTemplate(dataSource);
        new ResourceDatabasePopulator(new ClassPathResource("db/migration/V14__annual_grid_cache.sql"),
                new ClassPathResource("db/migration/V15__annual_cache_postgis.sql"),
                new ClassPathResource("db/migration/V16__annual_athena_exports.sql")).execute(dataSource);
    }

    @AfterEach
    void dropSchema() {
        if (schema != null) admin.execute("DROP SCHEMA " + schema + " CASCADE");
    }

    @Test
    void pendingTokensRunningQueriesAndCompletedManifestsSurviveNewRepositoryInstances() throws Exception {
        var key = new AthenaExportStore.Key("snapshot", "GRID", 6);
        var store = new AthenaExportStore(jdbc);
        var pending = store.begin(key, "token-1", "s3://results/export/attempt-1/");
        assertEquals("pending", new AthenaExportStore(jdbc).find(key).state());
        assertEquals(pending, store.begin(key, "must-not-replace", "s3://results/unused/"));
        store.started(pending, "query-1");
        var running = new AthenaExportStore(jdbc).find(key);
        assertEquals("query-1", running.queryId());
        assertEquals("running", running.state());
        assertEquals(running, store.begin(key, "must-not-replace", "s3://results/unused/"));
        List<String> files = List.of(pending.prefix() + "part-1", pending.prefix() + "part-2");
        store.complete(running, "s3://results/query-1-manifest.csv", files);
        var completed = new AthenaExportStore(jdbc).find(key);
        assertEquals("complete", completed.state());
        assertEquals(files, completed.files());
        assertEquals("query-1", completed.queryId());
        assertEquals(completed, store.begin(key, "must-not-replace", "s3://results/unused/"));
        store.failed(running);
        assertEquals(completed, new AthenaExportStore(jdbc).find(key));
        assertThrows(UnsupportedOperationException.class, () -> completed.files().clear());
    }

    @Test
    void onlyTerminalFailureReplacesAttemptAndDoesNotTouchOtherCompletedPartitions() throws Exception {
        var store = new AthenaExportStore(jdbc);
        var first = store.begin(new AthenaExportStore.Key("snapshot", "GRID", 0), "token-0", "s3://results/a/");
        store.started(first, "query-0");
        store.complete(first, "s3://results/query-0-manifest.csv", List.of(first.prefix() + "part"));
        var second = store.begin(new AthenaExportStore.Key("snapshot", "GRID", 1), "token-1", "s3://results/b/");
        store.started(second, "query-1");
        store.failed(second);
        var retry = new AthenaExportStore(jdbc).begin(second.key(), "token-2", "s3://results/c/");
        assertEquals("token-2", retry.token());
        assertEquals("s3://results/c/", retry.prefix());
        assertEquals("pending", retry.state());
        assertNull(retry.queryId());
        assertEquals(List.of(), retry.files());
        assertEquals("complete", store.find(first.key()).state());
        assertEquals(List.of(first.prefix() + "part"), store.find(first.key()).files());
    }
}
