package com.fireatlas.gold;

import com.fireatlas.gold.bootstrap.repository.BootstrapRepository;
import com.fireatlas.lake.integration.AnnualCacheSource;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class BootstrapRepositoryConfigurationTest {
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final AnnualCacheSource source = mock(AnnualCacheSource.class);

    @Test
    void workerCountIsBoundedFromOneToTwelve() {
        DataSource dataSource = mock(DataSource.class);
        assertThrows(IllegalArgumentException.class, () -> new BootstrapRepository(dataSource, jdbc, source, 6, 0));
        assertThrows(IllegalArgumentException.class, () -> new BootstrapRepository(dataSource, jdbc, source, 6, 13));
        assertDoesNotThrow(() -> new BootstrapRepository(dataSource, jdbc, source, 6, 1));
        assertDoesNotThrow(() -> new BootstrapRepository(dataSource, jdbc, source, 6, 12));
    }

    @Test
    void coordinatorProgressAndStatusConnectionsHaveHeadroomBeyondWorkers() {
        try (var pool = new HikariDataSource()) {
            pool.setMaximumPoolSize(5);
            assertThrows(IllegalArgumentException.class, () -> new BootstrapRepository(pool, jdbc, source, 6, 2));
            pool.setMaximumPoolSize(6);
            assertDoesNotThrow(() -> new BootstrapRepository(pool, jdbc, source, 6, 2));
            assertThrows(IllegalArgumentException.class, () -> new BootstrapRepository(pool, jdbc, source, 6, 4));
            pool.setMaximumPoolSize(16);
            assertDoesNotThrow(() -> new BootstrapRepository(pool, jdbc, source, 6, 12));
        }
    }
}
