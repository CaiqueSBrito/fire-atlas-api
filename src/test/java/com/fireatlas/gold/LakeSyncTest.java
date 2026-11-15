package com.fireatlas.gold;

import com.fireatlas.gold.ingestion.repository.IngestionRepository;
import com.fireatlas.gold.ingestion.service.LakeSyncService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Sincronizacao automatica da gold: carga inicial, nada a fazer sem versao nova e recarga so do fim do periodo. */
@ExtendWith(DbAvailable.class)
@SpringBootTest
class LakeSyncTest {

    static final String SCHEMA = "it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    static final String GRID = "gold/map_grid/";

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", () -> DbAvailable.URL + "?currentSchema=" + SCHEMA + ",public");
        r.add("spring.datasource.username", () -> DbAvailable.USER);
        r.add("spring.datasource.password", () -> DbAvailable.PASSWORD);
        r.add("spring.flyway.schemas", () -> SCHEMA + ",public");
        r.add("spring.flyway.default-schema", () -> SCHEMA);
        r.add("spring.flyway.create-schemas", () -> "true");
    }

    @AfterAll
    static void dropSchema() throws Exception {
        try (Connection c = DriverManager.getConnection(DbAvailable.URL, DbAvailable.USER, DbAvailable.PASSWORD);
             Statement s = c.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
        }
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired IngestionRepository repository;

    private Map<String, LakeSyncService.Status> run(Path lake, String today) {
        List<LakeSyncService.Result> results = new LakeSyncService(repository, new GoldFixtures.LocalStorage(lake),
                LocalDate.parse("2026-09-01"), 3).run(LocalDate.parse(today));
        return results.stream().collect(Collectors.toMap(LakeSyncService.Result::table, LakeSyncService.Result::status));
    }

    private int completedGridImports() {
        return jdbc.queryForObject("SELECT count(*) FROM lake_import_log WHERE product = 'map_grid' "
                + "AND status = 'COMPLETED'", Integer.class);
    }

    @Test
    void carga_inicial_depois_so_versao_nova_e_so_o_fim_do_periodo(@TempDir Path dir) throws Exception {
        long cell = TestH3.cell(-10.5, -55.5, 3);
        var grid = GoldFixtures.table(dir, GRID).append(List.of(GoldFixtures.Row.viirs(3, cell, "2026-09-02", 2, 5.5)));

        var first = run(dir, "2026-09-10");
        assertEquals(LakeSyncService.Status.IMPORTED, first.get(GRID));
        // as demais tabelas nao existem neste lake: falham sem impedir a grade e ficam sem estado (tentadas de novo)
        assertEquals(LakeSyncService.Status.FAILED, first.get("gold/h3_places/"));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM lake_sync_state WHERE table_path <> ?",
                Integer.class, GRID));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM daily_h3", Integer.class));
        assertEquals(Map.of("version", 0L, "synced_through", java.sql.Date.valueOf("2026-09-10")), jdbc.queryForMap(
                "SELECT version, synced_through FROM lake_sync_state WHERE table_path = ?", GRID));

        assertEquals(LakeSyncService.Status.UNCHANGED, run(dir, "2026-09-11").get(GRID));
        assertEquals(1, completedGridImports());

        grid.append(List.of(GoldFixtures.Row.viirs(3, cell, "2026-09-09", 4, 5.5)));
        assertEquals(LakeSyncService.Status.IMPORTED, run(dir, "2026-09-12").get(GRID));
        assertEquals(2, completedGridImports());
        assertEquals(LocalDate.parse("2026-09-07"), jdbc.queryForObject("SELECT period_start FROM lake_import_log "
                + "WHERE product = 'map_grid' AND status = 'COMPLETED' ORDER BY id DESC LIMIT 1", LocalDate.class));
        assertEquals(6L, jdbc.queryForObject("SELECT sum(n) FROM daily_h3", Long.class));
        assertEquals(1L, jdbc.queryForObject("SELECT version FROM lake_sync_state WHERE table_path = ?", Long.class,
                GRID));
    }
}
