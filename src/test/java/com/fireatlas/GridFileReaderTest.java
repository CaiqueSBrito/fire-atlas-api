package com.fireatlas;

import com.fireatlas.ingestion.integration.GridFileReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Leitura e validacao de um arquivo Parquet da gold, sem banco. */
class GridFileReaderTest {

    private static final long CELL3 = TestH3.cell(-10.5, -55.5, 3);
    private static final long CELL4 = TestH3.cell(-10.5, -55.5, 4);

    private static Path parquetOf(Path dir, List<GoldFixtures.Row> rows) throws Exception {
        var table = GoldFixtures.table(dir, "gold/map_grid/").append(rows);
        try (var files = Files.walk(table.tableDir())) {
            return files.filter(p -> p.toString().endsWith(".parquet")).findFirst().orElseThrow();
        }
    }

    @Test
    void validRowsGoToTheCsvAndEachRejectionIsCountedByReason(@TempDir Path dir) throws Exception {
        var rows = List.of(
                GoldFixtures.Row.viirs(3, CELL3, "2026-09-02", 4, 5.5),                    // ok
                GoldFixtures.Row.modis(3, CELL3, "2026-09-02", 2, 25.5),                   // ok
                new GoldFixtures.Row(3, CELL3, "2026-09-03", "FOO", 1, 1, 1, 0, 1, 0, null),   // sensor invalido
                GoldFixtures.Row.viirs(3, CELL4, "2026-09-03", 1, 5.5),                    // H3 de outra resolucao
                new GoldFixtures.Row(3, CELL3, "2026-09-04", "VIIRS", 0, 0, 0, 0, 0, 0, null), // sem deteccoes
                new GoldFixtures.Row(3, CELL3, "2026-09-05", "VIIRS", 2, -4.0, 2, 0, 2, 0, null)); // FRP negativo
        Path csv = dir.resolve("out.csv");
        var r = GridFileReader.extract(parquetOf(dir, rows), 3, "gold/map_grid/resolution=3/year=2026@v0",
                LocalDate.of(2026, 9, 1), LocalDate.of(2026, 9, 30), csv);

        assertEquals(6, r.scanned());
        assertEquals(2, r.loaded());
        assertEquals(4, r.rejected());
        assertEquals(1L, r.rejects().get("sensor_invalido"));
        assertEquals(1L, r.rejects().get("h3_invalido"));
        assertEquals(1L, r.rejects().get("deteccoes_invalidas"));
        assertEquals(1L, r.rejects().get("frp_negativo"));
        assertEquals(LocalDate.of(2026, 9, 2), r.minDay());
        assertEquals(LocalDate.of(2026, 9, 2), r.maxDay());

        List<String> lines = Files.readAllLines(csv);
        assertEquals(2, lines.size());
        assertTrue(lines.stream().allMatch(l -> l.startsWith("3," + CELL3 + ",2026-09-02,")), lines.toString());
        assertTrue(lines.stream().allMatch(l -> l.endsWith(",gold/map_grid/resolution=3/year=2026@v0")));
    }

    @Test
    void periodFilterOnlyReadsTheRequestedDays(@TempDir Path dir) throws Exception {
        var rows = List.of(
                GoldFixtures.Row.viirs(3, CELL3, "2026-09-01", 1, 5.5),
                GoldFixtures.Row.viirs(3, CELL3, "2026-09-10", 1, 5.5),
                GoldFixtures.Row.viirs(3, CELL3, "2026-09-20", 1, 5.5));
        var r = GridFileReader.extract(parquetOf(dir, rows), 3, "k", LocalDate.of(2026, 9, 5),
                LocalDate.of(2026, 9, 15), dir.resolve("o.csv"));
        assertEquals(1, r.scanned());
        assertEquals(1, r.loaded());
        assertEquals(LocalDate.of(2026, 9, 10), r.minDay());
    }

    @Test
    void zeroNegativeFrpFromTheSourceIsNotARejection(@TempDir Path dir) throws Exception {
        // a gold traz -0.0 como FRP somado quando nao ha energia medida: nao e FRP negativo
        var rows = List.of(new GoldFixtures.Row(3, CELL3, "2026-09-02", "MODIS", 1, -0.0, -0.0, 0, 0, 0, 80.0));
        var r = GridFileReader.extract(parquetOf(dir, rows), 3, "k", LocalDate.of(2026, 9, 1),
                LocalDate.of(2026, 9, 30), dir.resolve("o.csv"));
        assertEquals(1, r.loaded());
        assertEquals(0, r.rejected());
    }

    @Test
    void fileWithoutRequiredColumnsFailsWithTheMissingNames(@TempDir Path dir) throws Exception {
        Path bad = dir.resolve("bad.parquet");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("COPY (SELECT 1::BIGINT AS h3_cell, DATE '2026-09-02' AS day, 'VIIRS' AS sensor) TO '"
                    + bad.toString().replace("\\", "/") + "' (FORMAT parquet)");
        }
        var ex = assertThrows(IOException.class, () -> GridFileReader.extract(bad, 3, "k",
                LocalDate.of(2026, 9, 1), LocalDate.of(2026, 9, 30), dir.resolve("o.csv")));
        assertTrue(ex.getMessage().contains("detections"), ex.getMessage());
        assertTrue(ex.getMessage().contains("frp_sum_mw"), ex.getMessage());
    }

    @Test
    void corruptedFileFailsInsteadOfReturningRows(@TempDir Path dir) throws Exception {
        Path bad = dir.resolve("corrupt.parquet");
        Files.writeString(bad, "isto nao e um parquet");
        assertThrows(IOException.class, () -> GridFileReader.extract(bad, 3, "k", LocalDate.of(2026, 9, 1),
                LocalDate.of(2026, 9, 30), dir.resolve("o.csv")));
    }
}
