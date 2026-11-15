package com.fireatlas.gold;

import com.fireatlas.gold.ingestion.integration.HarmonizedFileReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Leitura de gold/harmonized/cells com e sem as faixas de temperatura de brilho, sem banco. */
class HarmonizedFileReaderTest {

    private static final long CELL3 = TestH3.cell(-10.5, -55.5, 3);
    private static final LocalDate START = LocalDate.of(2026, 11, 1);
    private static final LocalDate END = LocalDate.of(2026, 11, 30);
    /** Posicao da primeira faixa no CSV: res, cell, day, harmonized, contributing e 15 colunas de satelite. */
    private static final int FIRST_BIN = 20;

    private static Path parquet(Path dir, String select) throws Exception {
        Path file = dir.resolve("cells.parquet");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("COPY (" + select + ") TO '" + file.toString().replace("\\", "/") + "' (FORMAT parquet)");
        }
        return file;
    }

    private static String row(String day, double harmonized, String bins, String basis) {
        return "SELECT CAST(" + CELL3 + " AS BIGINT) AS h3_cell, DATE '" + day + "' AS day, CAST(" + harmonized
                + " AS DOUBLE) AS harmonized, CAST(2 AS INTEGER) AS contributing" + bins + ", " + basis + " AS basis";
    }

    private static String bins(String... values) {
        String[] names = {"lt320", "320", "330", "340", "350", "ge360", "none"};
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            String n = names[i].matches("\\d+") ? "harmonized_t" + names[i] : "harmonized_t_" + names[i];
            sb.append(", CAST(").append(values[i]).append(" AS DOUBLE) AS ").append(n);
        }
        return sb.toString();
    }

    @Test
    void binsAndBasisGoToTheCsvAndANegativeBinRejectsTheRow(@TempDir Path dir) throws Exception {
        String select = row("2026-11-13", 10.0, bins("1", "2", "NULL", "3", "1.5", "2", "0.5"), "'viirs'")
                + " UNION ALL " + row("2026-11-14", 4.0, bins("1", "-1", "1", "1", "1", "1", "0"), "'aqua'")
                + " UNION ALL " + row("2026-11-15", 4.0, bins("1", "'nan'", "1", "1", "1", "0", "0"), "'aqua'");
        Path csv = dir.resolve("out.csv");
        var r = HarmonizedFileReader.extract(parquet(dir, select), 3, "k", START, END, csv);

        assertEquals(3, r.scanned());
        assertEquals(1, r.loaded());
        assertEquals(2L, r.rejects().get("temperatura_invalida"));
        List<String> cols = List.of(Files.readAllLines(csv).get(0).split(",", -1));
        assertEquals(List.of("1.0", "2.0", "", "3.0", "1.5", "2.0", "0.5", "viirs", "k"),
                cols.subList(FIRST_BIN, cols.size()));
    }

    @Test
    void oldFilesWithoutBinsStillLoadWithNulls(@TempDir Path dir) throws Exception {
        String select = "SELECT CAST(" + CELL3 + " AS BIGINT) AS h3_cell, DATE '2026-11-13' AS day, "
                + "CAST(10.0 AS DOUBLE) AS harmonized, CAST(2 AS INTEGER) AS contributing";
        Path csv = dir.resolve("out.csv");
        var r = HarmonizedFileReader.extract(parquet(dir, select), 3, "k", START, END, csv);

        assertEquals(1, r.loaded());
        assertTrue(r.rejects().isEmpty());
        List<String> cols = List.of(Files.readAllLines(csv).get(0).split(",", -1));
        assertEquals(List.of("", "", "", "", "", "", "", "", "k"), cols.subList(FIRST_BIN, cols.size()));
        assertEquals(HarmonizedFileReader.COPY_COLUMNS.split(",").length, cols.size());
    }
}
