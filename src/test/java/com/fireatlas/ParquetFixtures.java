package com.fireatlas;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/** Somente para testes: fabrica arquivos Parquet no formato esperado da camada ouro (um foco por linha, com H3). */
final class ParquetFixtures {

    private ParquetFixtures() {}

    static String sql(Path p) {
        return p.toAbsolutePath().toString().replace("\\", "/");
    }

    /**
     * Converte o texto CSV em um arquivo Parquet. typesClause e a lista de tipos do DuckDB, ex.:
     * "{'acq_time': 'INTEGER', 'h3': 'BIGINT'}" ou null para inferir.
     */
    static Path parquet(Path dir, String name, String csvText, String typesClause) throws Exception {
        Path csv = Files.createTempFile(dir, "fixture-", ".csv");
        Files.writeString(csv, csvText);
        Path out = dir.resolve(name);
        Files.createDirectories(out.getParent());
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            String types = typesClause == null ? "" : ", types=" + typesClause;
            s.execute("COPY (SELECT * FROM read_csv('" + sql(csv) + "', header=true" + types + ")) TO '"
                    + sql(out) + "' (FORMAT PARQUET)");
        }
        Files.deleteIfExists(csv);
        return out;
    }
}
