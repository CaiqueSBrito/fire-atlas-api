package com.fireatlas.ingestion.integration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Le um arquivo Parquet (camada ouro do data lake: um registro por foco, com o H3 na linha) e o entrega como CSV
 * (cabecalho + linhas), para o mesmo parser dos CSVs brutos. Nao assume nomes de colunas: o cabecalho sai com os
 * nomes do proprio arquivo. O filtro de data e regiao e aplicado na leitura (so as linhas pedidas sao lidas).
 * Usa DuckDB embutido; nada e convertido (o H3 ja vem no arquivo).
 */
public final class ParquetCsvReader {

    /** Rodape do arquivo (numero de linhas) e periodo coberto por acq_date (nulo se a coluna nao existir). */
    public record Info(long rows, LocalDate minDate, LocalDate maxDate) {}

    private static final int CHUNK_ROWS = 2000;

    private ParquetCsvReader() {}

    private static String literal(Path file) {
        return "'" + file.toAbsolutePath().toString().replace("\\", "/").replace("'", "''") + "'";
    }

    /** Metadados do rodape e periodo coberto. Lanca IOException se o arquivo nao for um Parquet valido. */
    public static Info info(Path file) throws IOException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            long rows;
            try (ResultSet rs = s.executeQuery("SELECT num_rows FROM parquet_file_metadata(" + literal(file) + ")")) {
                if (!rs.next()) throw new IOException("Parquet sem metadados: " + file.getFileName());
                rows = rs.getLong(1);
            }
            LocalDate min = null;
            LocalDate max = null;
            if (columns(c, file).contains("acq_date")) {
                try (ResultSet rs = s.executeQuery("SELECT min(CAST(acq_date AS DATE)), max(CAST(acq_date AS DATE)) "
                        + "FROM read_parquet(" + literal(file) + ")")) {
                    if (rs.next() && rs.getString(1) != null) {
                        min = LocalDate.parse(rs.getString(1));
                        max = LocalDate.parse(rs.getString(2));
                    }
                }
            }
            return new Info(rows, min, max);
        } catch (SQLException e) {
            throw new IOException("Parquet invalido ou ilegivel: " + e.getMessage(), e);
        }
    }

    /**
     * Abre o Parquet como fluxo CSV. Se o arquivo tiver as colunas acq_date, latitude e longitude, o filtro de
     * periodo e regiao e aplicado na propria leitura. O chamador deve fechar o fluxo.
     */
    public static InputStream openAsCsv(Path file, DetectionCsvLoader.Filter filter) throws IOException {
        Connection c = null;
        Statement s = null;
        ResultSet rs = null;
        try {
            c = DriverManager.getConnection("jdbc:duckdb:");
            Set<String> cols = columns(c, file);
            StringBuilder where = new StringBuilder();
            if (filter != null) {
                if (cols.contains("acq_date")) {
                    where.append("CAST(acq_date AS DATE) BETWEEN DATE '").append(filter.start())
                            .append("' AND DATE '").append(filter.end()).append("'");
                }
                if (cols.contains("latitude") && cols.contains("longitude")) {
                    if (where.length() > 0) where.append(" AND ");
                    var b = filter.bbox();
                    where.append("latitude BETWEEN ").append(b.minLat()).append(" AND ").append(b.maxLat())
                            .append(" AND longitude BETWEEN ").append(b.minLon()).append(" AND ").append(b.maxLon());
                }
            }
            String sql = "SELECT * FROM read_parquet(" + literal(file) + ")"
                    + (where.length() > 0 ? " WHERE " + where : "");
            s = c.createStatement();
            rs = s.executeQuery(sql);
            return new CsvStream(c, s, rs);
        } catch (SQLException e) {
            closeQuietly(rs, s, c);
            throw new IOException("Parquet invalido ou ilegivel: " + e.getMessage(), e);
        }
    }

    /** Nomes das colunas do arquivo, em minusculas. */
    private static Set<String> columns(Connection c, Path file) throws SQLException {
        Set<String> names = new LinkedHashSet<>();
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM read_parquet(" + literal(file) + ") LIMIT 0")) {
            ResultSetMetaData md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) names.add(md.getColumnName(i).toLowerCase(Locale.ROOT));
        }
        return names;
    }

    private static void closeQuietly(AutoCloseable... resources) {
        for (AutoCloseable r : resources) {
            if (r == null) continue;
            try {
                r.close();
            } catch (Exception ignored) {
                // fechamento: nada a fazer
            }
        }
    }

    /** Fluxo CSV sob demanda: gera o cabecalho e depois as linhas em blocos, sem carregar o arquivo na memoria. */
    private static final class CsvStream extends InputStream {

        private final Connection connection;
        private final Statement statement;
        private final ResultSet rows;
        private final String[] names;
        private byte[] buffer = new byte[0];
        private int pos;
        private boolean headerDone;
        private boolean finished;
        private boolean closed;

        CsvStream(Connection connection, Statement statement, ResultSet rows) throws SQLException {
            this.connection = connection;
            this.statement = statement;
            this.rows = rows;
            ResultSetMetaData md = rows.getMetaData();
            this.names = new String[md.getColumnCount()];
            for (int i = 0; i < names.length; i++) names[i] = clean(md.getColumnName(i + 1));
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] out, int off, int len) throws IOException {
            if (closed) throw new IOException("fluxo fechado");
            while (pos >= buffer.length) {
                if (finished) return -1;
                fill();
            }
            int n = Math.min(len, buffer.length - pos);
            System.arraycopy(buffer, pos, out, off, n);
            pos += n;
            return n;
        }

        private void fill() throws IOException {
            StringBuilder sb = new StringBuilder(1 << 16);
            if (!headerDone) {
                sb.append(String.join(",", names)).append('\n');
                headerDone = true;
            }
            try {
                int count = 0;
                while (count < CHUNK_ROWS && rows.next()) {
                    for (int i = 1; i <= names.length; i++) {
                        if (i > 1) sb.append(',');
                        String v = rows.getString(i);
                        if (v != null) sb.append(clean(v));
                    }
                    sb.append('\n');
                    count++;
                }
                if (count < CHUNK_ROWS) finished = true;
            } catch (SQLException e) {
                throw new IOException("Falha ao ler o Parquet: " + e.getMessage(), e);
            }
            buffer = sb.toString().getBytes(StandardCharsets.UTF_8);
            pos = 0;
        }

        /** O CSV de saida nao usa aspas: virgulas e quebras de linha dentro de um valor viram espaco. */
        private static String clean(String v) {
            return v.replace(',', ' ').replace('\n', ' ').replace('\r', ' ');
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            closeQuietly(rows, statement, connection);
        }
    }
}
