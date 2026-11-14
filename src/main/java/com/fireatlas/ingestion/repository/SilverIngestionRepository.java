package com.fireatlas.ingestion.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Map;

/**
 * Persistencia da carga da prata na tabela {@code detection}. Cada {@link Session} e uma transacao: ou todas as
 * linhas de um arquivo da prata entram, ou nenhuma. A identidade do foco e o {@code record_id}: reimportar o mesmo
 * arquivo nao duplica (o indice unico por acq_date e record_id ignora o que ja existe) e as linhas que vieram do CSV
 * da FIRMS (record_id nulo) nao sao tocadas.
 */
@Repository
public class SilverIngestionRepository {

    private static final Logger log = LoggerFactory.getLogger(SilverIngestionRepository.class);

    static final String STAGING_DDL = """
            CREATE TEMP TABLE tmp_silver (
                sensor text, satellite text, acq_date date, acq_ts timestamptz,
                lat double precision, lon double precision, brightness real, bright_t31 real, scan real, track real,
                frp real, confidence_raw text, confidence_class char(1), daynight char(1), version text,
                source_product text, source_file text, source_line_number bigint, source_file_sha256 text,
                record_id text, h3_r6 bigint
            ) ON COMMIT DROP""";

    private static final String COLUMNS = "sensor, satellite, acq_date, acq_ts, lat, lon, brightness, bright_t31, "
            + "scan, track, frp, confidence_raw, confidence_class, daynight, version, source_product, source_file, "
            + "source_line_number, source_file_sha256, record_id, h3_r6";

    private final DataSource dataSource;
    private final ObjectMapper mapper;

    public SilverIngestionRepository(DataSource dataSource, ObjectMapper mapper) {
        this.dataSource = dataSource;
        this.mapper = mapper;
    }

    public Session begin() throws SQLException {
        return new Session();
    }

    /** Registra a falha em transacao propria (a do arquivo ja foi desfeita). Nunca mascara a falha original. */
    public void recordFailure(String sourceKey, String bbox, LocalDate start, LocalDate end, String reason) {
        try (Connection con = dataSource.getConnection();
             PreparedStatement ins = con.prepareStatement("""
                     INSERT INTO lake_import_log (source_key, product, sensor, bbox, period_start, period_end,
                                                  rows_scanned, rows_loaded, status, error)
                     VALUES (?, 'silver', 'MODIS,VIIRS', ?, ?, ?, 0, 0, 'FAILED', ?)""")) {
            ins.setString(1, sourceKey);
            ins.setString(2, bbox);
            ins.setObject(3, start);
            ins.setObject(4, end);
            ins.setString(5, reason);
            ins.executeUpdate();
        } catch (SQLException e) {
            log.error("Nao foi possivel registrar a falha da importacao da prata: {}", e.getClass().getSimpleName());
        }
    }

    /** Unidade de trabalho de um arquivo da prata (uma conexao, sem autocommit). */
    public final class Session implements AutoCloseable {

        private final Connection con;

        private Session() throws SQLException {
            this.con = dataSource.getConnection();
            this.con.setAutoCommit(false);
        }

        /** Carrega o CSV mapeado (SilverFileReader) em uma tabela temporaria desta transacao. */
        public long stage(Path csv) throws IOException, SQLException {
            try (Statement st = con.createStatement()) {
                st.execute(STAGING_DDL);
            }
            try (var reader = Files.newBufferedReader(csv, StandardCharsets.UTF_8)) {
                return con.unwrap(PGConnection.class).getCopyAPI().copyIn(
                        "COPY tmp_silver (" + COLUMNS + ") FROM STDIN WITH (FORMAT csv)", reader);
            }
        }

        /** Insere so o que ainda nao existe (por acq_date e record_id); devolve quantas linhas entraram. */
        public long insertNew() throws SQLException {
            try (Statement st = con.createStatement()) {
                return st.executeUpdate("INSERT INTO detection (" + COLUMNS + ") SELECT " + COLUMNS
                        + " FROM tmp_silver ON CONFLICT (acq_date, record_id) WHERE record_id IS NOT NULL "
                        + "DO NOTHING");
            }
        }

        /** Recalcula a grade diaria (daily_grid) do intervalo, como na carga do CSV. */
        public void refreshGrid(LocalDate from, LocalDate to) throws SQLException {
            SilverIngestionRepository.refreshGrid(con, from, to);
        }

        public void recordCompleted(String sourceKey, String bbox, LocalDate start, LocalDate end, long scanned,
                                    long inserted, LocalDate min, LocalDate max, long rejected,
                                    Map<String, Long> rejects, Long expected, boolean verified)
                throws SQLException, JsonProcessingException {
            try (PreparedStatement ins = con.prepareStatement("""
                    INSERT INTO lake_import_log (source_key, product, sensor, bbox, period_start, period_end,
                                                 rows_scanned, rows_loaded, min_date, max_date, rows_rejected,
                                                 rejects, status, expected_rows, verified)
                    VALUES (?, 'silver', 'MODIS,VIIRS', ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, 'COMPLETED', ?, ?)""")) {
                ins.setString(1, sourceKey);
                ins.setString(2, bbox);
                ins.setObject(3, start);
                ins.setObject(4, end);
                ins.setLong(5, scanned);
                ins.setLong(6, inserted);
                ins.setObject(7, min);
                ins.setObject(8, max);
                ins.setLong(9, rejected);
                ins.setString(10, mapper.writeValueAsString(rejects));
                ins.setObject(11, expected);
                ins.setBoolean(12, verified);
                ins.executeUpdate();
            }
        }

        public void commit() throws SQLException {
            con.commit();
        }

        public void rollback() {
            try {
                con.rollback();
            } catch (SQLException e) {
                log.error("Falha ao desfazer a transacao: {}", e.getClass().getSimpleName());
            }
        }

        @Override
        public void close() throws SQLException {
            con.close();
        }
    }

    /** Recalcula a grade diaria (0,1 grau) dos dias afetados a partir de detection, na mesma transacao. */
    static void refreshGrid(java.sql.Connection con, LocalDate from, LocalDate to) throws SQLException {
        try (PreparedStatement del = con.prepareStatement(
                "DELETE FROM daily_grid WHERE acq_date BETWEEN ? AND ?")) {
            del.setObject(1, from);
            del.setObject(2, to);
            del.executeUpdate();
        }
        try (PreparedStatement ins = con.prepareStatement("""
                INSERT INTO daily_grid (acq_date, sensor, cx, cy, n, frp_sum, frp_max)
                SELECT acq_date, sensor,
                       GREATEST(0, LEAST(floor((lon + 180) * 10)::int, 3599)),
                       GREATEST(0, LEAST(floor((lat + 90) * 10)::int, 1799)),
                       count(*), coalesce(sum(frp), 0), max(frp)
                FROM detection
                WHERE acq_date BETWEEN ? AND ?
                GROUP BY 1, 2, 3, 4""")) {
            ins.setObject(1, from);
            ins.setObject(2, to);
            ins.executeUpdate();
        }
    }
}
