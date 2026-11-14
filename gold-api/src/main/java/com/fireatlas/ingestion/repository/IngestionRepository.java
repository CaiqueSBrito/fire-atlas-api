package com.fireatlas.ingestion.repository;

import com.fireatlas.ingestion.entity.ImportLogEntry;
import com.fireatlas.ingestion.entity.ImportRecord;
import com.fireatlas.ingestion.integration.AvailabilityFileReader;
import com.fireatlas.ingestion.integration.ForecastFileReader;
import com.fireatlas.ingestion.integration.HarmonizedFileReader;
import com.fireatlas.ingestion.integration.GridFileReader;
import com.fireatlas.ingestion.integration.PlaceFileReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

/**
 * Persistencia da importacao: carga em lote da grade H3 (daily_h3) e registro (ingestion_log).
 * Uma {@link Session} e uma transacao: ou tudo que foi lido de uma particao entra, ou nada entra.
 */
@Repository
public class IngestionRepository {

    private static final Logger log = LoggerFactory.getLogger(IngestionRepository.class);

    private final DataSource dataSource;
    private final ObjectMapper mapper;

    public IngestionRepository(DataSource dataSource, ObjectMapper mapper) {
        this.dataSource = dataSource;
        this.mapper = mapper;
    }

    public Session begin() throws SQLException {
        return new Session();
    }

    /** Registra a falha em transacao propria (a do arquivo ja foi desfeita). Nunca mascara a falha original. */
    public void recordFailure(String sourceKey, String product, String sensor, String bbox, LocalDate start,
                              LocalDate end, String reason) {
        try (Connection con = dataSource.getConnection();
             PreparedStatement ins = con.prepareStatement("""
                     INSERT INTO ingestion_log (source_key, product, sensor, bbox, period_start, period_end,
                                                rows_scanned, rows_loaded, status, error)
                     VALUES (?, ?, ?, ?, ?, ?, 0, 0, 'FAILED', ?)""")) {
            ins.setString(1, sourceKey);
            ins.setString(2, product);
            ins.setString(3, sensor);
            ins.setString(4, bbox);
            ins.setObject(5, start);
            ins.setObject(6, end);
            ins.setString(7, reason);
            ins.executeUpdate();
        } catch (SQLException e) {
            log.error("Nao foi possivel registrar a falha da importacao: {}", e.getClass().getSimpleName());
        }
    }

    /** Importacoes registradas (concluidas e falhas), mais recentes primeiro. */
    public List<ImportRecord> findImports() {
        return new JdbcTemplate(dataSource).query("""
                SELECT product, sensor, source_key, bbox, period_start, period_end, status, error, rows_loaded,
                       rows_rejected, expected_rows, verified, source_sha256, min_date, max_date, loaded_at
                FROM ingestion_log ORDER BY loaded_at DESC, id DESC LIMIT 500
                """, (rs, i) -> {
            long expected = rs.getLong("expected_rows");
            Long expectedRows = rs.wasNull() ? null : expected;
            return new ImportRecord(rs.getString("product"), rs.getString("sensor"),
                    rs.getString("source_key"), rs.getString("bbox"),
                    rs.getObject("period_start", LocalDate.class), rs.getObject("period_end", LocalDate.class),
                    rs.getString("status"), rs.getString("error"), rs.getLong("rows_loaded"),
                    rs.getLong("rows_rejected"), expectedRows, rs.getBoolean("verified"),
                    rs.getString("source_sha256"), rs.getObject("min_date", LocalDate.class),
                    rs.getObject("max_date", LocalDate.class), rs.getTimestamp("loaded_at").toInstant());
        });
    }

    /** Unidade de trabalho de um arquivo (uma conexao, sem autocommit). */
    public final class Session implements AutoCloseable {

        private final Connection con;

        private Session() throws SQLException {
            this.con = dataSource.getConnection();
            this.con.setAutoCommit(false);
        }

        /** Remove a grade ja importada da resolucao no intervalo de datas (torna a importacao idempotente). */
        public long deleteGrid(int resolution, LocalDate start, LocalDate end) throws SQLException {
            try (PreparedStatement del = con.prepareStatement(
                    "DELETE FROM daily_h3 WHERE res = ? AND acq_date BETWEEN ? AND ?")) {
                del.setInt(1, resolution);
                del.setObject(2, start);
                del.setObject(3, end);
                return del.executeUpdate();
            }
        }

        /** Carga em lote (COPY) do CSV gerado por {@link GridFileReader}; devolve as linhas gravadas. */
        public long copyGrid(Path csv) throws IOException, SQLException {
            try (var reader = Files.newBufferedReader(csv, StandardCharsets.UTF_8)) {
                return con.unwrap(PGConnection.class).getCopyAPI().copyIn(
                        "COPY daily_h3 (" + GridFileReader.COPY_COLUMNS + ") FROM STDIN WITH (FORMAT csv)", reader);
            }
        }

        /** Remove os lugares da resolucao (a carga recarrega a resolucao inteira). */
        public long deletePlaces(int resolution) throws SQLException {
            try (PreparedStatement del = con.prepareStatement("DELETE FROM h3_place WHERE res = ?")) {
                del.setInt(1, resolution);
                return del.executeUpdate();
            }
        }

        public long copyPlaces(Path csv) throws IOException, SQLException {
            return copy(csv, "COPY h3_place (" + PlaceFileReader.COPY_COLUMNS + ") FROM STDIN WITH (FORMAT csv)");
        }

        /** Remove as emissoes de previsao que serao recarregadas. */
        public long deleteForecast(java.util.Collection<LocalDate> issueDates) throws SQLException {
            long total = 0;
            try (PreparedStatement del = con.prepareStatement("DELETE FROM forecast WHERE issue_date = ?")) {
                for (LocalDate d : issueDates) {
                    del.setObject(1, d);
                    total += del.executeUpdate();
                }
            }
            return total;
        }

        public long copyForecast(Path csv) throws IOException, SQLException {
            return copy(csv, "COPY forecast (" + ForecastFileReader.COPY_COLUMNS + ") FROM STDIN WITH (FORMAT csv)");
        }

        /** Remove a atividade harmonizada da resolucao no intervalo de datas (carga idempotente). */
        public long deleteHarmonized(int resolution, LocalDate start, LocalDate end) throws SQLException {
            try (PreparedStatement del = con.prepareStatement(
                    "DELETE FROM harmonized_cell WHERE res = ? AND day BETWEEN ? AND ?")) {
                del.setInt(1, resolution);
                del.setObject(2, start);
                del.setObject(3, end);
                return del.executeUpdate();
            }
        }

        public long copyHarmonized(Path csv) throws IOException, SQLException {
            return copy(csv, "COPY harmonized_cell (" + HarmonizedFileReader.COPY_COLUMNS
                    + ") FROM STDIN WITH (FORMAT csv)");
        }

        public long deleteAvailability(LocalDate start, LocalDate end) throws SQLException {
            try (PreparedStatement del = con.prepareStatement(
                    "DELETE FROM harmonized_availability WHERE day BETWEEN ? AND ?")) {
                del.setObject(1, start);
                del.setObject(2, end);
                return del.executeUpdate();
            }
        }

        public long copyAvailability(Path csv) throws IOException, SQLException {
            return copy(csv, "COPY harmonized_availability (" + AvailabilityFileReader.COPY_COLUMNS
                    + ") FROM STDIN WITH (FORMAT csv)");
        }

        private long copy(Path csv, String sql) throws IOException, SQLException {
            try (var reader = Files.newBufferedReader(csv, StandardCharsets.UTF_8)) {
                return con.unwrap(PGConnection.class).getCopyAPI().copyIn(sql, reader);
            }
        }

        public void recordCompleted(ImportLogEntry e) throws SQLException, JsonProcessingException {
            try (PreparedStatement ins = con.prepareStatement("""
                    INSERT INTO ingestion_log (source_key, product, sensor, bbox, period_start, period_end,
                                               rows_scanned, rows_loaded, min_date, max_date,
                                               rows_rejected, rejects, status, expected_rows, source_sha256,
                                               verified)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, 'COMPLETED', ?, ?, ?)""")) {
                ins.setString(1, e.sourceKey());
                ins.setString(2, e.product());
                ins.setString(3, e.sensor());
                ins.setString(4, e.bbox());
                ins.setObject(5, e.periodStart());
                ins.setObject(6, e.periodEnd());
                ins.setLong(7, e.rowsScanned());
                ins.setLong(8, e.rowsLoaded());
                ins.setObject(9, e.minDate());
                ins.setObject(10, e.maxDate());
                ins.setLong(11, e.rowsRejected());
                ins.setString(12, mapper.writeValueAsString(e.rejects()));
                ins.setObject(13, e.expectedRows());
                ins.setString(14, e.sourceSha256());
                ins.setBoolean(15, e.verified());
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
}
