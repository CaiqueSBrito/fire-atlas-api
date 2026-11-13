package com.fireatlas.ingestion;

import com.fireatlas.domain.BBox;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;

/** Le um CSV da FIRMS em streaming, filtra por regiao/periodo e carrega no PostgreSQL via COPY. */
public class DetectionCsvLoader {

    private static final String COPY_SQL = "COPY detection (sensor, satellite, acq_date, acq_ts, lat, lon, "
            + "brightness, bright_t31, scan, track, frp, confidence_raw, confidence_class, daynight, version, "
            + "source_product, source_file) FROM STDIN WITH (FORMAT csv, NULL '')";

    /** Quantas linhas seguidas depois do fim do periodo antes de assumir que o arquivo (ordenado) acabou. */
    private static final long EARLY_STOP_AFTER = 600_000;
    private static final int FLUSH_BYTES = 1 << 20;

    public record Filter(BBox bbox, LocalDate start, LocalDate end) {}

    public record Stats(long scanned, long kept, long malformed, LocalDate minDate, LocalDate maxDate,
                        boolean stoppedEarly) {}

    public Stats load(BufferedReader reader, DetectionRowParser parser, Filter filter, CopyManager copyManager)
            throws IOException, SQLException {
        CopyIn copy = copyManager.copyIn(COPY_SQL);
        StringBuilder buf = new StringBuilder(FLUSH_BYTES + 4096);
        long scanned = 0, kept = 0, malformed = 0, pastEnd = 0;
        LocalDate min = null, max = null;
        boolean stoppedEarly = false;
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                scanned++;
                DetectionRowParser.Row r = parser.parse(line);
                if (r == null) {
                    malformed++;
                    continue;
                }
                if (r.date().isAfter(filter.end())) {
                    if (++pastEnd >= EARLY_STOP_AFTER) {
                        stoppedEarly = true;
                        break;
                    }
                    continue;
                }
                pastEnd = 0;
                if (r.date().isBefore(filter.start()) || !filter.bbox().contains(r.lon(), r.lat())) continue;
                buf.append(r.csvLine()).append('\n');
                kept++;
                if (min == null || r.date().isBefore(min)) min = r.date();
                if (max == null || r.date().isAfter(max)) max = r.date();
                if (buf.length() >= FLUSH_BYTES) {
                    byte[] b = buf.toString().getBytes(StandardCharsets.UTF_8);
                    copy.writeToCopy(b, 0, b.length);
                    buf.setLength(0);
                }
            }
            if (buf.length() > 0) {
                byte[] b = buf.toString().getBytes(StandardCharsets.UTF_8);
                copy.writeToCopy(b, 0, b.length);
            }
            copy.endCopy();
        } catch (IOException | SQLException | RuntimeException e) {
            if (copy.isActive()) {
                try {
                    copy.cancelCopy();
                } catch (SQLException ignored) {
                    // a falha original e a relevante
                }
            }
            throw e;
        }
        return new Stats(scanned, kept, malformed, min, max, stoppedEarly);
    }
}
