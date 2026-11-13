package com.fireatlas.ingestion;

import com.fireatlas.domain.BBox;
import com.fireatlas.domain.Detection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Map;
import java.util.TreeMap;

/** Le um CSV da FIRMS em streaming, valida, filtra por regiao/periodo e carrega no PostgreSQL via COPY. */
public class DetectionCsvLoader {

    private static final String COPY_SQL = "COPY detection (sensor, satellite, acq_date, acq_ts, lat, lon, "
            + "brightness, bright_t31, scan, track, frp, confidence_raw, confidence_class, daynight, version, "
            + "source_product, source_file, h3) FROM STDIN WITH (FORMAT csv, NULL '')";

    /** Quantas linhas seguidas depois do fim do periodo antes de assumir que o arquivo (ordenado) acabou. */
    private static final long EARLY_STOP_AFTER = 600_000;
    private static final int FLUSH_BYTES = 1 << 20;

    public record Filter(BBox bbox, LocalDate start, LocalDate end) {}

    /**
     * @param rejected       total de linhas rejeitadas por validacao (RN03), dentro da regiao/periodo pedidos
     * @param rejects        contagem por motivo de rejeicao
     * @param rejectSamples  primeira linha de cada motivo (para diagnostico em log)
     */
    public record Stats(long scanned, long kept, long rejected, Map<String, Long> rejects,
                        Map<String, String> rejectSamples, LocalDate minDate, LocalDate maxDate,
                        boolean stoppedEarly) {}

    public Stats load(BufferedReader reader, DetectionRowParser parser, Filter filter, CopyManager copyManager)
            throws IOException, SQLException {
        CopyIn copy = copyManager.copyIn(COPY_SQL);
        StringBuilder buf = new StringBuilder(FLUSH_BYTES + 4096);
        long scanned = 0, kept = 0, rejected = 0, pastEnd = 0;
        Map<String, Long> rejects = new TreeMap<>();
        Map<String, String> samples = new TreeMap<>();
        LocalDate min = null, max = null;
        boolean stoppedEarly = false;
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                scanned++;
                DetectionRowParser.Result res = parser.parse(line);
                if (!res.accepted()) {
                    // Linha invalida mas fora da regiao/periodo pedidos: nao e relevante, nao conta como rejeicao.
                    DetectionRowParser.Peek loc = parser.peekLocation(line);
                    if (loc != null) {
                        if (loc.date().isAfter(filter.end())) {
                            if (++pastEnd >= EARLY_STOP_AFTER) {
                                stoppedEarly = true;
                                break;
                            }
                            continue;
                        }
                        pastEnd = 0;
                        if (loc.date().isBefore(filter.start()) || !filter.bbox().contains(loc.lon(), loc.lat())) {
                            continue;
                        }
                    }
                    rejected++;
                    rejects.merge(res.rejectReason(), 1L, Long::sum);
                    samples.putIfAbsent(res.rejectReason(), line.length() > 200 ? line.substring(0, 200) : line);
                    continue;
                }
                Detection d = res.detection();
                LocalDate date = d.acqDate();
                if (date.isAfter(filter.end())) {
                    if (++pastEnd >= EARLY_STOP_AFTER) {
                        stoppedEarly = true;
                        break;
                    }
                    continue;
                }
                pastEnd = 0;
                if (date.isBefore(filter.start())
                        || !filter.bbox().contains(d.location().lon(), d.location().lat())) {
                    continue;
                }
                buf.append(d.toCopyLine()).append('\n');
                kept++;
                if (min == null || date.isBefore(min)) min = date;
                if (max == null || date.isAfter(max)) max = date;
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
        return new Stats(scanned, kept, rejected, rejects, samples, min, max, stoppedEarly);
    }
}
