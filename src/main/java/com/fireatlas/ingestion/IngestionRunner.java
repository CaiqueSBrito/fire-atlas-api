package com.fireatlas.ingestion;

import com.fireatlas.domain.BBox;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Carga do data lake (bronze/raw no S3) para o PostGIS, filtrando por regiao e periodo.
 *
 * <pre>
 * mvn spring-boot:run -Dspring-boot.run.profiles=ingest \
 *   -Dspring-boot.run.arguments="--ingest.start=2024-08-01 --ingest.end=2024-09-30 \
 *   --ingest.bbox=-74,-34,-34,6 --ingest.sensors=MODIS,VIIRS"
 * </pre>
 * Opcional: --ingest.products=MODIS_C6.1,VIIRS_S-NPP, --ingest.stream=nrt|archive  e
 * --ingest.file=caminho.csv --ingest.product=VIIRS_S-NPP
 * (carga de um CSV local, util como contingencia).
 */
@Component
@Profile("ingest")
public class IngestionRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(IngestionRunner.class);
    private static final Pattern KEY = Pattern.compile("^bronze/raw/([^/]+)/(\\d{4})/([^/]+\\.csv)$");

    private final DataSource dataSource;
    private final DatalakeClient lake;
    private final com.fasterxml.jackson.databind.ObjectMapper mapper;

    public IngestionRunner(DataSource dataSource, @org.springframework.context.annotation.Lazy DatalakeClient lake,
                           com.fasterxml.jackson.databind.ObjectMapper mapper) {
        this.dataSource = dataSource;
        this.lake = lake;
        this.mapper = mapper;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        LocalDate start = LocalDate.parse(required(args, "ingest.start"));
        LocalDate end = LocalDate.parse(required(args, "ingest.end"));
        if (end.isBefore(start)) throw new IllegalArgumentException("ingest.end < ingest.start");
        BBox bbox = BBox.parse(opt(args, "ingest.bbox", "-74,-34,-34,6")); // Brasil por padrao
        var filter = new DetectionCsvLoader.Filter(bbox, start, end);

        if (args.containsOption("ingest.file")) {
            String product = required(args, "ingest.product");
            Path p = Path.of(required(args, "ingest.file"));
            try (InputStream in = Files.newInputStream(p)) {
                loadOne(in, product, p.getFileName().toString(), "local:" + p.getFileName(), filter);
            }
            return;
        }

        List<String> sensors = List.of(opt(args, "ingest.sensors", "MODIS,VIIRS").toUpperCase(Locale.ROOT).split(","));
        List<String> products = args.containsOption("ingest.products")
                ? List.of(required(args, "ingest.products").split(",")) : null;

        String stream = args.containsOption("ingest.stream") ? required(args, "ingest.stream").toLowerCase(Locale.ROOT) : null;
        if (stream != null && !stream.equals("nrt") && !stream.equals("archive")) {
            throw new IllegalArgumentException("--ingest.stream deve ser nrt ou archive");
        }

        for (String key : lake.listCsvKeys("bronze/raw/")) {
            Matcher m = KEY.matcher(key);
            if (!m.matches()) continue;
            String product = m.group(1);
            int year = Integer.parseInt(m.group(2));
            String sensor = product.startsWith("MODIS") ? "MODIS" : "VIIRS";
            if (year < start.getYear() || year > end.getYear()) continue;
            if (!sensors.contains(sensor)) continue;
            if (products != null && !products.contains(product)) continue;
            if (stream != null && !m.group(3).startsWith("fire_" + stream)) continue;

            log.info("Carregando {} ...", key);
            try (ResponseInputStream<GetObjectResponse> in = lake.open(key)) {
                try {
                    loadOne(in, product, m.group(3), key, filter);
                } finally {
                    in.abort(); // evita baixar o restante do arquivo quando paramos antes do fim
                }
            }
        }
        lake.close();
    }

    private void loadOne(InputStream in, String product, String fileName, String sourceKey,
                         DetectionCsvLoader.Filter filter) throws IOException, SQLException {
        String sensor = product.startsWith("MODIS") ? "MODIS" : "VIIRS";
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 16);
             Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            try {
                String header = reader.readLine();
                if (header == null) throw new IOException("CSV vazio: " + sourceKey);
                var parser = new DetectionRowParser(header, sensor, product, sourceKey);

                try (PreparedStatement del = con.prepareStatement(
                        "DELETE FROM detection WHERE source_file = ? AND acq_date BETWEEN ? AND ?")) {
                    del.setString(1, sourceKey);
                    del.setObject(2, filter.start());
                    del.setObject(3, filter.end());
                    del.executeUpdate();
                }

                var stats = new DetectionCsvLoader().load(reader, parser, filter,
                        con.unwrap(PGConnection.class).getCopyAPI());

                try (PreparedStatement ins = con.prepareStatement("""
                        INSERT INTO ingestion_log (source_key, product, sensor, bbox, period_start, period_end,
                                                   rows_scanned, rows_loaded, min_date, max_date,
                                                   rows_rejected, rejects)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)""")) {
                    ins.setString(1, sourceKey);
                    ins.setString(2, product);
                    ins.setString(3, sensor);
                    ins.setString(4, filter.bbox().toString());
                    ins.setObject(5, filter.start());
                    ins.setObject(6, filter.end());
                    ins.setLong(7, stats.scanned());
                    ins.setLong(8, stats.kept());
                    ins.setObject(9, stats.minDate());
                    ins.setObject(10, stats.maxDate());
                    ins.setLong(11, stats.rejected());
                    ins.setString(12, mapper.writeValueAsString(stats.rejects()));
                    ins.executeUpdate();
                }
                // RN06: so motivo e uma linha de exemplo dos dados publicos da FIRMS; nenhuma credencial aqui.
                stats.rejectSamples().forEach((reason, sample) ->
                        log.warn("{}: {} linha(s) rejeitada(s) por {} (exemplo: {})", fileName,
                                stats.rejects().get(reason), reason, sample));
                if (stats.kept() > 0) H3Aggregates.refresh(con, stats.minDate(), stats.maxDate());
                con.commit();
                log.info("{}: {} linhas lidas, {} carregadas, {} rejeitadas{}", fileName, stats.scanned(),
                        stats.kept(), stats.rejected(), stats.stoppedEarly() ? " (parou apos o periodo)" : "");
            } catch (IOException | SQLException | RuntimeException e) {
                con.rollback();
                throw e;
            }
        }
    }

    private static String required(ApplicationArguments a, String name) {
        List<String> v = a.getOptionValues(name);
        if (v == null || v.isEmpty()) throw new IllegalArgumentException("argumento obrigatorio: --" + name);
        return v.get(0);
    }

    private static String opt(ApplicationArguments a, String name, String def) {
        List<String> v = a.getOptionValues(name);
        return v == null || v.isEmpty() ? def : v.get(0);
    }
}
