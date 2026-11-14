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
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.HexFormat;
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

    public IngestionRunner(DataSource dataSource, @org.springframework.context.annotation.Lazy DatalakeClient lake) {
        this.dataSource = dataSource;
        this.lake = lake;
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
                } catch (IOException | SQLException | RuntimeException e) {
                    in.abort(); // cancela download restante somente quando o processamento falha/aborta
                    throw e;
                }
            }
        }
        lake.close();
    }

    /** Sinaliza interrupcao ou leitura incompleta de CSV antes do EOF, impedindo exclusoes destrutivas. */
    public static final class IncompleteSourceReadException extends IOException {
        public IncompleteSourceReadException(String message) {
            super(message);
        }
    }

    public record IngestResult(
            DetectionCsvLoader.Stats stats,
            String sourceSha256,
            long rowsInserted,
            long rowsUnchanged,
            long rowsDeleted
    ) {}

    private void loadOne(InputStream in, String product, String fileName, String sourceKey,
                         DetectionCsvLoader.Filter filter) throws IOException, SQLException {
        IngestResult res = ingestStream(dataSource, in, product, sourceKey, filter);
        log.info("{}: {} lidas, {} no recorte ({} novas, {} preservadas, {} removidas), {} malformadas{}",
                fileName, res.stats().scanned(), res.stats().kept(),
                res.rowsInserted(), res.rowsUnchanged(), res.rowsDeleted(),
                res.stats().malformed(), res.stats().stoppedEarly() ? " (parou apos o periodo)" : "");
    }

    /**
     * Executa ingestao transacional com staging via COPY, reconciliacao estrita dentro de
     * (source_file, [start..end], bbox) preservando identificadores de registros identicos e regioes
     * externas ao bbox, seguida de atualizacao incremental da daily_grid apenas nas celulas afetadas.
     */
    public static IngestResult ingestStream(
            DataSource dataSource, InputStream in, String product, String sourceKey,
            DetectionCsvLoader.Filter filter) throws IOException, SQLException {
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            try {
                IngestResult result = ingestOnConnection(con, in, product, sourceKey, filter);
                con.commit();
                return result;
            } catch (IOException | SQLException | RuntimeException e) {
                try {
                    con.rollback();
                } catch (SQLException ignored) {
                    // preserva excecao original
                }
                throw e;
            }
        }
    }

    private static final String SEMANTIC_HASH_VERSION = DetectionRowParser.SEMANTIC_HASH_VERSION;
    private static final String STATUS_CURRENT = "CURRENT";

    /**
     * Predicado autoritativo de correspondencia para reconciliacao (exige exatamente 1 parametro ? = sourceSha256):
     * - Caminho 1: registros ja certificados sob o algoritmo atual (v2-ieee754-direct) casam por assinatura semantica;
     * - Caminho 2: registros historicos pre-V6 (LEGACY_UNVERIFIED) ou V3 completos sem hash semantico casam apenas quando
     *   origem, linha, hash bruto da linha, checksum integral do CSV (incluindo cabecalho) e todos os 15 valores persistidos coincidem.
     */
    private static final String RECONCILIATION_MATCH_PREDICATE = """
            d.acq_date = s.acq_date
            AND d.source_file = s.source_file
            AND d.source_line_number = s.source_line_number
            AND (
                (
                    d.source_semantic_hash_version = 'v2-ieee754-direct'
                    AND d.source_semantic_hash IS NOT NULL
                    AND d.source_semantic_hash = s.source_semantic_hash
                )
                OR (
                    d.source_semantic_hash IS NULL
                    AND (d.source_semantic_hash_version = 'v1-legacy-unverified' OR d.source_semantic_hash_version IS NULL)
                    AND d.source_row_hash IS NOT NULL
                    AND d.source_row_hash = s.source_row_hash
                    AND d.source_file_sha256 IS NOT NULL
                    AND d.source_file_sha256 = ?
                )
            )
            AND d.sensor = s.sensor
            AND d.satellite = s.satellite
            AND d.acq_ts = s.acq_ts
            AND d.lat = s.lat
            AND d.lon = s.lon
            AND d.brightness IS NOT DISTINCT FROM s.brightness
            AND d.bright_t31 IS NOT DISTINCT FROM s.bright_t31
            AND d.scan IS NOT DISTINCT FROM s.scan
            AND d.track IS NOT DISTINCT FROM s.track
            AND d.frp IS NOT DISTINCT FROM s.frp
            AND d.confidence_raw IS NOT DISTINCT FROM s.confidence_raw
            AND d.confidence_class IS NOT DISTINCT FROM s.confidence_class
            AND d.daynight IS NOT DISTINCT FROM s.daynight
            AND d.version IS NOT DISTINCT FROM s.version
            AND d.source_product = s.source_product
            """;

    /**
     * Contrato transacional suportado: exatamente UMA origem (`sourceKey`) por transacao explicita
     * (`autoCommit=false`). A ordem global de bloqueios e estrita: Fase 1 (advisory lock da origem)
     * -> Fase 2 (advisory locks ordenados por celula `daily_grid`), eliminando deadlocks entre cargas
     * concorrentes da mesma origem ou de origens distintas em celulas compartilhadas.
     * Chamadas adicionais na mesma transacao (ou `autoCommit=true`) sao rejeitadas antes de alterar dados.
     */
    public static IngestResult ingestOnConnection(
            Connection con, InputStream in, String product, String sourceKey,
            DetectionCsvLoader.Filter filter) throws IOException, SQLException {
        return ingestOnConnection(con, in, product, sourceKey, filter, DetectionCsvLoader.NO_EARLY_STOP);
    }

    /** Variante parametrizada de {@link #ingestOnConnection(Connection, InputStream, String, String, DetectionCsvLoader.Filter)} para validar aborto fail-safe em leitura interrompida. */
    public static IngestResult ingestOnConnection(
            Connection con, InputStream in, String product, String sourceKey,
            DetectionCsvLoader.Filter filter, long maxConsecutivePastEnd) throws IOException, SQLException {
        return ingestOnConnection(con, in, product, sourceKey, filter, maxConsecutivePastEnd, null);
    }

    /** Variante de pacote com gancho opcional antes da recomputacao da grade para testes de atomicidade sob falha. */
    static IngestResult ingestOnConnection(
            Connection con, InputStream in, String product, String sourceKey,
            DetectionCsvLoader.Filter filter, long maxConsecutivePastEnd,
            Runnable beforeGridRefresh) throws IOException, SQLException {
        if (sourceKey == null || sourceKey.isBlank()) {
            throw new IllegalArgumentException("sourceKey obrigatorio para ingestao");
        }
        if (con.getAutoCommit()) {
            throw new IllegalStateException(
                    "Contrato transacional violado: ingestOnConnection requer autoCommit=false (uma origem por transacao).");
        }
        try (Statement txRead = con.createStatement();
             ResultSet rs = txRead.executeQuery("SELECT NULLIF(current_setting('fireatlas.ingest_tx_source', true), '')")) {
            if (rs.next()) {
                String existing = rs.getString(1);
                if (existing != null) {
                    if (!existing.equals(sourceKey)) {
                        throw new IllegalStateException(
                                "Contrato transacional violado: multiplas origens na mesma transacao nao sao suportadas "
                                        + "(atual='" + existing + "', solicitada='" + sourceKey + "').");
                    }
                    throw new IllegalStateException(
                            "Contrato transacional violado: apenas uma execucao de ingestOnConnection por transacao e suportada "
                                    + "(origem='" + sourceKey + "').");
                }
            }
        }
        try (PreparedStatement txMark = con.prepareStatement("SELECT set_config('fireatlas.ingest_tx_source', ?, true)")) {
            txMark.setString(1, sourceKey);
            txMark.execute();
        }

        // Savepoint JDBC garante atomicidade da operacao mesmo se o chamador capturar a excecao e tentar commit().
        // O marcador transacional fireatlas.ingest_tx_source (set_config com is_local=true) e definido ANTES do savepoint
        // porque permanece ativo apos ROLLBACK TO SAVEPOINT, ao passo que pg_advisory_xact_lock adquiridos apos o savepoint
        // sao liberados pelo PostgreSQL durante rollback parcial; manter o marcador impede segunda chamada ou multi-origem na mesma transacao.
        Savepoint operationSavepoint = con.setSavepoint();
        boolean completed = false;
        try {
            String sensor = product.startsWith("MODIS") ? "MODIS" : "VIIRS";
            MessageDigest fileDigest;
            try {
                fileDigest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 indisponivel", e);
            }

            DigestInputStream digestIn = new DigestInputStream(in, fileDigest);
            BufferedReader reader = new BufferedReader(new InputStreamReader(digestIn, StandardCharsets.UTF_8), 1 << 16);
            String header = reader.readLine();
            if (header == null) throw new IOException("CSV vazio: " + sourceKey);
            var parser = new DetectionRowParser(header, sensor, product, sourceKey);

            try (Statement st = con.createStatement()) {
                // Qualifica estritamente em pg_temp para jamais remover tabelas permanentes homonimas em public
                st.execute("""
                        DROP TABLE IF EXISTS pg_temp.tmp_detection_stage;
                        DROP TABLE IF EXISTS pg_temp.tmp_affected_grid;
                        CREATE TABLE pg_temp.tmp_detection_stage (
                            sensor text NOT NULL,
                            satellite text NOT NULL,
                            acq_date date NOT NULL,
                            acq_ts timestamptz NOT NULL,
                            lat double precision NOT NULL,
                            lon double precision NOT NULL,
                            brightness real,
                            bright_t31 real,
                            scan real,
                            track real,
                            frp real,
                            confidence_raw text,
                            confidence_class text,
                            daynight text,
                            version text,
                            source_product text NOT NULL,
                            source_file text NOT NULL,
                            source_line_number bigint NOT NULL,
                            source_row_hash text NOT NULL,
                            source_semantic_hash text NOT NULL
                        ) ON COMMIT DROP;
                        CREATE TABLE pg_temp.tmp_affected_grid (
                            acq_date date NOT NULL,
                            sensor text NOT NULL,
                            cx integer NOT NULL,
                            cy integer NOT NULL
                        ) ON COMMIT DROP;
                        """);
            }

            var stats = new DetectionCsvLoader().stage(reader, parser, filter,
                    con.unwrap(PGConnection.class).getCopyAPI(), maxConsecutivePastEnd);
            if (stats.stoppedEarly()) {
                throw new IncompleteSourceReadException(
                        "Leitura incompleta da fonte '" + sourceKey + "' interrompida antes do EOF apos "
                                + stats.scanned() + " linhas; reconciliacao abortada com rollback para impedir exclusoes.");
            }
            String sourceSha256 = HexFormat.of().formatHex(fileDigest.digest());

            if (stats.kept() > 0) {
                try (Statement st = con.createStatement()) {
                    st.execute("CREATE INDEX idx_tmp_stage_key ON pg_temp.tmp_detection_stage (acq_date, source_file, source_line_number, source_semantic_hash)");
                    st.execute("ANALYZE pg_temp.tmp_detection_stage");
                }
            }

            // Fase 1: serializa cargas concorrentes do mesmo source_file sem bloquear outros arquivos (espaco >= 0)
            try (PreparedStatement lock = con.prepareStatement(
                    "SELECT pg_advisory_xact_lock(hashtextextended('ingest_source:' || ?, 0) & 9223372036854775807::bigint)")) {
                lock.setString(1, sourceKey);
                try (ResultSet ignored = lock.executeQuery()) {
                    // lock adquirido ate commit/rollback
                }
            }

            // Sinaliza explicitamente qualquer ambiguidade quando multiplas linhas existentes correspondem ao mesmo registro staged
            try (PreparedStatement ambCheck = con.prepareStatement("""
                    SELECT count(*)
                    FROM (
                        SELECT s.acq_date, s.source_file, s.source_line_number
                        FROM pg_temp.tmp_detection_stage s
                        JOIN detection d ON\s""" + RECONCILIATION_MATCH_PREDICATE + """
                        GROUP BY s.acq_date, s.source_file, s.source_line_number
                        HAVING count(*) > 1
                    ) ambiguous
                    """)) {
                ambCheck.setString(1, sourceSha256);
                try (ResultSet rs = ambCheck.executeQuery()) {
                    rs.next();
                    if (rs.getLong(1) > 0) {
                        throw new IllegalStateException(
                                "Ambiguidade na reconciliacao de registros historicos para '" + sourceKey
                                        + "': multiplas deteccoes existentes correspondem ao mesmo registro-fonte.");
                    }
                }
            }

            // Passo A: remove somente registros obsoletos, sem comprovacao ou semanticamente alterados dentro de (source_file, [start..end], bbox)
            long rowsDeleted;
            try (PreparedStatement del = con.prepareStatement("""
                    WITH deleted AS (
                        DELETE FROM detection d
                        WHERE d.source_file = ?
                          AND d.acq_date BETWEEN ? AND ?
                          AND d.lon BETWEEN ? AND ?
                          AND d.lat BETWEEN ? AND ?
                          AND NOT EXISTS (
                              SELECT 1
                              FROM pg_temp.tmp_detection_stage s
                              WHERE\s""" + RECONCILIATION_MATCH_PREDICATE + """
                          )
                        RETURNING d.acq_date,
                                  d.sensor,
                                  GREATEST(0, LEAST(floor((d.lon + 180) * 10)::int, 3599)) AS cx,
                                  GREATEST(0, LEAST(floor((d.lat + 90) * 10)::int, 1799)) AS cy
                    )
                    INSERT INTO pg_temp.tmp_affected_grid (acq_date, sensor, cx, cy)
                    SELECT acq_date, sensor, cx, cy FROM deleted
                    """)) {
                del.setString(1, sourceKey);
                del.setObject(2, filter.start());
                del.setObject(3, filter.end());
                del.setDouble(4, filter.bbox().minLon());
                del.setDouble(5, filter.bbox().maxLon());
                del.setDouble(6, filter.bbox().minLat());
                del.setDouble(7, filter.bbox().maxLat());
                del.setString(8, sourceSha256);
                rowsDeleted = del.executeUpdate();
            }

            // Passo A2: atualiza metadados ou promove assinatura semantica legada verificada, preservando id e valores cientificos
            try (PreparedStatement updMeta = con.prepareStatement("""
                    UPDATE detection d
                    SET source_row_hash = s.source_row_hash,
                        source_semantic_hash = s.source_semantic_hash,
                        source_semantic_hash_version = 'v2-ieee754-direct',
                        source_semantic_hash_status = CASE
                            WHEN d.legacy_source_semantic_hash IS NOT NULL
                                 AND d.legacy_source_semantic_hash IS DISTINCT FROM s.source_semantic_hash
                                THEN 'UPDATED_FROM_LEGACY'
                            WHEN d.source_semantic_hash_status = 'UPDATED_FROM_LEGACY'
                                THEN 'UPDATED_FROM_LEGACY'
                            ELSE 'CURRENT'
                        END,
                        source_file_sha256 = ?
                    FROM pg_temp.tmp_detection_stage s
                    WHERE\s""" + RECONCILIATION_MATCH_PREDICATE + """
                      AND (
                          d.source_row_hash IS DISTINCT FROM s.source_row_hash
                          OR d.source_semantic_hash IS DISTINCT FROM s.source_semantic_hash
                          OR d.source_semantic_hash_version IS DISTINCT FROM 'v2-ieee754-direct'
                          OR d.source_file_sha256 IS DISTINCT FROM ?
                      )
                    """)) {
                updMeta.setString(1, sourceSha256);
                updMeta.setString(2, sourceSha256);
                updMeta.setString(3, sourceSha256);
                updMeta.executeUpdate();
            }

            long rowsUnchanged;
            try (PreparedStatement st = con.prepareStatement("""
                    SELECT count(*)
                    FROM pg_temp.tmp_detection_stage s
                    WHERE EXISTS (
                        SELECT 1
                        FROM detection d
                        WHERE\s""" + RECONCILIATION_MATCH_PREDICATE + """
                    )
                    """)) {
                st.setString(1, sourceSha256);
                try (ResultSet rs = st.executeQuery()) {
                    rs.next();
                    rowsUnchanged = rs.getLong(1);
                }
            }

            // Passo B: insere somente registros novos ou semanticamente alterados sem ocultar conflitos via ON CONFLICT DO NOTHING
            long rowsInserted;
            try (PreparedStatement ins = con.prepareStatement("""
                    WITH inserted AS (
                        INSERT INTO detection (
                            sensor, satellite, acq_date, acq_ts, lat, lon,
                            brightness, bright_t31, scan, track, frp,
                            confidence_raw, confidence_class, daynight, version,
                            source_product, source_file, source_line_number,
                            source_row_hash, source_semantic_hash, source_file_sha256,
                            source_semantic_hash_version, source_semantic_hash_status
                        )
                        SELECT s.sensor, s.satellite, s.acq_date, s.acq_ts, s.lat, s.lon,
                               s.brightness, s.bright_t31, s.scan, s.track, s.frp,
                               s.confidence_raw, s.confidence_class, s.daynight, s.version,
                               s.source_product, s.source_file, s.source_line_number,
                               s.source_row_hash, s.source_semantic_hash, ?, ?, ?
                        FROM pg_temp.tmp_detection_stage s
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM detection d
                            WHERE\s""" + RECONCILIATION_MATCH_PREDICATE + """
                        )
                        RETURNING acq_date,
                                  sensor,
                                  GREATEST(0, LEAST(floor((lon + 180) * 10)::int, 3599)) AS cx,
                                  GREATEST(0, LEAST(floor((lat + 90) * 10)::int, 1799)) AS cy
                    )
                    INSERT INTO pg_temp.tmp_affected_grid (acq_date, sensor, cx, cy)
                    SELECT acq_date, sensor, cx, cy FROM inserted
                    """)) {
                ins.setString(1, sourceSha256);
                ins.setString(2, SEMANTIC_HASH_VERSION);
                ins.setString(3, STATUS_CURRENT);
                ins.setString(4, sourceSha256);
                rowsInserted = ins.executeUpdate();
            }
            if (rowsInserted + rowsUnchanged != stats.kept()) {
                throw new IllegalStateException(
                        "Inconsistencia na reconciliacao de staging para '" + sourceKey
                                + "': staged=" + stats.kept()
                                + ", inserted=" + rowsInserted
                                + ", unchanged=" + rowsUnchanged);
            }

            if (beforeGridRefresh != null) {
                beforeGridRefresh.run();
            }

            // Passo C: recalcula daily_grid somente para as celulas afetadas (incluindo zeramento apos remocoes)
            refreshAffectedGridCells(con);

            try (PreparedStatement ins = con.prepareStatement("""
                    INSERT INTO ingestion_log (
                        source_key, product, sensor, bbox, period_start, period_end,
                        rows_scanned, rows_loaded, min_date, max_date,
                        source_sha256, rows_inserted, rows_unchanged, rows_deleted
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")) {
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
                ins.setString(11, sourceSha256);
                ins.setLong(12, rowsInserted);
                ins.setLong(13, rowsUnchanged);
                ins.setLong(14, rowsDeleted);
                ins.executeUpdate();
            }

            completed = true;
            con.releaseSavepoint(operationSavepoint);
            return new IngestResult(stats, sourceSha256, rowsInserted, rowsUnchanged, rowsDeleted);
        } catch (IOException | SQLException | RuntimeException | Error e) {
            if (!completed) {
                try {
                    if (!con.isClosed()) {
                        con.rollback(operationSavepoint);
                        con.releaseSavepoint(operationSavepoint);
                    }
                } catch (SQLException rbEx) {
                    e.addSuppressed(rbEx);
                }
            }
            throw e;
        }
    }

    /** Recalcula apenas as celulas (acq_date, sensor, cx, cy) presentes em pg_temp.tmp_affected_grid. */
    static void refreshAffectedGridCells(Connection con) throws SQLException {
        boolean anyAffected;
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery("SELECT EXISTS (SELECT 1 FROM pg_temp.tmp_affected_grid)")) {
            rs.next();
            anyAffected = rs.getBoolean(1);
        }
        if (!anyAffected) {
            return;
        }
        try (Statement st = con.createStatement()) {
            // Fase 2: serializa escritas concorrentes nas mesmas celulas em ordem deterministica (espaco < 0, disjunto da Fase 1)
            try (ResultSet ignored = st.executeQuery("""
                    SELECT pg_advisory_xact_lock(k)
                    FROM (
                        SELECT DISTINCT (hashtextextended(
                            'daily_grid:' || acq_date::text || ':' || sensor || ':' || cx::text || ':' || cy::text, 0
                        ) | (-9223372036854775808)::bigint) AS k
                        FROM pg_temp.tmp_affected_grid
                        ORDER BY 1
                    ) locks
                    """)) {
                while (ignored.next()) {
                    // locks adquiridos em ordem ascendente
                }
            }
            st.executeUpdate("""
                    WITH cells AS MATERIALIZED (
                        SELECT DISTINCT acq_date, sensor, cx, cy
                        FROM pg_temp.tmp_affected_grid
                    )
                    DELETE FROM daily_grid g
                    USING cells c
                    WHERE g.acq_date = c.acq_date
                      AND g.sensor = c.sensor
                      AND g.cx = c.cx
                      AND g.cy = c.cy
                    """);
            st.executeUpdate("""
                    WITH cells AS MATERIALIZED (
                        SELECT DISTINCT acq_date, sensor, cx, cy
                        FROM pg_temp.tmp_affected_grid
                    )
                    INSERT INTO daily_grid (acq_date, sensor, cx, cy, n, frp_sum, frp_max)
                    SELECT c.acq_date,
                           c.sensor,
                           c.cx,
                           c.cy,
                           count(*),
                           coalesce(sum(d.frp), 0),
                           max(d.frp)
                    FROM cells c
                    JOIN detection d
                      ON d.acq_date = c.acq_date
                     AND d.sensor = c.sensor
                     AND d.geom && ST_MakeEnvelope(
                             (c.cx / 10.0) - 180.0 - 0.01,
                             (c.cy / 10.0) - 90.0 - 0.01,
                             ((c.cx + 1) / 10.0) - 180.0 + 0.01,
                             ((c.cy + 1) / 10.0) - 90.0 + 0.01,
                             4326)
                     AND GREATEST(0, LEAST(floor((d.lon + 180) * 10)::int, 3599)) = c.cx
                     AND GREATEST(0, LEAST(floor((d.lat + 90) * 10)::int, 1799)) = c.cy
                    GROUP BY c.acq_date, c.sensor, c.cx, c.cy
                    """);
        }
    }

    /** Recalcula a grade diaria a partir das deteccoes no intervalo de datas informado. */
    static void refreshGrid(Connection con, LocalDate from, LocalDate to) throws SQLException {
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
