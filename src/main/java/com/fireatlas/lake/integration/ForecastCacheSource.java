package com.fireatlas.lake.integration;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Previsao nativa r6: snapshot Delta, UNLOAD mensal projetado e leitura limitada dos objetos do manifesto. */
@Component
public class ForecastCacheSource {
    public static final List<String> FORECAST_COLUMNS = List.of("issue_date", "h3_r6", "h3_r1", "prob_fire_7d",
            "det_24h", "det_3d", "det_7d", "frp_mean_7d", "modis_brightness_mean_7d", "viirs_brightness_mean_7d",
            "ring1_det_24h", "ring1_det_7d", "ring2_det_7d", "active", "country_name_pt", "city_name",
            "city_distance_km", "latitude", "longitude", "continent", "country_code", "country_iso_a2", "city_geonameid");
    private static final List<String> PLACE_COLUMNS = List.of("country_name_pt", "city_name", "city_distance_km",
            "latitude", "longitude", "continent", "country_code", "country_iso_a2", "city_geonameid");

    public record Plan(long forecastVersion, long placesVersion, String snapshot,
                       List<AnnualCacheSource.Partition> partitions) {
        public Plan { partitions = List.copyOf(partitions); }
    }

    private final ObjectProvider<S3LakeStorage> storage;
    private final ObjectProvider<AthenaQuery> athena;
    private final AthenaExportStore store;
    private final int concurrency;
    private final long timeoutMs;
    private final String configuredPrefix;

    public ForecastCacheSource(ObjectProvider<S3LakeStorage> storage, ObjectProvider<AthenaQuery> athena,
                               AthenaExportStore store,
                               @Value("${fireatlas.bootstrap.export-concurrency:12}") int concurrency,
                               @Value("${fireatlas.bootstrap.export-timeout-ms:300000}") long timeoutMs,
                               @Value("${fireatlas.bootstrap.export-prefix:}") String configuredPrefix) {
        if (concurrency < 1 || concurrency > 12) {
            throw new IllegalArgumentException("bootstrap.export-concurrency deve estar entre 1 e 12");
        }
        if (timeoutMs < 1) throw new IllegalArgumentException("bootstrap.export-timeout-ms deve ser positivo");
        this.storage = storage;
        this.athena = athena;
        this.store = store;
        this.concurrency = concurrency;
        this.timeoutMs = timeoutMs;
        this.configuredPrefix = configuredPrefix.strip();
    }

    /** O listener permite COPY durante os UNLOADs; somente o plano reconferido autoriza publicar o snapshot. */
    public Plan plan(Consumer<String> progress, AnnualCacheExports.Listener listener) throws IOException {
        var lake = storage.getObject();
        var forecast = DeltaTable.loadRaw(lake, "gold/forecast/");
        var places = DeltaTable.loadRaw(lake, "gold/h3_places/");
        Map<YearMonth, Long> months = months(forecast);
        var query = athena.getObject();
        String base = (configuredPrefix.isEmpty() ? trailingSlash(query.workgroupOutputLocation()) + "bootstrap/"
                : trailingSlash(configuredPrefix)) + "forecast/";
        if (!base.startsWith("s3://") || base.indexOf('/', 5) < 6 || base.contains("\n") || base.contains("\r")) {
            throw new IOException("BOOTSTRAP_EXPORT_PREFIX deve ser um prefixo s3://bucket/pasta/");
        }
        String snapshot = fingerprint(lake.location("gold/"), query.sourceIdentity(), base,
                forecast.version(), places.version(), List.copyOf(months.keySet()));
        listener.snapshot(snapshot);
        List<AnnualCacheSource.Partition> partitions;
        if (months.isEmpty()) {
            partitions = List.of(empty(snapshot, null));
            listener.exported(partitions);
        } else {
            partitions = export(query, lake, base, snapshot, months, progress, listener);
        }
        progress.accept("Conferindo versoes Delta da previsao apos exportacao Athena");
        if (DeltaTable.loadRaw(lake, "gold/forecast/").version() != forecast.version()
                || DeltaTable.loadRaw(lake, "gold/h3_places/").version() != places.version()) {
            throw new IOException("snapshot Delta mudou durante a exportacao Athena da previsao; tente novamente para a versao atual");
        }
        return new Plan(forecast.version(), places.version(), snapshot, partitions);
    }

    private static Map<YearMonth, Long> months(DeltaTable.RawSnapshot forecast) throws IOException {
        Map<YearMonth, Long> months = new TreeMap<>();
        for (var file : forecast.files()) {
            final YearMonth month;
            try {
                int year = Integer.parseInt(file.partitions().get("year"));
                if (year < 1 || year > 9999) throw new IllegalArgumentException("year invalido");
                month = YearMonth.of(year, Integer.parseInt(file.partitions().get("month")));
            } catch (RuntimeException e) {
                throw new IOException("forecast sem particao year/month valida no manifesto Delta: " + file.key(), e);
            }
            Long previous = months.getOrDefault(month, 0L);
            months.put(month, previous == null || file.numRecords() == null ? null : Math.addExact(previous, file.numRecords()));
        }
        return months;
    }

    private List<AnnualCacheSource.Partition> export(AthenaQuery query, S3LakeStorage lake, String base, String snapshot,
                                                    Map<YearMonth, Long> months, Consumer<String> progress,
                                                    AnnualCacheExports.Listener listener) throws IOException {
        var executor = Executors.newFixedThreadPool(concurrency);
        var completion = new ExecutorCompletionService<List<AnnualCacheSource.Partition>>(executor);
        List<Future<List<AnnualCacheSource.Partition>>> futures = new ArrayList<>();
        List<AnnualCacheSource.Partition> partitions = new ArrayList<>();
        boolean succeeded = false;
        progress.accept("Athena: exportando previsao, 0/" + months.size() + " meses verificados");
        try {
            for (var entry : months.entrySet()) {
                futures.add(completion.submit(() -> exportOne(query, lake, base, snapshot, entry.getKey(), entry.getValue())));
            }
            for (int completed = 1; completed <= months.size(); completed++) {
                var exported = completion.take().get();
                partitions.addAll(exported);
                listener.exported(exported);
                progress.accept("Athena: previsao " + completed + "/" + months.size() + " meses exportados ou confirmados vazios");
            }
            succeeded = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("exportacao Athena da previsao interrompida", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException io) throw io;
            if (e.getCause() instanceof RuntimeException runtime) throw runtime;
            throw new IOException("exportacao Athena da previsao falhou", e.getCause());
        } finally {
            if (!succeeded) futures.forEach(future -> future.cancel(true));
            executor.shutdownNow();
            boolean interrupted = Thread.interrupted();
            while (true) {
                try {
                    if (executor.awaitTermination(1, TimeUnit.SECONDS)) break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }
        partitions.sort(Comparator.comparing(AnnualCacheSource.Partition::key));
        return List.copyOf(partitions);
    }

    private List<AnnualCacheSource.Partition> exportOne(AthenaQuery query, S3LakeStorage lake, String base,
                                                       String snapshot, YearMonth month, Long expectedRows) throws IOException {
        if (Thread.currentThread().isInterrupted()) throw new IOException("exportacao Athena da previsao interrompida");
        if (Long.valueOf(0).equals(expectedRows)) return List.of(empty(snapshot, month));
        // A chave duravel nao tem coluna year: a identidade por ano impede colisoes de janeiro entre anos distintos.
        String yearSnapshot = hash(snapshot + "\nyear=" + month.getYear());
        var key = new AthenaExportStore.Key(yearSnapshot, "FORECAST", 6, month.getMonthValue(), 0);
        var export = store.find(key);
        if (export == null || export.state().equals("failed")) {
            String token = UUID.randomUUID().toString();
            export = store.begin(key, token, base + snapshot + "/year=" + month.getYear() + "/" + yearSnapshot
                    + "/r=6/m=" + month.getMonthValue() + "/" + token + "/");
        }
        List<String> files;
        if (export.state().equals("complete")) {
            files = AnnualCacheExports.validateFiles(export.files(), export.prefix());
        } else {
            try {
                String id = export.queryId();
                if (id == null) {
                    id = query.startCommand(unloadSql(month, export.prefix()), export.token());
                    store.started(export, id);
                }
                var command = query.awaitCommand(id, timeoutMs);
                if (command.manifestLocation() == null || command.manifestLocation().isBlank()) {
                    throw new IOException("UNLOAD da previsao terminou sem DataManifestLocation");
                }
                files = AnnualCacheExports.manifest(lake.readLocation(command.manifestLocation()), export.prefix());
                ensureRows(month, expectedRows, files);
                // Uma falha de S3/persistencia conserva query_id: o UNLOAD bem-sucedido nao e repetido.
                store.complete(export, command.manifestLocation(), files);
            } catch (RuntimeException e) {
                if (e instanceof AthenaQuery.QueryFailedException || Thread.currentThread().isInterrupted()) store.failed(export);
                throw e;
            }
        }
        ensureRows(month, expectedRows, files);
        if (files.isEmpty()) return List.of(empty(snapshot, month));
        Long expected = files.size() == 1 ? expectedRows : null;
        return files.stream().map(file -> new AnnualCacheSource.Partition(file, "FORECAST", 6, -1, expected, false)).toList();
    }

    private static void ensureRows(YearMonth month, Long expectedRows, List<String> files) throws IOException {
        if (files.isEmpty() && expectedRows != null && expectedRows > 0) {
            throw new IOException("UNLOAD vazio diverge do snapshot Delta da previsao: " + month);
        }
    }

    private static AnnualCacheSource.Partition empty(String snapshot, YearMonth month) {
        return new AnnualCacheSource.Partition("empty:athena:" + snapshot + ":FORECAST:6"
                + (month == null ? "" : ":" + month), "FORECAST", 6, 0, 0L, true);
    }

    static String unloadSql(YearMonth month, String prefix) {
        String places = PLACE_COLUMNS.stream().map(column -> "\"" + column + "\"").collect(Collectors.joining(", "));
        String selection = FORECAST_COLUMNS.stream().map(column -> (PLACE_COLUMNS.contains(column) ? "pl." : "f.")
                + "\"" + column + "\"").collect(Collectors.joining(", "));
        // Escolhe uma linha inteira de lugares por celula; duplicatas nunca multiplicam probabilidades nativas.
        return "UNLOAD (SELECT " + selection + " FROM forecast f LEFT JOIN (SELECT h3_cell, " + places
                + " FROM (SELECT h3_cell, " + places + ", row_number() OVER (PARTITION BY h3_cell ORDER BY "
                + places + ") AS place_rank FROM h3_places WHERE resolution = 6) ranked WHERE place_rank = 1) pl"
                + " ON pl.h3_cell = f.h3_r6 WHERE f.year = " + month.getYear() + " AND f.month = " + month.getMonthValue()
                + ") TO '" + prefix.replace("'", "''") + "' WITH (format='PARQUET', compression='ZSTD')";
    }

    static String fingerprint(String lake, String context, String prefix, long forecast, long places, List<YearMonth> months) {
        return hash("forecast-native-r6-places-row-v1\n" + lake + "\n" + context + "\n" + prefix + "\n" + forecast
                + "\n" + places + "\n" + months + "\n" + String.join(",", FORECAST_COLUMNS));
    }

    private static String hash(String input) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponivel", e);
        }
    }

    private static String trailingSlash(String value) { return value.endsWith("/") ? value : value + "/"; }

    /** Nenhum Parquet original e baixado; cada worker tem temporario e DuckDB limitados. */
    public long stream(AnnualCacheSource.Partition partition, Consumer<List<Object[]>> consumer) throws IOException, SQLException {
        if (!partition.kind().equals("FORECAST") || partition.resolution() != 6) {
            throw new IOException("particao nao pertence a previsao nativa r6");
        }
        if (partition.empty()) return 0;
        if (!partition.key().startsWith("s3://")) throw new IOException("particao nao e um objeto exportado pelo Athena");
        var lake = storage.getObject();
        Path file = lake.downloadLocation(partition.key());
        try (var connection = DriverManager.getConnection("jdbc:duckdb:"); var statement = connection.createStatement()) {
            statement.execute("SET memory_limit='128MB'");
            statement.execute("SET threads=2");
            String parquet = "read_parquet('" + file.toAbsolutePath().toString().replace("\\", "/").replace("'", "''")
                    + "', hive_partitioning=false)";
            var available = new HashSet<String>();
            try (ResultSet columns = statement.executeQuery("DESCRIBE SELECT * FROM " + parquet)) {
                while (columns.next()) available.add(columns.getString("column_name"));
            }
            if (!available.containsAll(FORECAST_COLUMNS)) throw new IOException("Parquet projetado sem colunas contratadas da previsao");
            String selection = FORECAST_COLUMNS.stream().map(column -> "\"" + column + "\"").collect(Collectors.joining(", "));
            long count = 0;
            List<Object[]> batch = new ArrayList<>(AnnualCacheSource.BATCH_SIZE);
            try (ResultSet rows = statement.executeQuery("SELECT " + selection + " FROM " + parquet)) {
                while (rows.next()) {
                    if (Thread.currentThread().isInterrupted()) throw new IOException("carga da previsao interrompida");
                    Object[] values = new Object[FORECAST_COLUMNS.size()];
                    values[0] = rows.getDate(1);
                    for (int i = 1; i < values.length; i++) values[i] = rows.getObject(i + 1);
                    batch.add(values);
                    count++;
                    if (batch.size() == AnnualCacheSource.BATCH_SIZE) {
                        consumer.accept(batch);
                        batch.clear();
                    }
                }
            }
            if (!batch.isEmpty()) consumer.accept(batch);
            if (partition.expectedRows() != null && count != partition.expectedRows()) {
                throw new IOException("numRecords do snapshot da previsao divergente: esperado " + partition.expectedRows() + ", lido " + count);
            }
            return count;
        } finally {
            lake.release(file);
        }
    }
}
