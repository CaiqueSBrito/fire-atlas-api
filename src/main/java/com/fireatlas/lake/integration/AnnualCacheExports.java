package com.fireatlas.lake.integration;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** UNLOAD filtra/projeta no Athena. Somente os objetos do manifesto de sucesso podem ser importados. */
@Component
public class AnnualCacheExports {
    public record Request(String kind, int resolution, Long expectedRows) {}

    private final ObjectProvider<AthenaQuery> athena;
    private final ObjectProvider<S3LakeStorage> storage;
    private final AthenaExportStore store;
    private final int concurrency;
    private final long timeoutMs;
    private final String configuredPrefix;

    public AnnualCacheExports(ObjectProvider<AthenaQuery> athena, ObjectProvider<S3LakeStorage> storage,
                              AthenaExportStore store,
                              @Value("${fireatlas.bootstrap.export-concurrency:2}") int concurrency,
                              @Value("${fireatlas.bootstrap.export-timeout-ms:300000}") long timeoutMs,
                              @Value("${fireatlas.bootstrap.export-prefix:}") String configuredPrefix) {
        if (concurrency < 1 || concurrency > 4) {
            throw new IllegalArgumentException("bootstrap.export-concurrency deve estar entre 1 e 4");
        }
        if (timeoutMs < 1) throw new IllegalArgumentException("bootstrap.export-timeout-ms deve ser positivo");
        this.athena = athena;
        this.storage = storage;
        this.store = store;
        this.concurrency = concurrency;
        this.timeoutMs = timeoutMs;
        this.configuredPrefix = configuredPrefix.strip();
    }

    public List<AnnualCacheSource.Partition> export(long gridVersion, long placesVersion, List<Request> requests,
                                                   Consumer<String> progress) throws IOException {
        var query = athena.getObject();
        var lake = storage.getObject();
        String prefix = configuredPrefix.isEmpty()
                ? trailingSlash(query.workgroupOutputLocation()) + "bootstrap/2026/" : trailingSlash(configuredPrefix);
        if (!prefix.startsWith("s3://") || prefix.length() <= 6 || prefix.indexOf('/', 5) < 6
                || prefix.contains("\n") || prefix.contains("\r")) {
            throw new IOException("BOOTSTRAP_EXPORT_PREFIX deve ser um prefixo s3://bucket/pasta/");
        }
        String snapshot = fingerprint(lake.location("gold/"), query.sourceIdentity(), prefix, gridVersion, placesVersion);
        progress.accept("Athena: exportando colunas, 0/" + requests.size() + " particoes verificadas");
        var executor = Executors.newFixedThreadPool(concurrency);
        var completion = new ExecutorCompletionService<List<AnnualCacheSource.Partition>>(executor);
        List<Future<List<AnnualCacheSource.Partition>>> futures = new ArrayList<>();
        List<AnnualCacheSource.Partition> partitions = new ArrayList<>();
        boolean succeeded = false;
        try {
            for (var request : requests) {
                futures.add(completion.submit(() -> exportOne(query, lake, snapshot, prefix, request)));
            }
            for (int completed = 1; completed <= requests.size(); completed++) {
                partitions.addAll(completion.take().get());
                progress.accept("Athena: " + completed + "/" + requests.size()
                        + " particoes exportadas ou confirmadas vazias; publicacao PostGIS ainda pendente");
            }
            succeeded = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("exportacao Athena interrompida", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException runtime) throw runtime;
            throw new IOException("exportacao Athena falhou", cause);
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
        partitions.sort(Comparator.comparing(AnnualCacheSource.Partition::kind)
                .thenComparingInt(AnnualCacheSource.Partition::resolution).thenComparing(AnnualCacheSource.Partition::key));
        return List.copyOf(partitions);
    }

    private List<AnnualCacheSource.Partition> exportOne(AthenaQuery query, S3LakeStorage lake, String snapshot,
                                                       String base, Request request) throws IOException {
        if (Thread.currentThread().isInterrupted()) throw new IOException("exportacao Athena interrompida");
        if (Long.valueOf(0).equals(request.expectedRows())) return List.of(empty(snapshot, request));
        var key = new AthenaExportStore.Key(snapshot, request.kind(), request.resolution());
        var export = store.find(key);
        if (export == null || export.state().equals("failed")) {
            String token = UUID.randomUUID().toString();
            export = store.begin(key, token, base + snapshot + "/" + request.kind().toLowerCase(java.util.Locale.ROOT)
                    + "/r=" + request.resolution() + "/" + token + "/");
        }
        List<String> files;
        if (export.state().equals("complete")) {
            files = validateFiles(export.files(), export.prefix());
        } else {
            try {
                String id = export.queryId();
                if (id == null) {
                    id = query.startCommand(unloadSql(request, export.prefix()), export.token());
                    store.started(export, id);
                }
                var command = query.awaitCommand(id, timeoutMs);
                files = manifest(lake.readLocation(command.manifestLocation()), export.prefix());
                ensureNotMissingRows(request, files);
                // Falhas de leitura/persistencia deixam query_id reutilizavel: nao repetem um UNLOAD bem-sucedido.
                store.complete(export, command.manifestLocation(), files);
            } catch (RuntimeException e) {
                if (e instanceof AthenaQuery.QueryFailedException || Thread.currentThread().isInterrupted()) {
                    store.failed(export);
                }
                throw e;
            }
        }
        ensureNotMissingRows(request, files);
        if (files.isEmpty()) return List.of(empty(snapshot, request));
        Long expected = files.size() == 1 ? request.expectedRows() : null;
        return files.stream().map(file -> new AnnualCacheSource.Partition(file, request.kind(), request.resolution(),
                -1, expected, false)).toList();
    }

    private static void ensureNotMissingRows(Request request, List<String> files) throws IOException {
        if (files.isEmpty() && request.expectedRows() != null && request.expectedRows() > 0) {
            throw new IOException("UNLOAD vazio diverge do snapshot Delta: " + request.kind() + " r=" + request.resolution());
        }
    }

    private static AnnualCacheSource.Partition empty(String snapshot, Request request) {
        return new AnnualCacheSource.Partition("empty:athena:" + snapshot + ":" + request.kind() + ":"
                + request.resolution(), request.kind(), request.resolution(), 0, 0L, true);
    }

    static String unloadSql(Request request, String prefix) {
        boolean grid = request.kind().equals("GRID");
        List<String> columns = grid ? AnnualCacheSource.GRID_COLUMNS : AnnualCacheSource.PLACE_COLUMNS;
        return "UNLOAD (SELECT " + columns.stream().map(column -> "\"" + column + "\"")
                .collect(java.util.stream.Collectors.joining(", ")) + " FROM " + (grid ? "map_grid" : "h3_places")
                + " WHERE resolution = " + request.resolution() + (grid ? " AND year = " + AnnualCacheSource.YEAR : "")
                + ") TO '" + prefix.replace("'", "''") + "' WITH (format='PARQUET', compression='ZSTD')";
    }

    /** Athena emite CSV de uma coluna, sem cabecalho; manifesto vazio confirma zero arquivos. */
    static List<String> manifest(byte[] bytes, String prefix) throws IOException {
        List<String> files = new ArrayList<>();
        for (String row : new String(bytes, StandardCharsets.UTF_8).split("\\r?\\n")) {
            if (row.isBlank()) continue;
            String file = row;
            if (row.startsWith("\"")) {
                if (!row.endsWith("\"") || row.length() < 2) throw new IOException("manifesto UNLOAD CSV invalido");
                file = row.substring(1, row.length() - 1).replace("\"\"", "\"");
            } else if (row.contains(",")) {
                throw new IOException("manifesto UNLOAD deve ter uma unica coluna");
            }
            files.add(file);
        }
        return validateFiles(files, prefix);
    }

    private static List<String> validateFiles(List<String> files, String prefix) throws IOException {
        var seen = new HashSet<String>();
        for (String file : files) {
            if (!file.startsWith(prefix) || file.length() == prefix.length() || !seen.add(file)) {
                throw new IOException("manifesto UNLOAD contem objeto duplicado ou fora do destino projetado");
            }
        }
        return List.copyOf(files);
    }

    private static String trailingSlash(String value) {
        return value.endsWith("/") ? value : value + "/";
    }

    static String fingerprint(String lake, String context, String prefix, long grid, long places) {
        String input = lake + "\n" + context + "\n" + prefix + "\n" + AnnualCacheSource.YEAR + "\n" + grid + "\n" + places
                + "\n" + String.join(",", AnnualCacheSource.GRID_COLUMNS) + "\n" + String.join(",", AnnualCacheSource.PLACE_COLUMNS);
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponivel", e);
        }
    }
}
