package com.fireatlas.ingestion.service;

import com.fireatlas.ingestion.entity.ImportLogEntry;
import com.fireatlas.ingestion.integration.DeltaTable;
import com.fireatlas.ingestion.integration.GridFileReader;
import com.fireatlas.ingestion.integration.LakeStorage;
import com.fireatlas.ingestion.repository.IngestionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Importacao da camada ouro do data lake (tabela Delta {@code gold/map_grid}: grade H3 por dia e sensor) para o
 * PostGIS. A API usa somente esta fonte.
 * <ul>
 *   <li>So os arquivos ATIVOS do Delta (log + checkpoint) entram; nunca arquivos removidos ou antigos;</li>
 *   <li>recorte por periodo e resolucoes H3, com cada linha validada e o motivo das rejeicoes contado (RN03);</li>
 *   <li>proveniencia: cada linha guarda a particao e a versao do Delta de onde veio; o registro da importacao
 *       guarda linhas lidas, gravadas, rejeitadas e esperadas (RN05);</li>
 *   <li>conferencia com o log do Delta: se o arquivo foi lido por inteiro, o numero de linhas tem de bater com
 *       {@code numRecords}; senao a importacao da particao falha e nada dela e gravado (RN06);</li>
 *   <li>cada particao (resolucao e ano) e uma transacao: idempotente (apaga e recarrega o intervalo).</li>
 * </ul>
 */
public class GoldImportService {

    private static final Logger log = LoggerFactory.getLogger(GoldImportService.class);
    private static final Pattern AWS_KEY_ID = Pattern.compile("(AKIA|ASIA)[0-9A-Z]{16}");

    /** Tabela Delta usada por padrao (prefixo no bucket). */
    public static final String DEFAULT_TABLE = "gold/map_grid/";
    /** Maior resolucao H3 que a gold entrega. */
    public static final int MAX_RESOLUTION = 6;

    /** resolutions nulo ou vazio = todas (0 ate a maior). */
    public record Request(LocalDate start, LocalDate end, Set<Integer> resolutions) {
        public Request {
            if (start == null || end == null || end.isBefore(start)) {
                throw new IllegalArgumentException("periodo invalido: start e end sao obrigatorios e end >= start");
            }
            if (resolutions == null || resolutions.isEmpty()) {
                resolutions = IntStream.rangeClosed(0, MAX_RESOLUTION).boxed().collect(Collectors.toSet());
            }
            for (int r : resolutions) {
                if (r < 0 || r > MAX_RESOLUTION) {
                    throw new IllegalArgumentException("resolucao H3 invalida: " + r + " (a gold tem 0 a "
                            + MAX_RESOLUTION + ")");
                }
            }
        }
    }

    public enum Status { COMPLETED, SKIPPED, FAILED }

    public record FileResult(String key, Status status, long scanned, long loaded, long rejected,
                             boolean verified, String detail) {}

    public record Report(List<FileResult> files) {
        public boolean failed() {
            return files.stream().anyMatch(f -> f.status() == Status.FAILED);
        }

        public long loaded() {
            return files.stream().mapToLong(FileResult::loaded).sum();
        }
    }

    /** A importacao nao foi concluida; o relatorio diz o que foi (ou nao) importado. */
    public static class ImportFailedException extends RuntimeException {
        private final transient Report report;

        public ImportFailedException(String message, Report report) {
            super(message);
            this.report = report;
        }

        public Report report() {
            return report;
        }
    }

    private final IngestionRepository repository;
    private final LakeStorage storage;
    private final String tablePath;
    private final String product;

    public GoldImportService(IngestionRepository repository, LakeStorage storage, String tablePath) {
        this.repository = repository;
        this.storage = storage;
        this.tablePath = tablePath.endsWith("/") ? tablePath : tablePath + "/";
        String trimmed = this.tablePath.substring(0, this.tablePath.length() - 1);
        this.product = trimmed.substring(trimmed.lastIndexOf('/') + 1);
    }

    public Report run(Request req) {
        DeltaTable.Snapshot snapshot;
        try {
            snapshot = DeltaTable.load(storage, tablePath);
        } catch (Exception e) {
            String reason = "fonte indisponivel ao ler o log da tabela Delta: " + describe(e);
            recordFailure(tablePath, req, reason);
            throw new ImportFailedException(reason, new Report(List.of(
                    new FileResult(tablePath, Status.FAILED, 0, 0, 0, false, reason))));
        }
        log.info("Tabela {} na versao {}: {} arquivo(s) ativo(s)", tablePath, snapshot.version(),
                snapshot.files().size());

        // particoes (resolucao, ano) que cobrem o pedido; arquivos fora do periodo sao ignorados pelas estatisticas
        Map<String, List<DeltaTable.DataFile>> partitions = new TreeMap<>();
        List<FileResult> results = new ArrayList<>();
        for (DeltaTable.DataFile f : snapshot.files()) {
            if (!req.resolutions().contains(f.resolution())) continue;
            if (f.year() < req.start().getYear() || f.year() > req.end().getYear()) continue;
            if (f.minDay() != null && f.maxDay() != null
                    && (f.maxDay().isBefore(req.start()) || f.minDay().isAfter(req.end()))) {
                results.add(new FileResult(f.key(), Status.SKIPPED, 0, 0, 0, false,
                        "fora do periodo (" + f.minDay() + " a " + f.maxDay() + ")"));
                continue;
            }
            partitions.computeIfAbsent(String.format("%02d/%d", f.resolution(), f.year()), k -> new ArrayList<>())
                    .add(f);
        }

        boolean anyFailed = false;
        for (List<DeltaTable.DataFile> files : partitions.values()) {
            FileResult r = importPartition(files, snapshot.version(), req);
            results.add(r);
            anyFailed |= r.status() == Status.FAILED;
        }
        if (partitions.isEmpty()) {
            String reason = "nenhuma particao da gold cobre o periodo e as resolucoes pedidos";
            recordFailure(tablePath, req, reason);
            results.add(new FileResult(tablePath, Status.FAILED, 0, 0, 0, false, reason));
            anyFailed = true;
        }
        Report report = new Report(results);
        if (anyFailed) {
            var failures = results.stream().filter(r -> r.status() == Status.FAILED).toList();
            throw new ImportFailedException("importacao NAO concluida: " + failures.size() + " falha(s); primeira: "
                    + failures.get(0).key() + " - " + failures.get(0).detail(), report);
        }
        return report;
    }

    /** Carrega todos os arquivos de uma particao (resolucao e ano) em uma transacao. */
    private FileResult importPartition(List<DeltaTable.DataFile> files, long version, Request req) {
        int res = files.get(0).resolution();
        int year = files.get(0).year();
        String key = tablePath + "resolution=" + res + "/year=" + year + "@v" + version;
        LocalDate from = max(req.start(), LocalDate.of(year, 1, 1));
        LocalDate to = min(req.end(), LocalDate.of(year, 12, 31));
        log.info("Importando {} ({} arquivo(s), {} a {})", key, files.size(), from, to);

        try (IngestionRepository.Session session = repository.begin()) {
            try {
                session.deleteGrid(res, from, to);
                long scanned = 0;
                long loaded = 0;
                Map<String, Long> rejects = new LinkedHashMap<>();
                LocalDate min = null;
                LocalDate max = null;
                long expected = 0;
                boolean comparable = true;
                for (DeltaTable.DataFile f : files) {
                    Path local = storage.download(f.key());
                    Path csv = Files.createTempFile("fireatlas-grid-", ".csv");
                    try {
                        var r = GridFileReader.extract(local, res, key, from, to, csv);
                        if (r.loaded() > 0) session.copyGrid(csv);
                        scanned += r.scanned();
                        loaded += r.loaded();
                        r.rejects().forEach((k, v) -> rejects.merge(k, v, Long::sum));
                        if (r.minDay() != null && (min == null || r.minDay().isBefore(min))) min = r.minDay();
                        if (r.maxDay() != null && (max == null || r.maxDay().isAfter(max))) max = r.maxDay();
                    } finally {
                        storage.release(local);
                        Files.deleteIfExists(csv);
                    }
                    boolean whole = f.numRecords() != null && f.minDay() != null && f.maxDay() != null
                            && !f.minDay().isBefore(from) && !f.maxDay().isAfter(to);
                    comparable &= whole;
                    if (f.numRecords() != null) expected += f.numRecords();
                }
                // RN06: arquivo lido por inteiro tem de bater com o log do Delta
                if (comparable && scanned != expected) {
                    throw new IOException("importacao incompleta: " + scanned + " linhas lidas, " + expected
                            + " esperadas pelo log do Delta");
                }
                session.recordCompleted(new ImportLogEntry(key, product, "MODIS,VIIRS", "global", from, to, scanned,
                        loaded, min, max, rejects.values().stream().mapToLong(Long::longValue).sum(), rejects,
                        comparable ? expected : null, null, comparable));
                session.commit();
                rejects.forEach((reason, n) -> log.warn("{}: {} linha(s) rejeitada(s) por {}", key, n, reason));
                log.info("{}: {} linhas lidas, {} carregadas{}", key, scanned, loaded,
                        comparable ? " [conferido com o log do Delta]" : "");
                return new FileResult(key, Status.COMPLETED, scanned, loaded,
                        scanned - loaded, comparable, loaded == 0 ? "nenhum registro valido no recorte" : "ok");
            } catch (Exception e) {
                session.rollback();
                throw e;
            }
        } catch (Exception e) {
            String reason = describe(e);
            log.error("Importacao FALHOU em {}: {}", key, reason);
            repository.recordFailure(key, product, "MODIS,VIIRS", "global", from, to, reason);
            return new FileResult(key, Status.FAILED, 0, 0, 0, false, reason);
        }
    }

    private void recordFailure(String key, Request req, String reason) {
        repository.recordFailure(key, product, "MODIS,VIIRS", "global", req.start(), req.end(), reason);
    }

    private static LocalDate max(LocalDate a, LocalDate b) {
        return a.isAfter(b) ? a : b;
    }

    private static LocalDate min(LocalDate a, LocalDate b) {
        return a.isBefore(b) ? a : b;
    }

    /** Descricao da falha para log e registro: classe + mensagem, sem credenciais, limitada em tamanho. */
    static String describe(Throwable e) {
        String msg = e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
        msg = AWS_KEY_ID.matcher(msg).replaceAll("[REDACTED]");
        return msg.length() > 500 ? msg.substring(0, 500) : msg;
    }

    public static Set<Integer> parseResolutions(String csv) {
        Set<Integer> out = new TreeSet<>();
        for (String p : csv.split(",")) if (!p.isBlank()) out.add(Integer.parseInt(p.trim()));
        return out;
    }
}
