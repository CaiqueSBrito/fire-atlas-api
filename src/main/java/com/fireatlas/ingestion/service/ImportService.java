package com.fireatlas.ingestion.service;

import com.fireatlas.ingestion.entity.ImportLogEntry;
import com.fireatlas.ingestion.integration.DetectionCsvLoader;
import com.fireatlas.ingestion.integration.DetectionRowParser;
import com.fireatlas.ingestion.integration.H3Options;
import com.fireatlas.ingestion.integration.LakeSource;
import com.fireatlas.ingestion.integration.ParquetCsvReader;
import com.fireatlas.ingestion.repository.IngestionRepository;
import com.fireatlas.region.entity.BBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Casos de uso da importacao do data lake para o PostGIS (RN01 a RN06 do card de ingestao):
 * <ul>
 *   <li>so produtos aprovados (RN01); recorte por regiao e periodo (RN02), respeitando a cobertura da fonte;</li>
 *   <li>validacao de cada registro, com motivo da rejeicao (RN03); proveniencia em cada linha (RN05);</li>
 *   <li>conferencia com o manifesto da fonte: arquivo incompleto nao vira importacao concluida (RN06);</li>
 *   <li>falha de fonte ou resposta invalida: nada e gravado do arquivo, a falha fica registrada e a execucao
 *       falha (CA04).</li>
 * </ul>
 * A persistencia fica no {@link IngestionRepository}; a fonte, em {@link LakeSource}.
 */
public class ImportService {

    private static final Logger log = LoggerFactory.getLogger(ImportService.class);
    /** Camada bronze (CSV bruto da FIRMS); a camada ouro (Parquet com H3) usa o prefixo "gold/". */
    public static final String DEFAULT_PREFIX = "bronze/raw/";
    private static final Pattern AWS_KEY_ID = Pattern.compile("(AKIA|ASIA)[0-9A-Z]{16}");

    public record Request(LocalDate start, LocalDate end, BBox bbox, Set<String> sensors, Set<String> products) {
        public Request {
            if (start == null || end == null || end.isBefore(start)) {
                throw new IllegalArgumentException("periodo invalido: start e end sao obrigatorios e end >= start");
            }
            if (bbox == null) throw new IllegalArgumentException("regiao (bbox) obrigatoria");
            if (sensors == null || sensors.isEmpty()) sensors = Set.of("MODIS", "VIIRS");
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
    private final LakeSource source;
    private final Set<String> approved;
    private final H3Options h3Options;
    private final String prefix;
    private final Pattern keyPattern;

    public ImportService(IngestionRepository repository, LakeSource source, Collection<String> approvedProducts) {
        this(repository, source, approvedProducts, H3Options.DEFAULT);
    }

    /** h3Options: coluna da fonte que traz o indice H3 pronto (o backend so o consome). */
    public ImportService(IngestionRepository repository, LakeSource source, Collection<String> approvedProducts,
                         H3Options h3Options) {
        this(repository, source, approvedProducts, h3Options, DEFAULT_PREFIX);
    }

    /**
     * prefix: pasta do data lake com os arquivos, no formato {prefix}{produto}/{ano}/{arquivo}.csv|.parquet
     * (ex.: "bronze/raw/" ou "gold/").
     */
    public ImportService(IngestionRepository repository, LakeSource source, Collection<String> approvedProducts,
                         H3Options h3Options, String prefix) {
        this.repository = repository;
        this.source = source;
        this.approved = Set.copyOf(approvedProducts);
        this.h3Options = h3Options;
        this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
        this.keyPattern = Pattern.compile("^" + Pattern.quote(this.prefix)
                + "([^/]+)/(\\d{4})/([^/]+\\.(?:csv|parquet))$");
    }

    public Report run(Request req) {
        if (req.products() != null) {
            for (String p : req.products()) {
                if (!approved.contains(p)) {
                    throw new IllegalArgumentException("produto nao aprovado pela pesquisa: " + p
                            + " (aprovados: " + new TreeSet<>(approved) + ")");
                }
            }
        }
        var filter = new DetectionCsvLoader.Filter(req.bbox(), req.start(), req.end());

        List<String> keys;
        try {
            keys = source.listKeys(prefix);
        } catch (Exception e) {
            String reason = "fonte indisponivel ao listar os arquivos: " + describe(e);
            recordFailure("-", "-", "-", filter, reason);
            throw new ImportFailedException(reason, new Report(List.of(
                    new FileResult("-", Status.FAILED, 0, 0, 0, false, reason))));
        }

        List<FileResult> results = new ArrayList<>();
        for (String key : keys) {
            Matcher m = keyPattern.matcher(key);
            if (!m.matches()) continue;
            String product = m.group(1);
            if (!approved.contains(product)) continue;
            if (req.products() != null && !req.products().isEmpty() && !req.products().contains(product)) continue;
            String sensor = sensorOf(product);
            if (!req.sensors().contains(sensor)) continue;
            int year = Integer.parseInt(m.group(2));
            if (year < req.start().getYear() || year > req.end().getYear()) continue;
            results.add(importOne(key, product, sensor, filter));
        }

        boolean anyCompleted = results.stream().anyMatch(r -> r.status() == Status.COMPLETED);
        boolean anyFailed = results.stream().anyMatch(r -> r.status() == Status.FAILED);
        if (!anyCompleted && !anyFailed) {
            // RN02/RN06: nada cobre o pedido; nao pode parecer uma importacao concluida.
            String reason = "nenhum arquivo da fonte cobre a regiao e o periodo pedidos (produtos aprovados: "
                    + new TreeSet<>(approved) + ")";
            recordFailure("-", "-", "-", filter, reason);
            results.add(new FileResult("-", Status.FAILED, 0, 0, 0, false, reason));
            anyFailed = true;
        }
        Report report = new Report(results);
        if (anyFailed) {
            throw new ImportFailedException("importacao NAO concluida: "
                    + results.stream().filter(r -> r.status() == Status.FAILED).count() + " falha(s)", report);
        }
        return report;
    }

    /** Importa um CSV local (contingencia com amostra baixada previamente). Sem manifesto, nao ha conferencia. */
    public FileResult importLocalFile(Path path, String product, DetectionCsvLoader.Filter filter) {
        if (!approved.contains(product)) {
            throw new IllegalArgumentException("produto nao aprovado pela pesquisa: " + product
                    + " (aprovados: " + new TreeSet<>(approved) + ")");
        }
        String key = "local:" + path.getFileName();
        String sensor = sensorOf(product);
        boolean parquet = path.getFileName().toString().endsWith(".parquet");
        try {
            LakeSource.Manifest manifest = null;
            if (parquet) {
                var info = ParquetCsvReader.info(path); // rodape: confere que o arquivo e um Parquet valido
                manifest = new LakeSource.Manifest(info.rows(), info.minDate(), info.maxDate(), null, false);
            }
            try (InputStream in = parquet ? ParquetCsvReader.openAsCsv(path, filter) : Files.newInputStream(path)) {
                return load(in, key, product, sensor, filter, manifest);
            }
        } catch (Exception e) {
            return fail(key, product, sensor, filter, describe(e));
        }
    }

    private FileResult importOne(String key, String product, String sensor, DetectionCsvLoader.Filter f) {
        LakeSource.Manifest manifest;
        try {
            manifest = source.manifest(key).orElse(null);
        } catch (Exception e) {
            return fail(key, product, sensor, f, "manifesto da fonte indisponivel: " + describe(e));
        }
        if (manifest != null && manifest.observedMin() != null && manifest.observedMax() != null
                && (manifest.observedMax().isBefore(f.start()) || manifest.observedMin().isAfter(f.end()))) {
            source.release(key); // nao sera aberto
            return new FileResult(key, Status.SKIPPED, 0, 0, 0, false,
                    "fora da cobertura do arquivo (" + manifest.observedMin() + " a " + manifest.observedMax() + ")");
        }
        log.info("Importando {}", key);
        InputStream in = null;
        try {
            in = source.open(key, f);
            return load(in, key, product, sensor, f, manifest);
        } catch (Exception e) {
            return fail(key, product, sensor, f, describe(e));
        } finally {
            if (in != null) {
                try {
                    in.close(); // a fonte decide se aborta a leitura (parou antes do fim)
                } catch (IOException | RuntimeException ignored) {
                    // fechamento: a falha relevante (se houver) ja foi tratada
                }
            }
        }
    }

    /** Carrega um arquivo em uma transacao: qualquer falha desfaz tudo que foi lido dele. */
    private FileResult load(InputStream in, String key, String product, String sensor,
                            DetectionCsvLoader.Filter f, LakeSource.Manifest manifest) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 16);
             IngestionRepository.Session session = repository.begin()) {
            try {
                String header = reader.readLine();
                if (header == null) throw new IOException("CSV vazio: " + key);
                var parser = new DetectionRowParser(header, sensor, product, key, h3Options);

                session.deleteDetections(key, f.start(), f.end());
                var stats = session.copyDetections(reader, parser, f);

                // RN06: leitura incompleta nunca vira "importacao concluida".
                if (!stats.stoppedEarly() && manifest != null && manifest.countComparable()
                        && stats.scanned() != manifest.rows()) {
                    throw new IOException("importacao incompleta: " + stats.scanned() + " linhas lidas, "
                            + manifest.rows() + " esperadas pelo manifesto da fonte");
                }
                boolean verified = !stats.stoppedEarly() && manifest != null;

                session.recordCompleted(new ImportLogEntry(key, product, sensor, f.bbox().toString(), f.start(),
                        f.end(), stats.scanned(), stats.kept(), stats.minDate(), stats.maxDate(),
                        stats.rejected(), stats.rejects(), manifest == null ? null : manifest.rows(),
                        manifest == null ? null : manifest.sha256(), verified));
                stats.rejectSamples().forEach((reason, sample) ->
                        log.warn("{}: {} linha(s) rejeitada(s) por {} (exemplo: {})", key,
                                stats.rejects().get(reason), reason, sample));
                if (stats.kept() > 0) session.refreshAggregates(stats.minDate(), stats.maxDate());
                session.commit();
                log.info("{}: {} linhas lidas, {} carregadas, {} rejeitadas{}{}", key, stats.scanned(),
                        stats.kept(), stats.rejected(), stats.stoppedEarly() ? " (parou apos o periodo)" : "",
                        verified ? " [conferido com o manifesto]" : "");
                return new FileResult(key, Status.COMPLETED, stats.scanned(), stats.kept(), stats.rejected(),
                        verified, stats.kept() == 0 ? "nenhum registro valido no recorte" : "ok");
            } catch (Exception e) {
                session.rollback();
                throw e;
            }
        }
    }

    private FileResult fail(String key, String product, String sensor, DetectionCsvLoader.Filter f, String reason) {
        log.error("Importacao FALHOU em {}: {}", key, reason);
        recordFailure(key, product, sensor, f, reason);
        return new FileResult(key, Status.FAILED, 0, 0, 0, false, reason);
    }

    private void recordFailure(String key, String product, String sensor, DetectionCsvLoader.Filter f,
                               String reason) {
        repository.recordFailure(key, product, sensor, f.bbox().toString(), f.start(), f.end(), reason);
    }

    /** Descricao da falha para log e registro: classe + mensagem, sem credenciais, limitada em tamanho. */
    static String describe(Throwable e) {
        String msg = e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
        msg = AWS_KEY_ID.matcher(msg).replaceAll("[REDACTED]");
        return msg.length() > 500 ? msg.substring(0, 500) : msg;
    }

    static String sensorOf(String product) {
        return product.startsWith("MODIS") ? "MODIS" : "VIIRS";
    }
}
