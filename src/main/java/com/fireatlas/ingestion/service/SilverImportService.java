package com.fireatlas.ingestion.service;

import com.fireatlas.ingestion.integration.SilverFileReader;
import com.fireatlas.ingestion.repository.SilverIngestionRepository;
import com.fireatlas.lake.integration.DeltaTable;
import com.fireatlas.lake.integration.LakeStorage;
import com.fireatlas.region.entity.BBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Importacao dos focos individuais da prata ({@code prata/silver}, Delta Lake) para a tabela {@code detection}, a
 * fonte da harmonizacao MODIS x VIIRS por pareamento.
 * <ul>
 *   <li>So os arquivos ATIVOS do Delta (log + checkpoint) entram;</li>
 *   <li>recorte por periodo e, opcionalmente, por regiao (bbox): a prata tem latitude e longitude por foco;</li>
 *   <li>cada linha e validada e o motivo das rejeicoes e contado; a identidade e o {@code record_id};</li>
 *   <li>arquivo lido por inteiro (sem bbox, dentro do periodo) e conferido com o {@code numRecords} do log;</li>
 *   <li>cada arquivo e uma transacao: idempotente (reimportar nao duplica) e a falha fica registrada.</li>
 * </ul>
 */
public class SilverImportService {

    private static final Logger log = LoggerFactory.getLogger(SilverImportService.class);
    private static final Pattern AWS_KEY_ID = Pattern.compile("(AKIA|ASIA)[0-9A-Z]{16}");

    public static final String DEFAULT_TABLE = "prata/silver/";

    /** bbox nulo = sem filtro de regiao. */
    public record Request(LocalDate start, LocalDate end, BBox bbox) {
        public Request {
            if (start == null || end == null || end.isBefore(start)) {
                throw new IllegalArgumentException("periodo invalido: start e end sao obrigatorios e end >= start");
            }
        }
    }

    public enum Status { COMPLETED, SKIPPED, FAILED }

    public record FileResult(String key, Status status, long scanned, long inserted, long unchanged, long rejected,
                             boolean verified, String detail) {}

    public record Report(List<FileResult> files) {
        public boolean failed() {
            return files.stream().anyMatch(f -> f.status() == Status.FAILED);
        }

        public long inserted() {
            return files.stream().mapToLong(FileResult::inserted).sum();
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

    private final SilverIngestionRepository repository;
    private final LakeStorage storage;
    private final String tablePath;

    public SilverImportService(SilverIngestionRepository repository, LakeStorage storage, String tablePath) {
        this.repository = repository;
        this.storage = storage;
        this.tablePath = tablePath.endsWith("/") ? tablePath : tablePath + "/";
    }

    public Report run(Request req) {
        String bbox = req.bbox() == null ? "global" : req.bbox().toString();
        DeltaTable.RawSnapshot snapshot;
        try {
            snapshot = DeltaTable.loadRaw(storage, tablePath);
        } catch (Exception e) {
            String reason = "fonte indisponivel ao ler o log da tabela Delta: " + describe(e);
            repository.recordFailure(tablePath, bbox, req.start(), req.end(), reason);
            throw new ImportFailedException(reason, new Report(List.of(
                    new FileResult(tablePath, Status.FAILED, 0, 0, 0, 0, false, reason))));
        }
        log.info("Tabela {} na versao {}: {} arquivo(s) ativo(s)", tablePath, snapshot.version(),
                snapshot.files().size());

        List<FileResult> results = new ArrayList<>();
        YearMonth from = YearMonth.from(req.start());
        YearMonth to = YearMonth.from(req.end());
        for (DeltaTable.RawFile f : snapshot.files()) {
            String y = f.partitions().get("acquisition_year");
            String m = f.partitions().get("acquisition_month");
            if (y != null && m != null) {
                YearMonth part = YearMonth.of(Integer.parseInt(y), Integer.parseInt(m));
                if (part.isBefore(from) || part.isAfter(to)) continue;
            }
            LocalDate fmin = f.minDate("acquisition_date");
            LocalDate fmax = f.maxDate("acquisition_date");
            if (fmin != null && fmax != null && (fmax.isBefore(req.start()) || fmin.isAfter(req.end()))) continue;
            results.add(importFile(f, snapshot.version(), req, bbox));
        }

        boolean anyFailed = results.stream().anyMatch(r -> r.status() == Status.FAILED);
        if (results.isEmpty()) {
            String reason = "nenhum arquivo da prata cobre o periodo pedido";
            repository.recordFailure(tablePath, bbox, req.start(), req.end(), reason);
            results.add(new FileResult(tablePath, Status.FAILED, 0, 0, 0, 0, false, reason));
            anyFailed = true;
        }
        Report report = new Report(results);
        if (anyFailed) {
            FileResult first = results.stream().filter(r -> r.status() == Status.FAILED).findFirst().orElseThrow();
            throw new ImportFailedException("importacao NAO concluida: " + first.key() + " - " + first.detail(),
                    report);
        }
        return report;
    }

    /** Carrega um arquivo da prata em uma transacao: qualquer falha desfaz tudo que foi lido dele. */
    private FileResult importFile(DeltaTable.RawFile f, long version, Request req, String bbox) {
        String key = f.key() + "@v" + version;
        log.info("Importando {}", key);
        Path local = null;
        Path csv = null;
        try (SilverIngestionRepository.Session session = repository.begin()) {
            try {
                local = storage.download(f.key());
                csv = Files.createTempFile("fireatlas-silver-", ".csv");
                SilverFileReader.Result r = SilverFileReader.extract(local, req.start(), req.end(), req.bbox(), csv);
                long inserted = 0;
                if (r.loaded() > 0) {
                    session.stage(csv);
                    inserted = session.insertNew();
                    session.refreshGrid(r.minDate(), r.maxDate());
                }
                // RN06: arquivo lido por inteiro (sem bbox, dentro do periodo) precisa bater com o log do Delta
                LocalDate fmin = f.minDate("acquisition_date");
                LocalDate fmax = f.maxDate("acquisition_date");
                boolean comparable = req.bbox() == null && f.numRecords() != null && fmin != null && fmax != null
                        && !fmin.isBefore(req.start()) && !fmax.isAfter(req.end());
                if (comparable && r.scanned() != f.numRecords()) {
                    throw new IOException("importacao incompleta: " + r.scanned() + " linhas lidas, "
                            + f.numRecords() + " esperadas pelo log do Delta");
                }
                session.recordCompleted(key, bbox, req.start(), req.end(), r.scanned(), inserted, r.minDate(),
                        r.maxDate(), r.rejected(), r.rejects(), comparable ? f.numRecords() : null, comparable);
                session.commit();
                r.rejects().forEach((reason, n) -> log.warn("{}: {} linha(s) rejeitada(s) por {}", key, n, reason));
                log.info("{}: {} lidas, {} novas, {} ja existiam, {} rejeitadas{}", key, r.scanned(), inserted,
                        r.loaded() - inserted, r.rejected(), comparable ? " [conferido com o log do Delta]" : "");
                return new FileResult(key, Status.COMPLETED, r.scanned(), inserted, r.loaded() - inserted,
                        r.rejected(), comparable, r.loaded() == 0 ? "nenhum registro valido no recorte" : "ok");
            } catch (Exception e) {
                session.rollback();
                throw e;
            }
        } catch (Exception e) {
            String reason = describe(e);
            log.error("Importacao FALHOU em {}: {}", key, reason);
            repository.recordFailure(key, bbox, req.start(), req.end(), reason);
            return new FileResult(key, Status.FAILED, 0, 0, 0, 0, false, reason);
        } finally {
            if (local != null) storage.release(local);
            try {
                if (csv != null) Files.deleteIfExists(csv);
            } catch (IOException ignored) {
                // temporario
            }
        }
    }

    /** Descricao da falha para log e registro: classe + mensagem, sem credenciais, limitada em tamanho. */
    static String describe(Throwable e) {
        String msg = e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
        msg = AWS_KEY_ID.matcher(msg).replaceAll("[REDACTED]");
        return msg.length() > 500 ? msg.substring(0, 500) : msg;
    }
}
