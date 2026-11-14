package com.fireatlas.ingestion.service;

import com.fireatlas.ingestion.entity.ImportLogEntry;
import com.fireatlas.ingestion.integration.AvailabilityFileReader;
import com.fireatlas.ingestion.integration.DeltaTable;
import com.fireatlas.ingestion.integration.GridFileReader;
import com.fireatlas.ingestion.integration.HarmonizedFileReader;
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

/**
 * Importacao da atividade de fogo harmonizada da camada ouro: {@code gold/harmonized/cells} (celula H3, dia local e
 * nivel de zoom; particionada por resolution e year) e {@code gold/harmonized/availability} (satelite por dia;
 * particionada por year). Mesmas regras da grade: so os arquivos ATIVOS do Delta, validacao por linha com o motivo
 * contado, conferencia com {@code numRecords} do log, uma transacao por particao e carga idempotente por intervalo.
 * Quem gera e atualiza os dados e o pipeline do time de dados; aqui so se importa.
 */
public class HarmonizedImportService {

    private static final Logger log = LoggerFactory.getLogger(HarmonizedImportService.class);

    public static final String CELLS_TABLE = "gold/harmonized/cells/";
    public static final String AVAILABILITY_TABLE = "gold/harmonized/availability/";
    public static final int MAX_RESOLUTION = 6;

    private final IngestionRepository repository;
    private final LakeStorage storage;

    public HarmonizedImportService(IngestionRepository repository, LakeStorage storage) {
        this.repository = repository;
        this.storage = storage;
    }

    /** resolutions nulo ou vazio = todas (0 a 6). */
    public GoldImportService.Report importCells(LocalDate start, LocalDate end, Set<Integer> resolutions) {
        if (start == null || end == null || end.isBefore(start)) {
            throw new IllegalArgumentException("periodo invalido: start e end sao obrigatorios e end >= start");
        }
        if (resolutions != null) {
            for (int r : resolutions) {
                if (r < 0 || r > MAX_RESOLUTION) {
                    throw new IllegalArgumentException("resolucao H3 invalida: " + r + " (0 a " + MAX_RESOLUTION + ")");
                }
            }
        }
        DeltaTable.RawSnapshot snapshot = load(CELLS_TABLE, start, end);
        Map<String, List<DeltaTable.RawFile>> parts = new TreeMap<>();
        for (DeltaTable.RawFile f : snapshot.files()) {
            String r = f.partitions().get("resolution");
            String y = f.partitions().get("year");
            if (r == null || y == null) continue;
            int res = Integer.parseInt(r);
            int year = Integer.parseInt(y);
            if (resolutions != null && !resolutions.isEmpty() && !resolutions.contains(res)) continue;
            if (year < start.getYear() || year > end.getYear()) continue;
            if (outside(f, start, end)) continue;
            parts.computeIfAbsent(String.format("%02d/%d", res, year), k -> new ArrayList<>()).add(f);
        }
        List<GoldImportService.FileResult> results = new ArrayList<>();
        for (List<DeltaTable.RawFile> files : parts.values()) {
            results.add(importCellsPartition(files, snapshot.version(), start, end));
        }
        return finish(results, CELLS_TABLE, start, end, "nenhuma particao de harmonized/cells cobre o pedido");
    }

    public GoldImportService.Report importAvailability(LocalDate start, LocalDate end) {
        if (start == null || end == null || end.isBefore(start)) {
            throw new IllegalArgumentException("periodo invalido: start e end sao obrigatorios e end >= start");
        }
        DeltaTable.RawSnapshot snapshot = load(AVAILABILITY_TABLE, start, end);
        Map<Integer, List<DeltaTable.RawFile>> parts = new TreeMap<>();
        for (DeltaTable.RawFile f : snapshot.files()) {
            String y = f.partitions().get("year");
            if (y == null) continue;
            int year = Integer.parseInt(y);
            if (year < start.getYear() || year > end.getYear() || outside(f, start, end)) continue;
            parts.computeIfAbsent(year, k -> new ArrayList<>()).add(f);
        }
        List<GoldImportService.FileResult> results = new ArrayList<>();
        for (var e : parts.entrySet()) {
            results.add(importAvailabilityPartition(e.getKey(), e.getValue(), snapshot.version(), start, end));
        }
        return finish(results, AVAILABILITY_TABLE, start, end,
                "nenhuma particao de harmonized/availability cobre o pedido");
    }

    // ---------------------------------------------------------------------------------------------- celulas

    private GoldImportService.FileResult importCellsPartition(List<DeltaTable.RawFile> files, long version,
                                                              LocalDate start, LocalDate end) {
        int res = Integer.parseInt(files.get(0).partitions().get("resolution"));
        int year = Integer.parseInt(files.get(0).partitions().get("year"));
        String key = CELLS_TABLE + "resolution=" + res + "/year=" + year + "@v" + version;
        LocalDate from = max(start, LocalDate.of(year, 1, 1));
        LocalDate to = min(end, LocalDate.of(year, 12, 31));
        log.info("Importando {} ({} arquivo(s), {} a {})", key, files.size(), from, to);
        try (IngestionRepository.Session session = repository.begin()) {
            try {
                session.deleteHarmonized(res, from, to);
                Totals t = new Totals();
                for (DeltaTable.RawFile f : files) {
                    Path local = storage.download(f.key());
                    Path csv = Files.createTempFile("fireatlas-harmonized-", ".csv");
                    try {
                        var r = HarmonizedFileReader.extract(local, res, key, from, to, csv);
                        if (r.loaded() > 0) session.copyHarmonized(csv);
                        t.add(r, f, from, to);
                    } finally {
                        storage.release(local);
                        Files.deleteIfExists(csv);
                    }
                }
                return complete(session, key, "harmonized_cells", from, to, t);
            } catch (Exception e) {
                session.rollback();
                throw e;
            }
        } catch (Exception e) {
            return failed(key, "harmonized_cells", from, to, e);
        }
    }

    // ---------------------------------------------------------------------------------------------- disponibilidade

    private GoldImportService.FileResult importAvailabilityPartition(int year, List<DeltaTable.RawFile> files,
                                                                     long version, LocalDate start, LocalDate end) {
        String key = AVAILABILITY_TABLE + "year=" + year + "@v" + version;
        LocalDate from = max(start, LocalDate.of(year, 1, 1));
        LocalDate to = min(end, LocalDate.of(year, 12, 31));
        log.info("Importando {} ({} arquivo(s), {} a {})", key, files.size(), from, to);
        try (IngestionRepository.Session session = repository.begin()) {
            try {
                session.deleteAvailability(from, to);
                Totals t = new Totals();
                for (DeltaTable.RawFile f : files) {
                    Path local = storage.download(f.key());
                    Path csv = Files.createTempFile("fireatlas-availability-", ".csv");
                    try {
                        var r = AvailabilityFileReader.extract(local, key, from, to, csv);
                        if (r.loaded() > 0) session.copyAvailability(csv);
                        t.add(r, f, from, to);
                    } finally {
                        storage.release(local);
                        Files.deleteIfExists(csv);
                    }
                }
                return complete(session, key, "harmonized_availability", from, to, t);
            } catch (Exception e) {
                session.rollback();
                throw e;
            }
        } catch (Exception e) {
            return failed(key, "harmonized_availability", from, to, e);
        }
    }

    // ---------------------------------------------------------------------------------------------- apoio

    /** Soma das leituras de uma particao e se a conferencia com o log do Delta e possivel. */
    private static final class Totals {
        long scanned;
        long loaded;
        long expected;
        boolean comparable = true;
        LocalDate min;
        LocalDate max;
        final Map<String, Long> rejects = new LinkedHashMap<>();

        void add(GridFileReader.Result r, DeltaTable.RawFile f, LocalDate from, LocalDate to) {
            scanned += r.scanned();
            loaded += r.loaded();
            r.rejects().forEach((k, v) -> rejects.merge(k, v, Long::sum));
            if (r.minDay() != null && (min == null || r.minDay().isBefore(min))) min = r.minDay();
            if (r.maxDay() != null && (max == null || r.maxDay().isAfter(max))) max = r.maxDay();
            LocalDate fmin = f.minDate("day");
            LocalDate fmax = f.maxDate("day");
            boolean whole = f.numRecords() != null && fmin != null && fmax != null && !fmin.isBefore(from)
                    && !fmax.isAfter(to);
            comparable &= whole;
            if (f.numRecords() != null) expected += f.numRecords();
        }
    }

    private GoldImportService.FileResult complete(IngestionRepository.Session session, String key, String product,
                                                  LocalDate from, LocalDate to, Totals t) throws Exception {
        // RN06: arquivo lido por inteiro tem de bater com o log do Delta
        if (t.comparable && t.scanned != t.expected) {
            throw new IOException("importacao incompleta: " + t.scanned + " linhas lidas, " + t.expected
                    + " esperadas pelo log do Delta");
        }
        session.recordCompleted(new ImportLogEntry(key, product, "n/a", "global", from, to, t.scanned, t.loaded,
                t.min, t.max, t.scanned - t.loaded, t.rejects, t.comparable ? t.expected : null, null,
                t.comparable));
        session.commit();
        t.rejects.forEach((reason, n) -> log.warn("{}: {} linha(s) rejeitada(s) por {}", key, n, reason));
        log.info("{}: {} linhas lidas, {} carregadas{}", key, t.scanned, t.loaded,
                t.comparable ? " [conferido com o log do Delta]" : "");
        return new GoldImportService.FileResult(key, GoldImportService.Status.COMPLETED, t.scanned, t.loaded,
                t.scanned - t.loaded, t.comparable, t.loaded == 0 ? "nenhum registro valido no recorte" : "ok");
    }

    private GoldImportService.FileResult failed(String key, String product, LocalDate from, LocalDate to,
                                                Exception e) {
        String reason = GoldImportService.describe(e);
        log.error("Importacao FALHOU em {}: {}", key, reason);
        repository.recordFailure(key, product, "n/a", "global", from, to, reason);
        return new GoldImportService.FileResult(key, GoldImportService.Status.FAILED, 0, 0, 0, false, reason);
    }

    private DeltaTable.RawSnapshot load(String table, LocalDate start, LocalDate end) {
        try {
            return DeltaTable.loadRaw(storage, table);
        } catch (Exception e) {
            String reason = "fonte indisponivel ao ler o log da tabela Delta: " + GoldImportService.describe(e);
            repository.recordFailure(table, table.replace("gold/", "").replace("/", "_"), "n/a", "global", start,
                    end, reason);
            throw new GoldImportService.ImportFailedException(reason, new GoldImportService.Report(List.of(
                    new GoldImportService.FileResult(table, GoldImportService.Status.FAILED, 0, 0, 0, false,
                            reason))));
        }
    }

    private GoldImportService.Report finish(List<GoldImportService.FileResult> results, String table, LocalDate start,
                                            LocalDate end, String none) {
        if (results.isEmpty()) {
            repository.recordFailure(table, table.replace("gold/", "").replace("/", "_"), "n/a", "global", start,
                    end, none);
            results = List.of(new GoldImportService.FileResult(table, GoldImportService.Status.FAILED, 0, 0, 0,
                    false, none));
        }
        var report = new GoldImportService.Report(results);
        if (report.failed()) {
            var first = results.stream().filter(r -> r.status() == GoldImportService.Status.FAILED).findFirst()
                    .orElseThrow();
            throw new GoldImportService.ImportFailedException("importacao NAO concluida: " + first.key() + " - "
                    + first.detail(), report);
        }
        return report;
    }

    /** Arquivo fora do periodo (pelas estatisticas do log) nao precisa ser lido. */
    private static boolean outside(DeltaTable.RawFile f, LocalDate start, LocalDate end) {
        LocalDate min = f.minDate("day");
        LocalDate max = f.maxDate("day");
        return min != null && max != null && (max.isBefore(start) || min.isAfter(end));
    }

    private static LocalDate max(LocalDate a, LocalDate b) {
        return a.isAfter(b) ? a : b;
    }

    private static LocalDate min(LocalDate a, LocalDate b) {
        return a.isBefore(b) ? a : b;
    }
}
