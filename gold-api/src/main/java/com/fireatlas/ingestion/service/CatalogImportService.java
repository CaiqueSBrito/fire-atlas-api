package com.fireatlas.ingestion.service;

import com.fireatlas.ingestion.entity.ImportLogEntry;
import com.fireatlas.ingestion.integration.DeltaTable;
import com.fireatlas.ingestion.integration.ForecastFileReader;
import com.fireatlas.ingestion.integration.GridFileReader;
import com.fireatlas.ingestion.integration.LakeStorage;
import com.fireatlas.ingestion.integration.PlaceFileReader;
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

/**
 * Importacao das tabelas de apoio da camada ouro: {@code gold/h3_places} (continente, pais e cidade por celula H3) e
 * {@code gold/forecast} (previsao de fogo). Mesmas regras da grade: so os arquivos ATIVOS do Delta, cada linha
 * validada com o motivo da rejeicao contado, conferencia com {@code numRecords} do log, uma transacao por unidade e
 * carga idempotente. Quem gera os dados e o pipeline do time de dados; aqui so se importa.
 */
public class CatalogImportService {

    private static final Logger log = LoggerFactory.getLogger(CatalogImportService.class);

    public static final String PLACES_TABLE = "gold/h3_places/";
    public static final String FORECAST_TABLE = "gold/forecast/";

    private final IngestionRepository repository;
    private final LakeStorage storage;

    public CatalogImportService(IngestionRepository repository, LakeStorage storage) {
        this.repository = repository;
        this.storage = storage;
    }

    // ---------------------------------------------------------------------------------------------- lugares

    /** resolutions nulo ou vazio = todas as particoes ativas. Cada resolucao e uma transacao. */
    public GoldImportService.Report importPlaces(Set<Integer> resolutions) {
        DeltaTable.RawSnapshot snapshot = load(PLACES_TABLE);
        Map<Integer, List<DeltaTable.RawFile>> byRes = new TreeMap<>();
        for (DeltaTable.RawFile f : snapshot.files()) {
            String r = f.partitions().get("resolution");
            if (r == null) continue;
            int res = Integer.parseInt(r);
            if (resolutions != null && !resolutions.isEmpty() && !resolutions.contains(res)) continue;
            byRes.computeIfAbsent(res, k -> new ArrayList<>()).add(f);
        }
        List<GoldImportService.FileResult> results = new ArrayList<>();
        for (var e : byRes.entrySet()) results.add(importPlacesPartition(e.getKey(), e.getValue(), snapshot.version()));
        return finish(results, PLACES_TABLE, "nenhuma particao de h3_places para as resolucoes pedidas");
    }

    private GoldImportService.FileResult importPlacesPartition(int res, List<DeltaTable.RawFile> files, long version) {
        String key = PLACES_TABLE + "resolution=" + res + "@v" + version;
        LocalDate today = LocalDate.now();
        log.info("Importando {} ({} arquivo(s))", key, files.size());
        try (IngestionRepository.Session session = repository.begin()) {
            try {
                session.deletePlaces(res);
                long scanned = 0;
                long loaded = 0;
                long expected = 0;
                boolean comparable = true;
                Map<String, Long> rejects = new LinkedHashMap<>();
                for (DeltaTable.RawFile f : files) {
                    Path local = storage.download(f.key());
                    Path csv = Files.createTempFile("fireatlas-places-", ".csv");
                    try {
                        var r = PlaceFileReader.extract(local, res, key, csv);
                        if (r.loaded() > 0) session.copyPlaces(csv);
                        scanned += r.scanned();
                        loaded += r.loaded();
                        r.rejects().forEach((k, v) -> rejects.merge(k, v, Long::sum));
                    } finally {
                        storage.release(local);
                        Files.deleteIfExists(csv);
                    }
                    if (f.numRecords() == null) comparable = false;
                    else expected += f.numRecords();
                }
                if (comparable && scanned != expected) {
                    throw new IOException("importacao incompleta: " + scanned + " linhas lidas, " + expected
                            + " esperadas pelo log do Delta");
                }
                session.recordCompleted(new ImportLogEntry(key, "h3_places", "n/a", "global", today, today, scanned,
                        loaded, null, null, scanned - loaded, rejects, comparable ? expected : null, null,
                        comparable));
                session.commit();
                log.info("{}: {} linhas lidas, {} carregadas", key, scanned, loaded);
                return new GoldImportService.FileResult(key, GoldImportService.Status.COMPLETED, scanned, loaded,
                        scanned - loaded, comparable, loaded == 0 ? "nenhum registro valido" : "ok");
            } catch (Exception e) {
                session.rollback();
                throw e;
            }
        } catch (Exception e) {
            return failed(key, "h3_places", today, today, e);
        }
    }

    // ---------------------------------------------------------------------------------------------- previsao

    /** Importa todas as emissoes ativas; as datas de emissao presentes sao recarregadas, as demais ficam. */
    public GoldImportService.Report importForecast() {
        DeltaTable.RawSnapshot snapshot = load(FORECAST_TABLE);
        String key = FORECAST_TABLE + "@v" + snapshot.version();
        LocalDate today = LocalDate.now();
        if (snapshot.files().isEmpty()) {
            return finish(List.of(), FORECAST_TABLE, "a tabela forecast nao tem arquivos ativos");
        }
        log.info("Importando {} ({} arquivo(s))", key, snapshot.files().size());
        List<Path> csvs = new ArrayList<>();
        try {
            Set<LocalDate> dates = new TreeSet<>();
            long scanned = 0;
            long loaded = 0;
            long expected = 0;
            boolean comparable = true;
            Map<String, Long> rejects = new LinkedHashMap<>();
            // le tudo antes de gravar: as datas de emissao definem o que sera apagado
            for (DeltaTable.RawFile f : snapshot.files()) {
                Path local = storage.download(f.key());
                Path csv = Files.createTempFile("fireatlas-forecast-", ".csv");
                csvs.add(csv);
                try {
                    GridFileReader.Result r = ForecastFileReader.extract(local, key, csv, dates);
                    scanned += r.scanned();
                    loaded += r.loaded();
                    r.rejects().forEach((k, v) -> rejects.merge(k, v, Long::sum));
                } finally {
                    storage.release(local);
                }
                if (f.numRecords() == null) comparable = false;
                else expected += f.numRecords();
            }
            if (comparable && scanned != expected) {
                throw new IOException("importacao incompleta: " + scanned + " linhas lidas, " + expected
                        + " esperadas pelo log do Delta");
            }
            LocalDate min = dates.isEmpty() ? today : ((TreeSet<LocalDate>) dates).first();
            LocalDate max = dates.isEmpty() ? today : ((TreeSet<LocalDate>) dates).last();
            try (IngestionRepository.Session session = repository.begin()) {
                try {
                    session.deleteForecast(dates);
                    for (Path csv : csvs) if (Files.size(csv) > 0) session.copyForecast(csv);
                    session.recordCompleted(new ImportLogEntry(key, "forecast", "n/a", "global", min, max, scanned,
                            loaded, min, max, scanned - loaded, rejects, comparable ? expected : null, null,
                            comparable));
                    session.commit();
                } catch (Exception e) {
                    session.rollback();
                    throw e;
                }
            }
            log.info("{}: {} linhas lidas, {} carregadas (emissoes {} a {})", key, scanned, loaded, min, max);
            return new GoldImportService.Report(List.of(new GoldImportService.FileResult(key,
                    GoldImportService.Status.COMPLETED, scanned, loaded, scanned - loaded, comparable,
                    loaded == 0 ? "nenhum registro valido" : "ok")));
        } catch (Exception e) {
            var r = failed(key, "forecast", today, today, e);
            throw new GoldImportService.ImportFailedException("importacao NAO concluida: " + r.detail(),
                    new GoldImportService.Report(List.of(r)));
        } finally {
            for (Path csv : csvs) {
                try {
                    Files.deleteIfExists(csv);
                } catch (IOException ignored) {
                    // temporario
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------------- apoio

    private DeltaTable.RawSnapshot load(String table) {
        try {
            return DeltaTable.loadRaw(storage, table);
        } catch (Exception e) {
            String reason = "fonte indisponivel ao ler o log da tabela Delta: " + GoldImportService.describe(e);
            repository.recordFailure(table, table.replace("gold/", "").replace("/", ""), "n/a", "global",
                    LocalDate.now(), LocalDate.now(), reason);
            throw new GoldImportService.ImportFailedException(reason, new GoldImportService.Report(List.of(
                    new GoldImportService.FileResult(table, GoldImportService.Status.FAILED, 0, 0, 0, false,
                            reason))));
        }
    }

    private GoldImportService.FileResult failed(String key, String product, LocalDate from, LocalDate to,
                                                Exception e) {
        String reason = GoldImportService.describe(e);
        log.error("Importacao FALHOU em {}: {}", key, reason);
        repository.recordFailure(key, product, "n/a", "global", from, to, reason);
        return new GoldImportService.FileResult(key, GoldImportService.Status.FAILED, 0, 0, 0, false, reason);
    }

    private GoldImportService.Report finish(List<GoldImportService.FileResult> results, String table, String none) {
        if (results.isEmpty()) {
            repository.recordFailure(table, table.replace("gold/", "").replace("/", ""), "n/a", "global",
                    LocalDate.now(), LocalDate.now(), none);
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
}
