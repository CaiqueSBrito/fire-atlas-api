package com.fireatlas.gold.ingestion.service;

import com.fireatlas.gold.ingestion.entity.SyncState;
import com.fireatlas.gold.ingestion.repository.IngestionRepository;
import com.fireatlas.lake.integration.DeltaTable;
import com.fireatlas.lake.integration.LakeStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Sincronizacao da gold do data lake com o PostGIS (a que roda sozinha ao subir a API). Para cada tabela compara a
 * versao atual do Delta com a ultima sincronizacao concluida ({@code lake_sync_state}) e so importa se ela mudou:
 * <ul>
 *   <li>primeira carga: de {@code start} ate hoje;</li>
 *   <li>seguintes: do fim da carga anterior menos {@code overlapDays} (o lake corrige dias recentes) ate hoje;</li>
 *   <li>tabelas sem periodo (lugares, previsao) sao recarregadas inteiras quando a versao muda.</li>
 * </ul>
 * O estado so avanca quando a tabela inteira entra; uma tabela que falha nao impede as outras e e tentada de novo na
 * proxima execucao. A prata nao entra: ela so alimenta a API antiga (/api/v1) e continua no profile ingest-silver.
 */
public class LakeSyncService {

    private static final Logger log = LoggerFactory.getLogger(LakeSyncService.class);

    public enum Status { IMPORTED, UNCHANGED, FAILED }

    public record Result(String table, Status status, String detail) {}

    private record Dataset(String table, boolean windowed,
                           BiFunction<LocalDate, LocalDate, GoldImportService.Report> importer) {}

    private final IngestionRepository repository;
    private final LakeStorage storage;
    private final LocalDate start;
    private final int overlapDays;

    public LakeSyncService(IngestionRepository repository, LakeStorage storage, LocalDate start, int overlapDays) {
        if (overlapDays < 0) throw new IllegalArgumentException("overlapDays deve ser >= 0");
        this.repository = repository;
        this.storage = storage;
        this.start = start;
        this.overlapDays = overlapDays;
    }

    /** Sincroniza todas as tabelas da gold ate {@code today}; a grade do mapa vem primeiro. */
    public List<Result> run(LocalDate today) {
        var grid = new GoldImportService(repository, storage, GoldImportService.DEFAULT_TABLE);
        var catalog = new CatalogImportService(repository, storage);
        var harmonized = new HarmonizedImportService(repository, storage);
        List<Dataset> datasets = List.of(
                new Dataset(GoldImportService.DEFAULT_TABLE, true,
                        (from, to) -> grid.run(new GoldImportService.Request(from, to, null))),
                new Dataset(CatalogImportService.PLACES_TABLE, false, (from, to) -> catalog.importPlaces(null)),
                new Dataset(HarmonizedImportService.CELLS_TABLE, true,
                        (from, to) -> harmonized.importCells(from, to, null)),
                new Dataset(HarmonizedImportService.AVAILABILITY_TABLE, true, harmonized::importAvailability),
                new Dataset(CatalogImportService.FORECAST_TABLE, false, (from, to) -> catalog.importForecast()));
        List<Result> results = new ArrayList<>();
        for (Dataset d : datasets) results.add(sync(d, today));
        return results;
    }

    private Result sync(Dataset d, LocalDate today) {
        try {
            long version = DeltaTable.loadRaw(storage, d.table()).version();
            Optional<SyncState> state = repository.findSyncState(d.table());
            if (state.isPresent() && state.get().version() == version) {
                log.info("{}: sem mudancas no data lake (versao {})", d.table(), version);
                return new Result(d.table(), Status.UNCHANGED, "versao " + version);
            }
            LocalDate from = state.map(SyncState::syncedThrough).map(t -> t.minusDays(overlapDays)).orElse(start);
            log.info("{}: importando a versao {}{}", d.table(), version,
                    d.windowed() ? " (" + from + " a " + today + ")" : "");
            long loaded = d.importer().apply(from, today).loaded();
            repository.saveSyncState(new SyncState(d.table(), version, d.windowed() ? today : null));
            log.info("{}: sincronizado na versao {} ({} linhas)", d.table(), version, loaded);
            return new Result(d.table(), Status.IMPORTED, loaded + " linhas");
        } catch (Exception e) {
            String reason = GoldImportService.describe(e);
            log.error("{}: sincronizacao NAO concluida (nova tentativa na proxima subida): {}", d.table(), reason);
            return new Result(d.table(), Status.FAILED, reason);
        }
    }
}
