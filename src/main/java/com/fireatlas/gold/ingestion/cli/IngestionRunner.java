package com.fireatlas.gold.ingestion.cli;

import com.fireatlas.lake.integration.S3LakeStorage;
import com.fireatlas.gold.ingestion.repository.IngestionRepository;
import com.fireatlas.gold.ingestion.service.CatalogImportService;
import com.fireatlas.gold.ingestion.service.GoldImportService;
import com.fireatlas.gold.ingestion.service.HarmonizedImportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;

/**
 * Rotina de importacao em linha de comando (profile "ingest-gold"), chamada por scripts/atualizar.sh: baixa a camada
 * ouro do data lake (grade, lugares, previsao e atividade harmonizada) para o PostgreSQL, que e a unica fonte da
 * /api/v2. E uma camada de entrada, como a API REST: so interpreta os argumentos e chama os servicos de importacao.
 * Se a importacao nao for concluida, o processo termina com erro (codigo de saida diferente de zero).
 *
 * <pre>
 * java -jar target/fire-atlas-api-0.1.0.jar --spring.profiles.active=ingest-gold --ingest.dataset=all \
 *   --ingest.start=2026-11-01 --ingest.end=2026-11-15 --ingest.resolutions=0,1,2,3,4,5
 * </pre>
 * {@code --ingest.resolutions} e opcional (padrao: 0 a 6).
 */
@Component("goldIngestionRunner")
@Profile("ingest-gold")
public class IngestionRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(IngestionRunner.class);

    private final IngestionRepository repository;
    private final S3LakeStorage lake;
    private final String table;

    public IngestionRunner(IngestionRepository repository, @Lazy S3LakeStorage lake,
                           @Value("${fireatlas.ingest.table:gold/map_grid/}") String table) {
        this.repository = repository;
        this.lake = lake;
        this.table = table;
    }

    @Override
    public void run(ApplicationArguments args) {
        // dataset: map_grid (padrao), places (continente/pais/cidade), forecast (previsao), harmonized (atividade
        // harmonizada: celulas e disponibilidade dos satelites) ou all
        String dataset = args.containsOption("ingest.dataset") ? required(args, "ingest.dataset") : "map_grid";
        try {
            if (dataset.equals("map_grid") || dataset.equals("all")) {
                LocalDate start = LocalDate.parse(required(args, "ingest.start"));
                LocalDate end = LocalDate.parse(required(args, "ingest.end"));
                var resolutions = args.containsOption("ingest.resolutions")
                        ? GoldImportService.parseResolutions(required(args, "ingest.resolutions")) : null;
                show(new GoldImportService(repository, lake, table)
                        .run(new GoldImportService.Request(start, end, resolutions)));
            }
            var catalog = new CatalogImportService(repository, lake);
            if (dataset.equals("places") || dataset.equals("all")) {
                var resolutions = args.containsOption("ingest.resolutions")
                        ? GoldImportService.parseResolutions(required(args, "ingest.resolutions")) : null;
                show(catalog.importPlaces(resolutions));
            }
            if (dataset.equals("forecast") || dataset.equals("all")) {
                show(catalog.importForecast());
            }
            if (dataset.equals("harmonized") || dataset.equals("all")) {
                LocalDate start = LocalDate.parse(required(args, "ingest.start"));
                LocalDate end = LocalDate.parse(required(args, "ingest.end"));
                var resolutions = args.containsOption("ingest.resolutions")
                        ? GoldImportService.parseResolutions(required(args, "ingest.resolutions")) : null;
                var harmonized = new HarmonizedImportService(repository, lake);
                show(harmonized.importCells(start, end, resolutions));
                show(harmonized.importAvailability(start, end));
            }
            if (!java.util.Set.of("map_grid", "places", "forecast", "harmonized", "all").contains(dataset)) {
                throw new IllegalArgumentException(
                        "--ingest.dataset deve ser map_grid, places, forecast, harmonized ou all");
            }
        } catch (GoldImportService.ImportFailedException e) {
            e.report().files().forEach(f -> log.error("  {} {} - {}", f.status(), f.key(), f.detail()));
            log.error("Importacao NAO CONCLUIDA: {}", e.getMessage());
            throw e; // codigo de saida != 0: nao parece sucesso
        } finally {
            lake.close();
        }
    }

    private static void show(GoldImportService.Report report) {
        report.files().forEach(f -> log.info("  {} {} - {}", f.status(), f.key(), f.detail()));
        log.info("Importacao CONCLUIDA: {} linhas carregadas", report.loaded());
    }

    private static String required(ApplicationArguments a, String name) {
        List<String> v = a.getOptionValues(name);
        if (v == null || v.isEmpty()) throw new IllegalArgumentException("argumento obrigatorio: --" + name);
        return v.get(0);
    }
}
