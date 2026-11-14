package com.fireatlas.ingestion.cli;

import com.fireatlas.ingestion.integration.S3LakeStorage;
import com.fireatlas.ingestion.repository.IngestionRepository;
import com.fireatlas.ingestion.service.GoldImportService;
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
 * Rotina de importacao em linha de comando (profile "ingest"): le a camada ouro (tabela Delta gold/map_grid) do
 * data lake e grava a grade H3 no PostGIS. E uma camada de entrada, como a API REST: so interpreta os argumentos e
 * chama o {@link GoldImportService}. Se a importacao nao for concluida, o processo termina com erro (codigo de saida
 * diferente de zero).
 *
 * <pre>
 * java -jar target/fire-atlas-api-0.1.0.jar --spring.profiles.active=ingest \
 *   --ingest.start=2023-01-01 --ingest.end=2026-12-31 --ingest.resolutions=0,1,2,3,4,5
 * </pre>
 * {@code --ingest.resolutions} e opcional (padrao: 0 a 6). Veja scripts/importar.sh.
 */
@Component
@Profile("ingest")
public class IngestionRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(IngestionRunner.class);

    private final IngestionRepository repository;
    private final S3LakeStorage lake;
    private final String table;

    public IngestionRunner(IngestionRepository repository, @Lazy S3LakeStorage lake,
                           @Value("${fireatlas.ingest.table}") String table) {
        this.repository = repository;
        this.lake = lake;
        this.table = table;
    }

    @Override
    public void run(ApplicationArguments args) {
        LocalDate start = LocalDate.parse(required(args, "ingest.start"));
        LocalDate end = LocalDate.parse(required(args, "ingest.end"));
        var resolutions = args.containsOption("ingest.resolutions")
                ? GoldImportService.parseResolutions(required(args, "ingest.resolutions")) : null;
        var service = new GoldImportService(repository, lake, table);
        try {
            GoldImportService.Report report = service.run(new GoldImportService.Request(start, end, resolutions));
            report.files().forEach(f -> log.info("  {} {} - {}", f.status(), f.key(), f.detail()));
            log.info("Importacao CONCLUIDA: {} linhas carregadas", report.loaded());
        } catch (GoldImportService.ImportFailedException e) {
            e.report().files().forEach(f -> log.error("  {} {} - {}", f.status(), f.key(), f.detail()));
            log.error("Importacao NAO CONCLUIDA: {}", e.getMessage());
            throw e; // codigo de saida != 0: nao parece sucesso
        } finally {
            lake.close();
        }
    }

    private static String required(ApplicationArguments a, String name) {
        List<String> v = a.getOptionValues(name);
        if (v == null || v.isEmpty()) throw new IllegalArgumentException("argumento obrigatorio: --" + name);
        return v.get(0);
    }
}
