package com.fireatlas.ingestion.cli;

import com.fireatlas.ingestion.integration.DatalakeClient;
import com.fireatlas.ingestion.integration.DetectionCsvLoader;
import com.fireatlas.ingestion.integration.H3Options;
import com.fireatlas.ingestion.repository.IngestionRepository;
import com.fireatlas.ingestion.service.ImportService;
import com.fireatlas.region.entity.BBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Rotina de importacao em linha de comando (profile "ingest"): le o recorte pedido do data lake e grava no PostGIS.
 * E uma camada de entrada, como a API REST: so interpreta os argumentos e chama o {@link ImportService}.
 * Se a importacao nao for concluida, o processo termina com erro (codigo de saida diferente de zero).
 *
 * <pre>
 * java -jar target/fire-atlas-api-0.1.0.jar --spring.profiles.active=ingest \
 *   --ingest.start=2024-08-01 --ingest.end=2024-09-30 --ingest.bbox=-74,-34,-34,6 --ingest.sensors=MODIS,VIIRS
 * </pre>
 * Opcional: --ingest.products=MODIS_C6.1,VIIRS_S-NPP e, como contingencia, --ingest.file=caminho.csv com
 * --ingest.product=VIIRS_S-NPP (CSV local baixado previamente). Veja scripts/importar.sh.
 */
@Component
@Profile("ingest")
public class IngestionRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(IngestionRunner.class);

    private final IngestionRepository repository;
    private final DatalakeClient lake;
    private final String approvedProducts;
    private final String h3Column;
    private final String prefix;
    private final int baseResolution;

    public IngestionRunner(IngestionRepository repository, @Lazy DatalakeClient lake,
                           @Value("${fireatlas.ingest.approved-products}") String approvedProducts,
                           @Value("${fireatlas.ingest.h3-column}") String h3Column,
                           @Value("${fireatlas.ingest.prefix}") String prefix,
                           @Value("${fireatlas.h3.base-resolution}") int baseResolution) {
        this.repository = repository;
        this.lake = lake;
        this.approvedProducts = approvedProducts;
        this.h3Column = h3Column;
        this.prefix = prefix;
        this.baseResolution = baseResolution;
    }

    @Override
    public void run(ApplicationArguments args) {
        LocalDate start = LocalDate.parse(required(args, "ingest.start"));
        LocalDate end = LocalDate.parse(required(args, "ingest.end"));
        BBox bbox = BBox.parse(opt(args, "ingest.bbox", "-74,-34,-34,6")); // Brasil por padrao
        List<String> approved = Arrays.stream(approvedProducts.split(",")).map(String::trim).toList();
        var service = new ImportService(repository, lake, approved, new H3Options(h3Column, baseResolution), prefix);

        if (args.containsOption("ingest.file")) {
            Path path = Path.of(required(args, "ingest.file"));
            var result = service.importLocalFile(path, required(args, "ingest.product"),
                    new DetectionCsvLoader.Filter(bbox, start, end));
            log.info("Resultado: {} - {} ({} carregadas, {} rejeitadas)", result.status(), result.detail(),
                    result.loaded(), result.rejected());
            if (result.status() == ImportService.Status.FAILED) {
                throw new ImportService.ImportFailedException("importacao NAO concluida: " + result.detail(),
                        new ImportService.Report(List.of(result)));
            }
            return;
        }

        Set<String> sensors = new LinkedHashSet<>(List.of(
                opt(args, "ingest.sensors", "MODIS,VIIRS").toUpperCase(Locale.ROOT).split(",")));
        Set<String> products = args.containsOption("ingest.products")
                ? new LinkedHashSet<>(List.of(required(args, "ingest.products").split(","))) : null;
        try {
            ImportService.Report report = service.run(new ImportService.Request(start, end, bbox, sensors, products));
            report.files().forEach(f -> log.info("  {} {} - {}", f.status(), f.key(), f.detail()));
            log.info("Importacao CONCLUIDA: {} registros carregados", report.loaded());
        } catch (ImportService.ImportFailedException e) {
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

    private static String opt(ApplicationArguments a, String name, String def) {
        List<String> v = a.getOptionValues(name);
        return v == null || v.isEmpty() ? def : v.get(0);
    }
}
