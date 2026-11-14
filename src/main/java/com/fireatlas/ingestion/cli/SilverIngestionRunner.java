package com.fireatlas.ingestion.cli;

import com.fireatlas.ingestion.repository.SilverIngestionRepository;
import com.fireatlas.ingestion.service.SilverImportService;
import com.fireatlas.lake.integration.S3LakeStorage;
import com.fireatlas.region.entity.BBox;
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
 * Importacao dos focos da prata ({@code prata/silver}) para a tabela {@code detection} (profile "ingest-silver"):
 * le o recorte pedido do data lake e grava os focos individuais, com o record_id como identidade. Se a importacao nao
 * for concluida, o processo termina com erro (codigo de saida diferente de zero).
 *
 * <pre>
 * java -jar target/fire-atlas-api-0.1.0.jar --spring.profiles.active=ingest-silver \
 *   --ingest.start=2026-11-01 --ingest.end=2026-11-14 [--ingest.bbox=-74,-34,-34,6 | --ingest.bbox=global]
 * </pre>
 * A regiao padrao e o Brasil (a prata e global e muito grande); use {@code global} para nao filtrar.
 */
@Component
@Profile("ingest-silver")
public class SilverIngestionRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SilverIngestionRunner.class);
    static final String DEFAULT_BBOX = "-74,-34,-34,6";

    private final SilverIngestionRepository repository;
    private final S3LakeStorage lake;
    private final String table;

    public SilverIngestionRunner(SilverIngestionRepository repository, @Lazy S3LakeStorage lake,
                                 @Value("${fireatlas.ingest.silver-table:prata/silver/}") String table) {
        this.repository = repository;
        this.lake = lake;
        this.table = table;
    }

    @Override
    public void run(ApplicationArguments args) {
        LocalDate start = LocalDate.parse(required(args, "ingest.start"));
        LocalDate end = LocalDate.parse(required(args, "ingest.end"));
        String bbox = args.containsOption("ingest.bbox") ? required(args, "ingest.bbox") : DEFAULT_BBOX;
        var request = new SilverImportService.Request(start, end,
                "global".equalsIgnoreCase(bbox) ? null : BBox.parse(bbox));
        try {
            var report = new SilverImportService(repository, lake, table).run(request);
            report.files().forEach(f -> log.info("  {} {} - {}", f.status(), f.key(), f.detail()));
            log.info("Importacao CONCLUIDA: {} focos novos", report.inserted());
        } catch (SilverImportService.ImportFailedException e) {
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
