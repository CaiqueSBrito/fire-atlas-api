package com.fireatlas.gold.ingestion.cli;

import com.fireatlas.gold.ingestion.repository.IngestionRepository;
import com.fireatlas.gold.ingestion.service.LakeSyncService;
import com.fireatlas.lake.integration.S3LakeStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Ao subir a API, sincroniza a gold do data lake com o PostGIS em segundo plano ({@link LakeSyncService}): a API ja
 * responde enquanto os dados entram. Nao roda nos profiles de importacao manual. Desligada com
 * {@code LAKE_SYNC_ENABLED=false} ou sem {@code DATALAKE_BUCKET}.
 */
@Component
@Profile("!ingest-gold & !ingest-silver")
public class LakeSyncRunner {

    private static final Logger log = LoggerFactory.getLogger(LakeSyncRunner.class);

    private final IngestionRepository repository;
    private final boolean enabled;
    private final String start;
    private final int overlapDays;
    private final String bucket;
    private final String region;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;

    public LakeSyncRunner(IngestionRepository repository,
                          @Value("${fireatlas.sync.enabled:true}") boolean enabled,
                          @Value("${fireatlas.sync.start:}") String start,
                          @Value("${fireatlas.sync.overlap-days:7}") int overlapDays,
                          @Value("${fireatlas.s3.bucket:}") String bucket,
                          @Value("${fireatlas.s3.region}") String region,
                          @Value("${fireatlas.s3.access-key-id:}") String accessKeyId,
                          @Value("${fireatlas.s3.secret-access-key:}") String secretAccessKey,
                          @Value("${fireatlas.s3.session-token:}") String sessionToken) {
        this.repository = repository;
        this.enabled = enabled;
        this.start = start;
        this.overlapDays = overlapDays;
        this.bucket = bucket;
        this.region = region;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!enabled) {
            log.info("Sincronizacao com o data lake desligada (LAKE_SYNC_ENABLED=false)");
            return;
        }
        if (bucket.isBlank()) {
            log.warn("Sincronizacao com o data lake desligada: defina DATALAKE_BUCKET (variavel ou .env)");
            return;
        }
        Thread.ofPlatform().name("lake-sync").daemon(true).start(this::sync);
    }

    private void sync() {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        try (var lake = new S3LakeStorage(bucket, region, accessKeyId, secretAccessKey, sessionToken)) {
            LocalDate from = start.isBlank() ? today.withDayOfYear(1) : LocalDate.parse(start.strip());
            log.info("Sincronizacao com o data lake iniciada (tabela sem carga anterior comeca em {})", from);
            var results = new LakeSyncService(repository, lake, from, overlapDays).run(today);
            results.forEach(r -> log.info("  {} {} - {}", r.status(), r.table(), r.detail()));
            log.info("Sincronizacao com o data lake terminada");
        } catch (RuntimeException e) {
            log.error("Sincronizacao com o data lake NAO iniciada: {}: {}", e.getClass().getSimpleName(),
                    e.getMessage());
        }
    }
}
