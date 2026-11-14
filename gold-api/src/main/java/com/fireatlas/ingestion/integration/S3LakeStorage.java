package com.fireatlas.ingestion.integration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Acesso somente leitura ao data lake no S3. Credenciais pela cadeia padrao da AWS (perfil, variaveis de ambiente
 * ou role): nunca no codigo nem no repositorio. Arquivos Parquet sao baixados para uma copia temporaria local,
 * apagada ao terminar.
 */
@Component
@Profile("ingest")
@Lazy
public class S3LakeStorage implements LakeStorage, AutoCloseable {

    private final S3Client s3;
    private final String bucket;

    public S3LakeStorage(@Value("${fireatlas.s3.bucket}") String bucket,
                         @Value("${fireatlas.s3.region}") String region) {
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalStateException("Defina DATALAKE_BUCKET (nome do bucket do data lake)");
        }
        this.bucket = bucket;
        this.s3 = S3Client.builder().region(Region.of(region)).build();
    }

    @Override
    public List<String> list(String prefix) {
        List<String> keys = new ArrayList<>();
        String token = null;
        do {
            var resp = s3.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucket).prefix(prefix).continuationToken(token).build());
            for (S3Object o : resp.contents()) keys.add(o.key());
            token = resp.isTruncated() ? resp.nextContinuationToken() : null;
        } while (token != null);
        return keys;
    }

    @Override
    public byte[] read(String key) {
        return s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build()).asByteArray();
    }

    @Override
    public Path download(String key) throws IOException {
        Path dir = Files.createTempDirectory("fireatlas-lake-");
        Path file = dir.resolve(key.substring(key.lastIndexOf('/') + 1));
        try {
            s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                    ResponseTransformer.toFile(file));
        } catch (RuntimeException e) {
            release(file);
            throw e;
        }
        return file;
    }

    @Override
    public void release(Path file) {
        try {
            Files.deleteIfExists(file);
            Files.deleteIfExists(file.getParent());
        } catch (IOException ignored) {
            // limpeza de temporarios: nao deve mascarar o resultado da importacao
        }
    }

    @Override
    public void close() {
        s3.close();
    }
}
