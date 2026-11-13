package com.fireatlas.ingestion;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.ArrayList;
import java.util.List;

/** Acesso somente leitura ao data lake. Credenciais pela cadeia padrao da AWS (perfil/env/role). */
@Component
@Profile("ingest")
public class DatalakeClient implements AutoCloseable {

    private final S3Client s3;
    private final String bucket;

    public DatalakeClient(@Value("${fireatlas.s3.bucket}") String bucket,
                          @Value("${fireatlas.s3.region}") String region) {
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalStateException("Defina DATALAKE_BUCKET (nome do bucket do data lake)");
        }
        this.bucket = bucket;
        this.s3 = S3Client.builder().region(Region.of(region)).build();
    }

    public List<String> listCsvKeys(String prefix) {
        List<String> keys = new ArrayList<>();
        String token = null;
        do {
            var resp = s3.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucket).prefix(prefix).continuationToken(token).build());
            for (S3Object o : resp.contents()) {
                if (o.key().endsWith(".csv")) keys.add(o.key());
            }
            token = resp.isTruncated() ? resp.nextContinuationToken() : null;
        } while (token != null);
        return keys;
    }

    public ResponseInputStream<GetObjectResponse> open(String key) {
        return s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
    }

    @Override
    public void close() {
        s3.close();
    }
}
