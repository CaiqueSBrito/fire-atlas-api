package com.fireatlas.ingestion.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Acesso somente leitura ao data lake. Credenciais pela cadeia padrao da AWS (perfil/env/role).
 * Le CSV bruto (bronze) em streaming e Parquet (ouro) baixando o arquivo para uma copia temporaria local,
 * apagada ao terminar.
 */
@Component
@Profile("ingest")
@org.springframework.context.annotation.Lazy
public class DatalakeClient implements LakeSource, AutoCloseable {

    private final S3Client s3;
    private final String bucket;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, Path> downloads = new HashMap<>();

    public DatalakeClient(@Value("${fireatlas.s3.bucket}") String bucket,
                          @Value("${fireatlas.s3.region}") String region) {
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalStateException("Defina DATALAKE_BUCKET (nome do bucket do data lake)");
        }
        this.bucket = bucket;
        this.s3 = S3Client.builder().region(Region.of(region)).build();
    }

    @Override
    public List<String> listKeys(String prefix) {
        List<String> keys = new ArrayList<>();
        for (S3Object o : listAll(prefix)) {
            if (o.key().endsWith(".csv") || o.key().endsWith(".parquet")) keys.add(o.key());
        }
        return keys;
    }

    /** CSV: leitura em streaming (o fechamento aborta a conexao se nao chegou ao fim). Parquet: filtro na leitura. */
    @Override
    public InputStream open(String key, DetectionCsvLoader.Filter filter) throws IOException {
        if (isParquet(key)) {
            InputStream csv = ParquetCsvReader.openAsCsv(download(key), filter);
            return new FilterInputStream(csv) {
                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        release(key);
                    }
                }
            };
        }
        return new AbortingStream(s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build()));
    }

    /**
     * CSV bruto: procura, na pasta, o validation.json que declara o arquivo como membro.
     * Parquet: usa o rodape do proprio arquivo (nao ha sha256).
     */
    @Override
    public Optional<Manifest> manifest(String key) throws IOException {
        if (isParquet(key)) {
            ParquetCsvReader.Info info = ParquetCsvReader.info(download(key));
            return Optional.of(new Manifest(info.rows(), info.minDate(), info.maxDate(), null, false));
        }
        int slash = key.lastIndexOf('/');
        String folder = key.substring(0, slash + 1);
        String file = key.substring(slash + 1);
        for (S3Object o : listAll(folder)) {
            if (!o.key().endsWith(".validation.json")) continue;
            byte[] bytes = s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(o.key()).build())
                    .asByteArray();
            JsonNode root = mapper.readTree(bytes);
            for (JsonNode member : root.path("members")) {
                if (!file.equals(member.path("member").asText())) continue;
                if (!member.has("rows") || !member.has("observed_min") || !member.has("observed_max")) continue;
                return Optional.of(new Manifest(member.get("rows").asLong(),
                        LocalDate.parse(member.get("observed_min").asText()),
                        LocalDate.parse(member.get("observed_max").asText()),
                        root.path("sha256").asText(null)));
            }
        }
        return Optional.empty();
    }

    /** Apaga a copia temporaria de um Parquet (se houver). */
    @Override
    public void release(String key) {
        Path file = downloads.remove(key);
        if (file == null) return;
        try {
            Files.deleteIfExists(file);
            Files.deleteIfExists(file.getParent());
        } catch (IOException ignored) {
            // limpeza de temporarios: nao deve mascarar o resultado da importacao
        }
    }

    private static boolean isParquet(String key) {
        return key.endsWith(".parquet");
    }

    /** Baixa o objeto para um arquivo temporario (uma vez por chave). */
    private Path download(String key) throws IOException {
        Path existing = downloads.get(key);
        if (existing != null) return existing;
        Path dir = Files.createTempDirectory("fireatlas-lake-");
        Path file = dir.resolve(key.substring(key.lastIndexOf('/') + 1));
        try {
            s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                    ResponseTransformer.toFile(file));
        } catch (RuntimeException e) {
            Files.deleteIfExists(file);
            Files.deleteIfExists(dir);
            throw e;
        }
        downloads.put(key, file);
        return file;
    }

    private List<S3Object> listAll(String prefix) {
        List<S3Object> out = new ArrayList<>();
        String token = null;
        do {
            var resp = s3.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucket).prefix(prefix).continuationToken(token).build());
            out.addAll(resp.contents());
            token = resp.isTruncated() ? resp.nextContinuationToken() : null;
        } while (token != null);
        return out;
    }

    @Override
    public void close() {
        new ArrayList<>(downloads.keySet()).forEach(this::release);
        s3.close();
    }

    /** O fechamento aborta a conexao se o arquivo nao foi lido ate o fim (evita baixar o restante). */
    private static final class AbortingStream extends FilterInputStream {
        private final ResponseInputStream<GetObjectResponse> source;
        private boolean endReached;

        AbortingStream(ResponseInputStream<GetObjectResponse> source) {
            super(source);
            this.source = source;
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b < 0) endReached = true;
            return b;
        }

        @Override
        public int read(byte[] buffer, int off, int len) throws IOException {
            int n = super.read(buffer, off, len);
            if (n < 0) endReached = true;
            return n;
        }

        @Override
        public void close() throws IOException {
            if (!endReached) source.abort();
            super.close();
        }
    }
}
