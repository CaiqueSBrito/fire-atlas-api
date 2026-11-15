package com.fireatlas.lake.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

/** Pequenos manifestos e intencoes de execucao sobrevivem a reinicios; nunca guarda linhas da gold. */
@Component
public class AthenaExportStore {
    public record Key(String snapshot, String kind, int resolution) {}
    public record Export(Key key, String token, String prefix, String queryId, String state, List<String> files) {}

    private static final ObjectMapper JSON = new ObjectMapper();
    private final JdbcTemplate jdbc;

    public AthenaExportStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Export find(Key key) {
        var rows = jdbc.query("""
                SELECT client_token, output_prefix, query_id, state, files::text AS files
                FROM annual_athena_export WHERE snapshot_id=? AND kind=? AND resolution=?
                """, (rs, row) -> {
            String files = rs.getString("files");
            try {
                return new Export(key, rs.getString("client_token"), rs.getString("output_prefix"),
                        rs.getString("query_id"), rs.getString("state"), files == null ? List.of()
                        : List.copyOf(JSON.readValue(files, new TypeReference<List<String>>() {})));
            } catch (IOException e) {
                throw new java.sql.SQLException("manifesto Athena duravel invalido", e);
            }
        }, key.snapshot(), key.kind(), key.resolution());
        return rows.isEmpty() ? null : rows.getFirst();
    }

    /** Chamado sob o lock global do bootstrap; apenas falhas terminais recebem nova tentativa. */
    public Export begin(Key key, String token, String prefix) {
        jdbc.update("""
                INSERT INTO annual_athena_export(snapshot_id,kind,resolution,client_token,output_prefix,state)
                VALUES (?,?,?,?,?,'pending')
                ON CONFLICT (snapshot_id,kind,resolution) DO UPDATE SET client_token=EXCLUDED.client_token,
                    output_prefix=EXCLUDED.output_prefix,query_id=NULL,state='pending',manifest_location=NULL,
                    files=NULL,updated_at=now() WHERE annual_athena_export.state='failed'
                """, key.snapshot(), key.kind(), key.resolution(), token, prefix);
        return find(key);
    }

    public void started(Export export, String queryId) {
        jdbc.update("""
                UPDATE annual_athena_export SET query_id=?,state='running',updated_at=now()
                WHERE snapshot_id=? AND kind=? AND resolution=? AND client_token=?
                """, queryId, export.key().snapshot(), export.key().kind(), export.key().resolution(), export.token());
    }

    public void complete(Export export, String manifest, List<String> files) throws IOException {
        jdbc.update("""
                UPDATE annual_athena_export SET state='complete',manifest_location=?,files=CAST(? AS jsonb),updated_at=now()
                WHERE snapshot_id=? AND kind=? AND resolution=? AND client_token=?
                """, manifest, JSON.writeValueAsString(files), export.key().snapshot(), export.key().kind(),
                export.key().resolution(), export.token());
    }

    public void failed(Export export) {
        jdbc.update("""
                UPDATE annual_athena_export SET state='failed',updated_at=now()
                WHERE snapshot_id=? AND kind=? AND resolution=? AND client_token=? AND state<>'complete'
                """, export.key().snapshot(), export.key().kind(), export.key().resolution(), export.token());
    }
}
