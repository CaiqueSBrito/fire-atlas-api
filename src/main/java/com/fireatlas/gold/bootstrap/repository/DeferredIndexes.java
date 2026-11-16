package com.fireatlas.gold.bootstrap.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

/**
 * Carga do zero: indices, chave primaria e chave estrangeira saem antes do COPY e voltam no fim, construidos de uma vez
 * (ordenados, em paralelo) em vez de mantidos linha a linha. As definicoes ficam em tabela com WAL: se a API cair no
 * meio, a proxima carga continua sem eles e os recria antes do {@code ready}; consultas so ocorrem depois do ready.
 */
final class DeferredIndexes {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DeferredIndexes.class);

    record Deferred(String name, String table, String kind, String createDdl, String attachDdl) {}

    private DeferredIndexes() {}

    static boolean hasDeferred(JdbcTemplate jdbc, String table) {
        return Boolean.TRUE.equals(jdbc.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM annual_cache_deferred_index WHERE table_name=?)", Boolean.class, table));
    }

    /** Um passo por indice confirmado, vinculacao de PK/UNIQUE e validacao de FK. */
    static long finalizationWork(JdbcTemplate jdbc) {
        return jdbc.queryForObject("SELECT coalesce(sum(CASE WHEN attach_ddl IS NULL THEN 1 ELSE 2 END), 0)"
                + " FROM annual_cache_deferred_index", Long.class);
    }

    /** Registra (uma vez) e remove indices, PK/UNIQUE e FKs da tabela. Chamar dentro de uma transacao. */
    static void defer(JdbcTemplate jdbc, String table) {
        // Indices: os que sustentam PK/UNIQUE voltam como indice + ADD CONSTRAINT ... USING INDEX (bloqueio curto).
        jdbc.update("""
                INSERT INTO annual_cache_deferred_index (index_name, table_name, kind, create_ddl, attach_ddl)
                SELECT i.relname, ?, CASE WHEN c.oid IS NULL THEN 'index' ELSE 'constraint_index' END,
                       pg_get_indexdef(x.indexrelid),
                       CASE WHEN c.oid IS NULL THEN NULL ELSE 'ALTER TABLE ' || x.indrelid::regclass || ' ADD CONSTRAINT '
                           || quote_ident(c.conname) || CASE c.contype WHEN 'p' THEN ' PRIMARY KEY' ELSE ' UNIQUE' END
                           || ' USING INDEX ' || quote_ident(i.relname) END
                FROM pg_index x
                JOIN pg_class i ON i.oid = x.indexrelid
                LEFT JOIN pg_constraint c ON c.conindid = x.indexrelid AND c.conrelid = x.indrelid AND c.contype IN ('p', 'u')
                WHERE x.indrelid = CAST(? AS regclass)
                ON CONFLICT (index_name) DO NOTHING
                """, table, table);
        // Chaves estrangeiras: cada linha disparava uma verificacao; voltam validadas por uma unica consulta.
        jdbc.update("""
                INSERT INTO annual_cache_deferred_index (index_name, table_name, kind, create_ddl, attach_ddl)
                SELECT c.conname, ?, 'constraint',
                       'ALTER TABLE ' || c.conrelid::regclass || ' ADD CONSTRAINT ' || quote_ident(c.conname) || ' '
                           || pg_get_constraintdef(c.oid), NULL
                FROM pg_constraint c WHERE c.conrelid = CAST(? AS regclass) AND c.contype = 'f'
                ON CONFLICT (index_name) DO NOTHING
                """, table, table);
        for (var deferred : list(jdbc, table)) {
            switch (deferred.kind()) {
                case "constraint", "constraint_index" -> {
                    if (constraintExists(jdbc, table, deferred.name(), deferred.kind())) {
                        jdbc.execute("ALTER TABLE " + table + " DROP CONSTRAINT " + quote(deferred.name()));
                    }
                }
                default -> jdbc.execute("DROP INDEX IF EXISTS " + quote(deferred.name()));
            }
        }
    }

    /**
     * Recria tudo o que foi adiado: indices em paralelo (conexoes proprias), depois PK/UNIQUE e por fim FKs.
     * Idempotente: retomar depois de uma falha pula o que ja existe. Duplicata na chave primaria falha aqui.
     */
    static void restore(DataSource dataSource, JdbcTemplate coordinator, int threads, BiConsumer<String, String> progress,
                        Runnable completed) throws Exception {
        List<Deferred> all = list(coordinator, null);
        if (all.isEmpty()) return;
        List<Deferred> indexes = all.stream().filter(d -> !d.kind().equals("constraint")).toList();
        progress.accept("indexes", "Recriando " + indexes.size() + " indices do cache em paralelo");
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, Math.min(threads, indexes.size())),
                runnable -> new Thread(runnable, "annual-cache-index-2026"));
        try {
            List<Future<?>> builds = new ArrayList<>();
            for (var index : indexes) {
                builds.add(pool.submit(() -> {
                    try (Connection connection = dataSource.getConnection()) {
                        var jdbc = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
                        if (!exists(jdbc, index.name(), index.table())) {
                            // 1 GB + 4 processos para a grade foi medido: PK 44 s -> 42 s, com swap-out; nao compensa.
                            jdbc.execute("SET maintenance_work_mem = '256MB'");
                            jdbc.execute("SET max_parallel_maintenance_workers = 2");
                            long started = System.nanoTime();
                            jdbc.execute(index.createDdl());
                            log.info("Indice {} recriado em {} ms", index.name(), elapsedMs(started));
                        }
                        completed.run();
                    }
                    return null;
                }));
            }
            for (var build : builds) {
                try {
                    build.get();
                } catch (ExecutionException e) {
                    builds.forEach(other -> other.cancel(true));
                    throw e.getCause() instanceof Exception cause ? cause : e;
                }
            }
        } finally {
            pool.shutdownNow();
            boolean interrupted = Thread.interrupted();
            while (!pool.isTerminated()) {
                try {
                    pool.awaitTermination(1, java.util.concurrent.TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }
        progress.accept("validating", "Validando chaves do cache");
        for (var deferred : all) {
            if (deferred.attachDdl() != null && !constraintExists(coordinator, deferred.table(), deferred.name(), deferred.kind())) {
                long started = System.nanoTime();
                coordinator.execute(deferred.attachDdl());
                log.info("Chave {} ligada ao indice em {} ms", deferred.name(), elapsedMs(started));
            }
            if (deferred.attachDdl() != null) completed.run();
        }
        for (var deferred : all) {
            if (deferred.kind().equals("constraint")
                    && !constraintExists(coordinator, deferred.table(), deferred.name(), deferred.kind())) {
                long started = System.nanoTime();
                coordinator.execute(deferred.createDdl());
                log.info("Chave estrangeira {} validada em {} ms", deferred.name(), elapsedMs(started));
            }
            if (deferred.kind().equals("constraint")) completed.run();
        }
        coordinator.update("DELETE FROM annual_cache_deferred_index");
    }

    private static List<Deferred> list(JdbcTemplate jdbc, String table) {
        String sql = "SELECT index_name, table_name, kind, create_ddl, attach_ddl FROM annual_cache_deferred_index"
                + (table == null ? "" : " WHERE table_name=?") + " ORDER BY index_name";
        var mapper = (org.springframework.jdbc.core.RowMapper<Deferred>) (rs, row) -> new Deferred(rs.getString(1),
                rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5));
        return table == null ? jdbc.query(sql, mapper) : jdbc.query(sql, mapper, table);
    }

    private static boolean exists(JdbcTemplate jdbc, String index, String table) {
        return Boolean.TRUE.equals(jdbc.queryForObject("""
                SELECT EXISTS (SELECT 1 FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid
                    WHERE x.indrelid=CAST(? AS regclass) AND i.relname=?)
                """, Boolean.class, table, index));
    }

    private static long elapsedMs(long started) {
        return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }

    /** constraint_index: a PK/UNIQUE com esse nome; constraint: a FK com esse nome. */
    private static boolean constraintExists(JdbcTemplate jdbc, String table, String name, String kind) {
        return Boolean.TRUE.equals(jdbc.queryForObject("""
                SELECT EXISTS (SELECT 1 FROM pg_constraint
                               WHERE conrelid = CAST(? AS regclass) AND conname = ? AND contype = ANY (?::char[]))
                """, Boolean.class, table, name, kind.equals("constraint") ? "{f}" : "{p,u}"));
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
