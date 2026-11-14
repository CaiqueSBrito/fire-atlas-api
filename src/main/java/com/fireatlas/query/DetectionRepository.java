package com.fireatlas.query;

import com.fireatlas.api.Dto;
import com.fireatlas.domain.BBox;
import com.fireatlas.domain.GridMath;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Consultas ao PostGIS. Toda consulta usa bbox + intervalo de datas (poda de particoes + indice GiST). */
@Repository
public class DetectionRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public DetectionRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static MapSqlParameterSource base(BBox b, LocalDate s, LocalDate e, List<String> sensors) {
        return new MapSqlParameterSource()
                .addValue("minLon", b.minLon()).addValue("minLat", b.minLat())
                .addValue("maxLon", b.maxLon()).addValue("maxLat", b.maxLat())
                .addValue("start", s).addValue("end", e)
                .addValue("sensors", sensors);
    }

    private static MapSqlParameterSource withCells(MapSqlParameterSource p, BBox b) {
        return p.addValue("cx0", GridMath.cx(b.minLon())).addValue("cx1", GridMath.cx(b.maxLon()))
                .addValue("cy0", GridMath.cy(b.minLat())).addValue("cy1", GridMath.cy(b.maxLat()));
    }

    /** Pontos individuais (pede limit+1 para sinalizar truncamento). */
    public List<Dto.Point> points(BBox b, LocalDate s, LocalDate e, List<String> sensors, int limit) {
        String sql = """
                SELECT sensor, satellite, acq_ts, lat, lon, frp, confidence_class, daynight, brightness
                FROM detection
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND geom && ST_MakeEnvelope(:minLon, :minLat, :maxLon, :maxLat, 4326)
                ORDER BY acq_ts
                LIMIT :limit
                """;
        return jdbc.query(sql, base(b, s, e, sensors).addValue("limit", limit + 1), (rs, i) -> new Dto.Point(
                rs.getString("sensor"), rs.getString("satellite"),
                rs.getTimestamp("acq_ts").toInstant(), rs.getDouble("lat"), rs.getDouble("lon"),
                getFloat(rs, "frp"), rs.getString("confidence_class"), rs.getString("daynight"),
                getFloat(rs, "brightness")));
    }

    /** Celulas agregadas a partir da grade diaria (rapido mesmo para periodos longos). */
    public List<Dto.Cell> cells(BBox b, LocalDate s, LocalDate e, List<String> sensors, int k, int limit) {
        String sql = """
                SELECT sensor, (cx / :k) AS gx, (cy / :k) AS gy,
                       sum(n) AS n, sum(frp_sum) AS frp_sum, max(frp_max) AS frp_max
                FROM daily_grid
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND cx BETWEEN :cx0 AND :cx1 AND cy BETWEEN :cy0 AND :cy1
                GROUP BY sensor, gx, gy
                LIMIT :limit
                """;
        MapSqlParameterSource p = withCells(base(b, s, e, sensors), b).addValue("k", k).addValue("limit", limit + 1);
        return jdbc.query(sql, p, (rs, i) -> new Dto.Cell(rs.getString("sensor"),
                GridMath.centerLat(rs.getInt("gy"), k), GridMath.centerLon(rs.getInt("gx"), k),
                rs.getInt("n"), rs.getDouble("frp_sum"), getFloat(rs, "frp_max")));
    }

    public record DayRow(LocalDate date, String sensor, int n, double frpSum) {}

    public List<DayRow> dailyTotals(BBox b, LocalDate s, LocalDate e, List<String> sensors) {
        String sql = """
                SELECT acq_date, sensor, sum(n) AS n, sum(frp_sum) AS frp_sum
                FROM daily_grid
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND cx BETWEEN :cx0 AND :cx1 AND cy BETWEEN :cy0 AND :cy1
                GROUP BY acq_date, sensor
                ORDER BY acq_date, sensor
                """;
        return jdbc.query(sql, withCells(base(b, s, e, sensors), b), (rs, i) -> new DayRow(
                rs.getObject("acq_date", LocalDate.class), rs.getString("sensor"),
                rs.getInt("n"), rs.getDouble("frp_sum")));
    }

    /** Estatisticas exatas por sensor (a partir das deteccoes). */
    public List<Dto.SensorStats> sensorStats(BBox b, LocalDate s, LocalDate e, List<String> sensors) {
        String sql = """
                SELECT sensor,
                       count(*) AS n,
                       count(DISTINCT acq_date) AS days,
                       avg(frp) AS frp_mean,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY frp) AS frp_p50,
                       max(frp) AS frp_max,
                       coalesce(sum(frp), 0) AS frp_total,
                       count(*) FILTER (WHERE confidence_class = 'l') AS conf_low,
                       count(*) FILTER (WHERE confidence_class = 'n') AS conf_nominal,
                       count(*) FILTER (WHERE confidence_class = 'h') AS conf_high
                FROM detection
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND geom && ST_MakeEnvelope(:minLon, :minLat, :maxLon, :maxLat, 4326)
                GROUP BY sensor
                ORDER BY sensor
                """;
        return jdbc.query(sql, base(b, s, e, sensors), (rs, i) -> {
            Map<String, Long> conf = new LinkedHashMap<>();
            conf.put("low", rs.getLong("conf_low"));
            conf.put("nominal", rs.getLong("conf_nominal"));
            conf.put("high", rs.getLong("conf_high"));
            String sensor = rs.getString("sensor");
            return new Dto.SensorStats(sensor, rs.getLong("n"), rs.getInt("days"),
                    getDouble(rs, "frp_mean"), getDouble(rs, "frp_p50"), getFloat(rs, "frp_max"),
                    rs.getDouble("frp_total"), conf, "MODIS".equals(sensor) ? 1000 : 375);
        });
    }

    public record HarmonizationWindowTotals(long modisTotalInWindow, long viirsTotalInWindow) {}

    public record HarmonizationQueryResult(
            long modisTotalInWindow,
            long viirsTotalInWindow,
            long modisEvaluated,
            long modisEvaluatedWithFrp,
            long modisEvaluatedWithoutFrp,
            long modisWithCandidateMatch,
            long distinctViirsMatched,
            long candidateAssociationsTotal,
            long distinctViirsMatchedWithFrp,
            long distinctViirsMatchedWithoutFrp,
            boolean incompleteProvenanceDetected,
            boolean legacyUnverifiedSemanticHashDetected,
            boolean upgradedLegacySemanticHashDetected,
            boolean associationsTruncated,
            List<Dto.HarmonizationResponse.CandidateAssociation> associations
    ) {
        public HarmonizationQueryResult(
                long modisTotalInWindow,
                long viirsTotalInWindow,
                long modisEvaluated,
                long modisEvaluatedWithFrp,
                long modisEvaluatedWithoutFrp,
                long modisWithCandidateMatch,
                long distinctViirsMatched,
                long candidateAssociationsTotal,
                long distinctViirsMatchedWithFrp,
                long distinctViirsMatchedWithoutFrp,
                boolean incompleteProvenanceDetected,
                boolean associationsTruncated,
                List<Dto.HarmonizationResponse.CandidateAssociation> associations) {
            this(modisTotalInWindow, viirsTotalInWindow, modisEvaluated, modisEvaluatedWithFrp,
                    modisEvaluatedWithoutFrp, modisWithCandidateMatch, distinctViirsMatched,
                    candidateAssociationsTotal, distinctViirsMatchedWithFrp, distinctViirsMatchedWithoutFrp,
                    incompleteProvenanceDetected, false, false, associationsTruncated, associations);
        }
    }

    public void setTransactionStatementTimeoutMs(long timeoutMs) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs deve ser positivo");
        }
        jdbc.getJdbcTemplate().execute("SET LOCAL statement_timeout = " + timeoutMs);
    }

    public HarmonizationWindowTotals harmonizationWindowTotals(BBox b, LocalDate s, LocalDate e) {
        String sql = """
                SELECT
                    count(*) FILTER (WHERE sensor = 'MODIS') AS modis_total,
                    count(*) FILTER (WHERE sensor = 'VIIRS') AS viirs_total
                FROM detection
                WHERE acq_date BETWEEN :start AND :end
                  AND geom && ST_MakeEnvelope(:minLon, :minLat, :maxLon, :maxLat, 4326)
                  AND lon BETWEEN :minLon AND :maxLon
                  AND lat BETWEEN :minLat AND :maxLat
                """;
        return jdbc.queryForObject(sql, base(b, s, e, List.of("MODIS", "VIIRS")), (rs, i) ->
                new HarmonizationWindowTotals(rs.getLong("modis_total"), rs.getLong("viirs_total")));
    }

    /**
     * Margem numerica usada exclusivamente no pre-filtro GiST (ST_DWithin) para compensar divergencias
     * de limite exato entre bounding/esfera do indice geocentrico e a distancia geodesica WGS84 autoritativa
     * aplicada por ST_Distance <= :dist (sem ampliar o limiar cientifico final).
     */
    public static final double SPATIAL_PREFILTER_TOLERANCE_METERS = 1.0;

    /**
     * Correspondencia exploratoria MODIS x VIIRS (N:M) com identidade composta (acq_date, id),
     * distancia geodesica WGS84 exata, poda de particoes e preservacao integral de origem e FRP/confianca originais.
     */
    public HarmonizationQueryResult findExploratoryAssociations(
            BBox b, LocalDate s, LocalDate e,
            int maxDistM, int maxMinutes, int modisCap,
            boolean includeAssociations, int associationsLimit) {
        HarmonizationWindowTotals totals = harmonizationWindowTotals(b, s, e);
        return findExploratoryAssociations(
                totals, b, s, e, maxDistM, maxMinutes, modisCap, includeAssociations, associationsLimit);
    }

    public HarmonizationQueryResult findExploratoryAssociations(
            HarmonizationWindowTotals totals,
            BBox b, LocalDate s, LocalDate e,
            int maxDistM, int maxMinutes, int modisCap,
            boolean includeAssociations, int associationsLimit) {
        if (totals.modisTotalInWindow() == 0) {
            return new HarmonizationQueryResult(
                    0, totals.viirsTotalInWindow(),
                    0, 0, 0, 0, 0, 0, 0, 0,
                    false, false, false, false,
                    includeAssociations ? List.of() : null
            );
        }

        int assocLimitPlusOne = includeAssociations ? associationsLimit + 1 : 0;
        String sql = """
                WITH m AS MATERIALIZED (
                    SELECT id, sensor, satellite, acq_date, acq_ts, lat, lon, geom, frp,
                           confidence_raw, confidence_class, daynight, version, source_product, source_file,
                           source_line_number, source_row_hash, source_file_sha256, source_semantic_hash,
                           source_semantic_hash_version, source_semantic_hash_status, legacy_source_semantic_hash
                    FROM detection
                    WHERE sensor = 'MODIS'
                      AND acq_date BETWEEN :start AND :end
                      AND geom && ST_MakeEnvelope(:minLon, :minLat, :maxLon, :maxLat, 4326)
                      AND lon BETWEEN :minLon AND :maxLon
                      AND lat BETWEEN :minLat AND :maxLat
                    ORDER BY acq_ts ASC, acq_date ASC, id ASC
                    LIMIT :cap
                ),
                p AS MATERIALIZED (
                    SELECT m.acq_date AS m_acq_date, m.id AS m_id, m.sensor AS m_sensor, m.satellite AS m_satellite,
                           m.acq_ts AS m_acq_ts, m.lat AS m_lat, m.lon AS m_lon, m.frp AS m_frp,
                           m.confidence_raw AS m_conf_raw, m.confidence_class AS m_conf_class,
                           m.daynight AS m_daynight, m.version AS m_version,
                           m.source_product AS m_product, m.source_file AS m_file,
                           m.source_line_number AS m_source_line, m.source_row_hash AS m_source_row_hash,
                           m.source_file_sha256 AS m_file_sha256, m.source_semantic_hash AS m_semantic_hash,
                           m.source_semantic_hash_version AS m_semantic_hash_version,
                           m.source_semantic_hash_status AS m_semantic_hash_status,
                           m.legacy_source_semantic_hash AS m_legacy_semantic_hash,
                           v.acq_date AS v_acq_date, v.id AS v_id, v.sensor AS v_sensor, v.satellite AS v_satellite,
                           v.acq_ts AS v_acq_ts, v.lat AS v_lat, v.lon AS v_lon, v.frp AS v_frp,
                           v.confidence_raw AS v_conf_raw, v.confidence_class AS v_conf_class,
                           v.daynight AS v_daynight, v.version AS v_version,
                           v.source_product AS v_product, v.source_file AS v_file,
                           v.source_line_number AS v_source_line, v.source_row_hash AS v_source_row_hash,
                           v.source_file_sha256 AS v_file_sha256, v.source_semantic_hash AS v_semantic_hash,
                           v.source_semantic_hash_version AS v_semantic_hash_version,
                           v.source_semantic_hash_status AS v_semantic_hash_status,
                           v.legacy_source_semantic_hash AS v_legacy_semantic_hash,
                           ST_Distance(v.geom::geography, m.geom::geography) AS distance_meters,
                           round(extract(epoch FROM (v.acq_ts - m.acq_ts)))::bigint AS time_offset_seconds
                    FROM m
                    JOIN detection v ON v.sensor = 'VIIRS'
                      AND v.acq_date BETWEEN :viirsStart AND :viirsEnd
                      AND v.acq_date BETWEEN m.acq_date - 1 AND m.acq_date + 1
                      AND v.acq_ts >= m.acq_ts - make_interval(mins => :maxMinutes)
                      AND v.acq_ts <= m.acq_ts + make_interval(mins => :maxMinutes)
                      AND ST_DWithin(v.geom::geography, m.geom::geography, :dist + :prefilterToleranceMeters)
                      AND ST_Distance(v.geom::geography, m.geom::geography) <= :dist
                ),
                m_stats AS (
                    SELECT count(*) AS modis_evaluated,
                           count(*) FILTER (WHERE frp IS NOT NULL) AS modis_with_frp,
                           count(*) FILTER (WHERE frp IS NULL) AS modis_without_frp,
                           coalesce(bool_or(
                               version IS NULL OR btrim(version) = '' OR lower(btrim(version)) = 'unknown'
                               OR btrim(satellite) = ''
                               OR btrim(source_product) = '' OR lower(btrim(source_product)) = 'unknown'
                               OR btrim(source_file) = '' OR lower(btrim(source_file)) = 'unknown'
                               OR confidence_raw IS NULL OR btrim(confidence_raw) = ''
                               OR daynight IS NULL OR btrim(daynight) = ''
                               OR source_line_number IS NULL
                               OR source_row_hash IS NULL OR btrim(source_row_hash) = ''
                               OR source_file_sha256 IS NULL OR btrim(source_file_sha256) = ''
                               OR source_semantic_hash IS NULL OR btrim(source_semantic_hash) = ''
                           ), false) AS modis_incomplete_prov,
                           coalesce(bool_or(source_semantic_hash_status = 'LEGACY_UNVERIFIED'), false) AS modis_legacy_unverified,
                           coalesce(bool_or(source_semantic_hash_status = 'UPDATED_FROM_LEGACY'), false) AS modis_legacy_upgraded
                    FROM m
                ),
                p_stats AS (
                    SELECT count(DISTINCT (m_acq_date, m_id)) AS modis_matched,
                           count(DISTINCT (v_acq_date, v_id)) AS viirs_matched,
                           count(*) AS pairs_total,
                           count(DISTINCT (v_acq_date, v_id)) FILTER (WHERE v_frp IS NOT NULL) AS viirs_with_frp,
                           count(DISTINCT (v_acq_date, v_id)) FILTER (WHERE v_frp IS NULL) AS viirs_without_frp,
                           coalesce(bool_or(
                               v_version IS NULL OR btrim(v_version) = '' OR lower(btrim(v_version)) = 'unknown'
                               OR btrim(v_satellite) = ''
                               OR btrim(v_product) = '' OR lower(btrim(v_product)) = 'unknown'
                               OR btrim(v_file) = '' OR lower(btrim(v_file)) = 'unknown'
                               OR v_conf_raw IS NULL OR btrim(v_conf_raw) = ''
                               OR v_daynight IS NULL OR btrim(v_daynight) = ''
                               OR v_source_line IS NULL
                               OR v_source_row_hash IS NULL OR btrim(v_source_row_hash) = ''
                               OR v_file_sha256 IS NULL OR btrim(v_file_sha256) = ''
                               OR v_semantic_hash IS NULL OR btrim(v_semantic_hash) = ''
                           ), false) AS pair_incomplete_prov,
                           coalesce(bool_or(v_semantic_hash_status = 'LEGACY_UNVERIFIED'), false) AS pair_legacy_unverified,
                           coalesce(bool_or(v_semantic_hash_status = 'UPDATED_FROM_LEGACY'), false) AS pair_legacy_upgraded
                    FROM p
                ),
                top_p AS (
                    SELECT *
                    FROM p
                    ORDER BY m_acq_ts ASC, m_acq_date ASC, m_id ASC,
                             distance_meters ASC, abs(time_offset_seconds) ASC,
                             v_acq_ts ASC, v_acq_date ASC, v_id ASC
                    LIMIT :assocLimitPlusOne
                )
                SELECT ms.modis_evaluated, ms.modis_with_frp, ms.modis_without_frp, ms.modis_incomplete_prov,
                       ms.modis_legacy_unverified, ms.modis_legacy_upgraded,
                       ps.modis_matched, ps.viirs_matched, ps.pairs_total, ps.viirs_with_frp, ps.viirs_without_frp,
                       ps.pair_incomplete_prov, ps.pair_legacy_unverified, ps.pair_legacy_upgraded,
                       tp.*
                FROM m_stats ms
                CROSS JOIN p_stats ps
                LEFT JOIN top_p tp ON true
                ORDER BY tp.m_acq_ts ASC NULLS LAST, tp.m_acq_date ASC NULLS LAST, tp.m_id ASC NULLS LAST,
                         tp.distance_meters ASC NULLS LAST, abs(tp.time_offset_seconds) ASC NULLS LAST,
                         tp.v_acq_ts ASC NULLS LAST, tp.v_acq_date ASC NULLS LAST, tp.v_id ASC NULLS LAST
                """;
        MapSqlParameterSource p = base(b, s, e, List.of("MODIS", "VIIRS"))
                .addValue("cap", modisCap)
                .addValue("viirsStart", s.minusDays(1))
                .addValue("viirsEnd", e.plusDays(1))
                .addValue("maxMinutes", maxMinutes)
                .addValue("dist", (double) maxDistM)
                .addValue("prefilterToleranceMeters", SPATIAL_PREFILTER_TOLERANCE_METERS)
                .addValue("assocLimitPlusOne", assocLimitPlusOne);

        List<Dto.HarmonizationResponse.CandidateAssociation> rows = new ArrayList<>();
        long[] statsHolder = new long[8];
        boolean[] flagsHolder = new boolean[4];

        jdbc.query(sql, p, (ResultSet rs) -> {
            if (!flagsHolder[0]) {
                statsHolder[0] = rs.getLong("modis_evaluated");
                statsHolder[1] = rs.getLong("modis_with_frp");
                statsHolder[2] = rs.getLong("modis_without_frp");
                statsHolder[3] = rs.getLong("modis_matched");
                statsHolder[4] = rs.getLong("viirs_matched");
                statsHolder[5] = rs.getLong("pairs_total");
                statsHolder[6] = rs.getLong("viirs_with_frp");
                statsHolder[7] = rs.getLong("viirs_without_frp");
                flagsHolder[1] = rs.getBoolean("modis_incomplete_prov") || rs.getBoolean("pair_incomplete_prov");
                flagsHolder[2] = rs.getBoolean("modis_legacy_unverified") || rs.getBoolean("pair_legacy_unverified");
                flagsHolder[3] = rs.getBoolean("modis_legacy_upgraded") || rs.getBoolean("pair_legacy_upgraded");
                flagsHolder[0] = true;
            }
            if (includeAssociations && rs.getObject("m_id") != null) {
                long offsetSec = rs.getLong("time_offset_seconds");
                rows.add(new Dto.HarmonizationResponse.CandidateAssociation(
                        "EXPLORATORY_CANDIDATE",
                        rs.getDouble("distance_meters"),
                        offsetSec,
                        Math.abs(offsetSec) / 60.0,
                        readObservation(rs, "m_"),
                        readObservation(rs, "v_")
                ));
            }
        });

        boolean assocTruncated = includeAssociations && rows.size() > associationsLimit;
        List<Dto.HarmonizationResponse.CandidateAssociation> returned =
                includeAssociations ? trim(rows, associationsLimit) : null;

        return new HarmonizationQueryResult(
                totals.modisTotalInWindow(),
                totals.viirsTotalInWindow(),
                statsHolder[0],
                statsHolder[1],
                statsHolder[2],
                statsHolder[3],
                statsHolder[4],
                statsHolder[5],
                statsHolder[6],
                statsHolder[7],
                flagsHolder[1],
                flagsHolder[2],
                flagsHolder[3],
                assocTruncated,
                returned
        );
    }

    private static Dto.HarmonizationResponse.DetectionObservation readObservation(ResultSet rs, String prefix)
            throws SQLException {
        LocalDate acquiredDate = rs.getObject(prefix + "acq_date", LocalDate.class);
        long id = rs.getLong(prefix + "id");
        String observationKey = acquiredDate + ":" + id;
        String sourceFile = trimToEmpty(rs.getString(prefix + "file"));
        Long sourceLineNumber = rs.getObject(prefix + "source_line", Long.class);
        String sourceRowHash = trimToNull(rs.getString(prefix + "source_row_hash"));
        String rawSemanticHash = trimToNull(rs.getString(prefix + "semantic_hash"));
        String sourceFileSha256 = trimToNull(rs.getString(prefix + "file_sha256"));
        String semanticHashVersion = trimToNull(rs.getString(prefix + "semantic_hash_version"));
        String legacySemanticHash = trimToNull(rs.getString(prefix + "legacy_semantic_hash"));
        String semanticHashStatus = trimToNull(rs.getString(prefix + "semantic_hash_status"));
        if (semanticHashStatus == null) {
            semanticHashStatus = rawSemanticHash != null ? "CURRENT"
                    : (legacySemanticHash != null ? "LEGACY_UNVERIFIED" : "UNKNOWN");
        }
        String exposedSemanticHash = "LEGACY_UNVERIFIED".equals(semanticHashStatus) ? null : rawSemanticHash;
        String sourceRawLocator = (sourceLineNumber != null && !sourceFile.isEmpty())
                ? sourceFile + "#L" + sourceLineNumber
                : null;
        String sourceRecordKey = buildSourceRecordKey(
                sourceRawLocator, sourceRowHash, exposedSemanticHash, legacySemanticHash, semanticHashStatus);
        String sensor = rs.getString(prefix + "sensor");
        Instant acquiredAt = rs.getTimestamp(prefix + "acq_ts").toInstant();
        String confidenceSemantics = "MODIS".equals(sensor)
                ? "MODIS_NUMERIC_0_100_NATIVE"
                : "VIIRS_CATEGORICAL_L_N_H_NATIVE";
        return new Dto.HarmonizationResponse.DetectionObservation(
                id,
                acquiredDate,
                observationKey,
                sourceLineNumber,
                sourceRecordKey,
                sensor,
                trimToEmpty(rs.getString(prefix + "satellite")),
                trimToEmpty(rs.getString(prefix + "product")),
                trimToNull(rs.getString(prefix + "version")),
                sourceFile,
                acquiredAt,
                rs.getDouble(prefix + "lat"),
                rs.getDouble(prefix + "lon"),
                getFloat(rs, prefix + "frp"),
                "MW",
                trimToNull(rs.getString(prefix + "conf_raw")),
                trimToNull(rs.getString(prefix + "conf_class")),
                confidenceSemantics,
                trimToNull(rs.getString(prefix + "daynight")),
                sourceRawLocator,
                sourceRowHash,
                exposedSemanticHash,
                sourceFileSha256,
                semanticHashStatus,
                semanticHashVersion,
                legacySemanticHash
        );
    }

    private static String buildSourceRecordKey(
            String rawLocator,
            String rawHash,
            String semanticHash,
            String legacySemanticHash,
            String semanticHashStatus) {
        if (rawLocator == null || (rawHash == null && semanticHash == null && legacySemanticHash == null)) {
            return null;
        }
        StringBuilder sb = new StringBuilder(rawLocator);
        if (rawHash != null) {
            sb.append(":raw=").append(shortHash(rawHash));
        }
        if (semanticHash != null) {
            sb.append(":sem=").append(shortHash(semanticHash));
        } else if ("LEGACY_UNVERIFIED".equals(semanticHashStatus) && legacySemanticHash != null) {
            sb.append(":legacySem=").append(shortHash(legacySemanticHash));
        }
        return sb.toString();
    }

    private static String shortHash(String hash) {
        return hash.substring(0, Math.min(12, hash.length()));
    }

    private static String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static String trimToEmpty(String s) {
        return s == null ? "" : s.trim();
    }

    public List<Dto.SourceInfo> sources() {
        return jdbc.getJdbcTemplate().query("""
                SELECT product, sensor, source_key, bbox, period_start, period_end, rows_loaded,
                       min_date, max_date, loaded_at
                FROM ingestion_log ORDER BY loaded_at DESC LIMIT 500
                """, (rs, i) -> new Dto.SourceInfo(rs.getString("product"), rs.getString("sensor"),
                rs.getString("source_key"), rs.getString("bbox"),
                rs.getObject("period_start", LocalDate.class), rs.getObject("period_end", LocalDate.class),
                rs.getLong("rows_loaded"), rs.getObject("min_date", LocalDate.class),
                rs.getObject("max_date", LocalDate.class), rs.getTimestamp("loaded_at").toInstant()));
    }

    public List<Dto.Coverage> coverage() {
        return jdbc.getJdbcTemplate().query("""
                SELECT sensor, min(acq_date) AS min_date, max(acq_date) AS max_date, sum(n) AS n
                FROM daily_grid GROUP BY sensor ORDER BY sensor
                """, (rs, i) -> new Dto.Coverage(rs.getString("sensor"),
                rs.getObject("min_date", LocalDate.class), rs.getObject("max_date", LocalDate.class),
                rs.getLong("n")));
    }

    private static Float getFloat(ResultSet rs, String col) throws SQLException {
        float v = rs.getFloat(col);
        return rs.wasNull() ? null : v;
    }

    private static Double getDouble(ResultSet rs, String col) throws SQLException {
        double v = rs.getDouble(col);
        return rs.wasNull() ? null : v;
    }

    public static <T> List<T> trim(List<T> l, int max) {
        return l.size() > max ? new ArrayList<>(l.subList(0, max)) : l;
    }
}
