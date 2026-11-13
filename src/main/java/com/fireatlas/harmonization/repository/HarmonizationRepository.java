package com.fireatlas.harmonization.repository;

import com.fireatlas.h3.policy.H3Settings;
import com.fireatlas.harmonization.entity.HarmonizationResult;
import com.fireatlas.harmonization.entity.MatchedPair;
import com.fireatlas.region.entity.Region;
import com.fireatlas.region.repository.SqlFilters;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

/**
 * Pareamento MODIS -> VIIRS por proximidade espaco-temporal (metodo v0, ver docs).
 * A distancia usa PostGIS (geografia): mais precisa que celulas H3 para um limiar em metros.
 */
@Repository
public class HarmonizationRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final H3Settings h3;

    public HarmonizationRepository(NamedParameterJdbcTemplate jdbc, H3Settings h3) {
        this.jdbc = jdbc;
        this.h3 = h3;
    }

    public HarmonizationResult computeHarmonization(Region region, LocalDate s, LocalDate e,
                                                    int maxDistM, int maxMinutes, int modisCap) {
        double cosLat = Math.cos(Math.toRadians(Math.min(region.maxAbsLat(), 80)));
        double degTol = (maxDistM / (111_320.0 * cosLat)) * 1.2;
        MapSqlParameterSource p = SqlFilters.period(s, e, List.of("MODIS", "VIIRS"))
                .addValue("cap", modisCap).addValue("degTol", degTol)
                .addValue("sec", maxMinutes * 60.0).addValue("dist", (double) maxDistM);
        String sql = """
                WITH m AS (
                    SELECT geom, acq_date, acq_ts, frp
                    FROM detection
                    WHERE sensor = 'MODIS' AND frp IS NOT NULL
                      AND acq_date BETWEEN :start AND :end
                      AND {region}
                    ORDER BY acq_ts
                    LIMIT :cap
                ), x AS (
                    SELECT m.frp AS mfrp, l.n, l.vfrp
                    FROM m CROSS JOIN LATERAL (
                        SELECT count(*) AS n, coalesce(sum(v.frp::double precision), 0) AS vfrp
                        FROM detection v
                        WHERE v.sensor = 'VIIRS'
                          AND v.acq_date BETWEEN m.acq_date - 1 AND m.acq_date + 1
                          AND v.geom && ST_Expand(m.geom, :degTol)
                          AND abs(extract(epoch FROM (v.acq_ts - m.acq_ts))) <= :sec
                          AND ST_Distance(v.geom::geography, m.geom::geography) <= :dist
                    ) l
                )
                SELECT count(*) AS total,
                       count(*) FILTER (WHERE n > 0) AS matched,
                       avg(mfrp) FILTER (WHERE n > 0) AS m_mean,
                       avg(vfrp) FILTER (WHERE n > 0) AS v_mean,
                       sum(vfrp) FILTER (WHERE n > 0) / nullif(sum(mfrp) FILTER (WHERE n > 0), 0) AS ratio,
                       corr(mfrp, vfrp) FILTER (WHERE n > 0) AS r,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY n) FILTER (WHERE n > 0) AS med_n
                FROM x
                """.replace("{region}", SqlFilters.region(region, p, h3.baseResolution()));
        return jdbc.queryForObject(sql, p, (rs, i) -> {
            long total = rs.getLong("total");
            long matched = rs.getLong("matched");
            return new HarmonizationResult(total, matched, total == 0 ? null : matched / (double) total,
                    getDouble(rs, "m_mean"), getDouble(rs, "v_mean"), getDouble(rs, "ratio"),
                    getDouble(rs, "r"), getDouble(rs, "med_n"), total >= modisCap);
        });
    }

    /** Pares MODIS x VIIRS (VIIRS mais proximo de cada MODIS dentro dos limiares), do mais proximo ao mais distante. */
    public List<MatchedPair> findMatches(Region region, LocalDate s, LocalDate e, int maxDistM, int maxMinutes,
                                         int modisCap, int limit) {
        double cosLat = Math.cos(Math.toRadians(Math.min(region.maxAbsLat(), 80)));
        double degTol = (maxDistM / (111_320.0 * cosLat)) * 1.2;
        MapSqlParameterSource p = SqlFilters.period(s, e, List.of("MODIS", "VIIRS"))
                .addValue("cap", modisCap).addValue("degTol", degTol).addValue("limit", limit)
                .addValue("sec", maxMinutes * 60.0).addValue("dist", (double) maxDistM);
        String sql = """
                WITH m AS (
                    SELECT geom, lat, lon, acq_date, acq_ts, frp, source_file
                    FROM detection
                    WHERE sensor = 'MODIS' AND frp IS NOT NULL
                      AND acq_date BETWEEN :start AND :end
                      AND {region}
                    ORDER BY acq_ts
                    LIMIT :cap
                )
                SELECT m.lat AS mlat, m.lon AS mlon, m.acq_ts AS mts, m.frp AS mfrp, m.source_file AS msrc,
                       l.lat AS vlat, l.lon AS vlon, l.acq_ts AS vts, l.frp AS vfrp, l.satellite AS vsat,
                       l.source_file AS vsrc, l.d AS dist, l.sec / 60.0 AS minutes
                FROM m CROSS JOIN LATERAL (
                    SELECT v.lat, v.lon, v.acq_ts, v.frp, v.satellite, v.source_file,
                           ST_Distance(v.geom::geography, m.geom::geography) AS d,
                           abs(extract(epoch FROM (v.acq_ts - m.acq_ts))) AS sec
                    FROM detection v
                    WHERE v.sensor = 'VIIRS'
                      AND v.acq_date BETWEEN m.acq_date - 1 AND m.acq_date + 1
                      AND v.geom && ST_Expand(m.geom, :degTol)
                      AND abs(extract(epoch FROM (v.acq_ts - m.acq_ts))) <= :sec
                      AND ST_Distance(v.geom::geography, m.geom::geography) <= :dist
                    ORDER BY d
                    LIMIT 1
                ) l
                ORDER BY l.d, m.acq_ts
                LIMIT :limit
                """.replace("{region}", SqlFilters.region(region, p, h3.baseResolution()));
        return jdbc.query(sql, p, (rs, i) -> new MatchedPair(rs.getDouble("mlat"), rs.getDouble("mlon"),
                rs.getTimestamp("mts").toInstant(), getFloat(rs, "mfrp"), rs.getString("msrc"),
                rs.getDouble("vlat"), rs.getDouble("vlon"), rs.getTimestamp("vts").toInstant(),
                getFloat(rs, "vfrp"), rs.getString("vsat"), rs.getString("vsrc"),
                rs.getDouble("dist"), rs.getDouble("minutes")));
    }

    private static Float getFloat(ResultSet rs, String col) throws SQLException {
        float v = rs.getFloat(col);
        return rs.wasNull() ? null : v;
    }

    private static Double getDouble(ResultSet rs, String col) throws SQLException {
        double v = rs.getDouble(col);
        return rs.wasNull() ? null : v;
    }
}
