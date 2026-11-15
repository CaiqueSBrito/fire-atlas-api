package com.fireatlas.gold.detection.repository;

import com.fireatlas.gold.detection.entity.CellAggregate;
import com.fireatlas.gold.detection.entity.CellDistribution;
import com.fireatlas.gold.detection.entity.Coverage;
import com.fireatlas.gold.detection.entity.DayTotal;
import com.fireatlas.gold.detection.entity.PeriodTotal;
import com.fireatlas.gold.detection.entity.RankedCell;
import com.fireatlas.gold.detection.entity.SensorStats;
import com.fireatlas.gold.region.entity.Region;
import com.fireatlas.gold.region.repository.TrinoFilters;
import com.fireatlas.gold.sensor.policy.Sensors;
import com.fireatlas.lake.integration.AthenaQuery;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Consultas da grade H3 diaria direto no data lake pelo Athena ({@code map_grid}, tabela Delta registrada no
 * Glue): nada e materializado no PostGIS. Toda consulta usa UMA resolucao, a da cobertura, e leva o filtro de
 * particao ({@code resolution} e {@code year}) para o Athena nao varrer a tabela inteira.
 */
@Repository("goldDetectionRepository")
public class DetectionRepository {

    static final String GRID = "map_grid";

    // a resolucao nominal do sensor vem de Sensors (policy); no data lake so existe a grade.

    /** Cobertura olha so a resolucao mais grossa carregada; e o filtro de particao da consulta sem periodo. */
    private static final int COVERAGE_RESOLUTION = 0;

    private final AthenaQuery athena;

    public DetectionRepository(AthenaQuery athena) {
        this.athena = athena;
    }

    /** Celulas H3 agregadas por sensor (pede limit+1 para o chamador poder sinalizar truncamento). */
    public List<CellAggregate> findCells(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors,
                                         int limit) {
        if (empty(cover, sensors)) return List.of();
        String sql = "SELECT sensor, h3_cell, sum(detections) AS n, sum(frp_sum_mw) AS frp_sum,"
                + " max(frp_max_mw) AS frp_max"
                + " FROM " + GRID
                + " WHERE " + TrinoFilters.where(cover, "h3_cell", s, e)
                + " AND " + sensorIn(sensors)
                + " GROUP BY sensor, h3_cell"
                + " LIMIT " + (limit + 1);
        return athena.run(sql).stream().map(r -> new CellAggregate(r.str("sensor"), r.lngOr("h3_cell", 0),
                r.intOr("n", 0), r.dblOr("frp_sum", 0), flt(r, "frp_max"))).toList();
    }

    public List<DayTotal> findDailyTotals(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors) {
        if (empty(cover, sensors)) return List.of();
        String sql = "SELECT day, sensor, sum(detections) AS n, sum(frp_sum_mw) AS frp_sum"
                + " FROM " + GRID
                + " WHERE " + TrinoFilters.where(cover, "h3_cell", s, e)
                + " AND " + sensorIn(sensors)
                + " GROUP BY day, sensor ORDER BY day, sensor";
        return athena.run(sql).stream().map(r -> new DayTotal(r.date("day"), r.str("sensor"),
                r.intOr("n", 0), r.dblOr("frp_sum", 0))).toList();
    }

    /** Totais por sensor no periodo, somando as celulas H3 da regiao (base da contagem e da comparacao). */
    public List<PeriodTotal> findPeriodTotals(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors) {
        if (empty(cover, sensors)) return List.of();
        String sql = "SELECT sensor, sum(detections) AS n, coalesce(sum(frp_sum_mw), 0) AS frp_sum,"
                + " count(DISTINCT day) AS days"
                + " FROM " + GRID
                + " WHERE " + TrinoFilters.where(cover, "h3_cell", s, e)
                + " AND " + sensorIn(sensors)
                + " GROUP BY sensor ORDER BY sensor";
        return athena.run(sql).stream().map(r -> new PeriodTotal(r.str("sensor"), r.lngOr("n", 0),
                r.dblOr("frp_sum", 0), r.intOr("days", 0))).toList();
    }

    /** Celulas H3 mais ativas (todos os sensores selecionados somados), ordenadas por contagem ou por FRP. */
    public List<RankedCell> findRanking(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors,
                                        boolean byFrp, int limit) {
        if (empty(cover, sensors)) return List.of();
        String order = byFrp ? "sum(frp_sum_mw)" : "sum(detections)";
        String sql = "SELECT h3_cell, sum(detections) AS n, coalesce(sum(frp_sum_mw), 0) AS frp_sum,"
                + " max(frp_max_mw) AS frp_max,"
                + " coalesce(sum(detections) FILTER (WHERE sensor = 'MODIS'), 0) AS modis,"
                + " coalesce(sum(detections) FILTER (WHERE sensor = 'VIIRS'), 0) AS viirs"
                + " FROM " + GRID
                + " WHERE " + TrinoFilters.where(cover, "h3_cell", s, e)
                + " AND " + sensorIn(sensors)
                + " GROUP BY h3_cell"
                + " ORDER BY " + order + " DESC, h3_cell"
                + " LIMIT " + limit;
        return athena.run(sql).stream().map(r -> new RankedCell(r.lngOr("h3_cell", 0), r.lngOr("n", 0),
                r.dblOr("frp_sum", 0), flt(r, "frp_max"), r.lngOr("modis", 0), r.lngOr("viirs", 0))).toList();
    }

    /** Quantas celulas tem registro e o valor da contagem no percentil pedido (0-1): define as regioes criticas. */
    public CellDistribution findCellDistribution(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors,
                                                 double percentile) {
        if (empty(cover, sensors)) return new CellDistribution(0, null);
        String sql = "SELECT count(*) AS cells,"
                + " approx_percentile(CAST(n AS double), " + percentile + ") AS pct_value"
                + " FROM (SELECT h3_cell, sum(detections) AS n"
                + " FROM " + GRID
                + " WHERE " + TrinoFilters.where(cover, "h3_cell", s, e)
                + " AND " + sensorIn(sensors)
                + " GROUP BY h3_cell) t";
        return athena.run(sql).stream().findFirst()
                .map(r -> new CellDistribution(r.lngOr("cells", 0), r.dbl("pct_value")))
                .orElse(new CellDistribution(0, null));
    }

    /** Estatisticas por sensor somando as celulas H3 da regiao; a resolucao nominal vem do catalogo fixo. */
    public List<SensorStats> findSensorStats(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors) {
        if (empty(cover, sensors)) return List.of();
        String sql = "SELECT sensor, sum(detections) AS n, count(DISTINCT day) AS days,"
                + " coalesce(sum(frp_sum_mw), 0) AS frp_total,"
                + " sum(frp_sum_mw) / nullif(sum(detections) FILTER (WHERE frp_sum_mw IS NOT NULL), 0) AS frp_mean,"
                + " max(frp_max_mw) AS frp_max,"
                + " coalesce(sum(viirs_confidence_low), 0) AS conf_low,"
                + " coalesce(sum(viirs_confidence_nominal), 0) AS conf_nominal,"
                + " coalesce(sum(viirs_confidence_high), 0) AS conf_high,"
                + " sum(modis_confidence_mean * detections)"
                + " / nullif(sum(detections) FILTER (WHERE modis_confidence_mean IS NOT NULL), 0) AS modis_conf_mean"
                + " FROM " + GRID
                + " WHERE " + TrinoFilters.where(cover, "h3_cell", s, e)
                + " AND " + sensorIn(sensors)
                + " GROUP BY sensor ORDER BY sensor";
        return athena.run(sql).stream().map(r -> {
            // classes l/n/h existem so no VIIRS; no MODIS a fonte traz a media da confianca (0-100)
            String sensor = r.str("sensor");
            Map<String, Long> conf = new LinkedHashMap<>();
            if ("VIIRS".equals(sensor)) {
                conf.put("low", r.lngOr("conf_low", 0));
                conf.put("nominal", r.lngOr("conf_nominal", 0));
                conf.put("high", r.lngOr("conf_high", 0));
            }
            return new SensorStats(sensor, r.lngOr("n", 0), r.intOr("days", 0), r.dbl("frp_mean"),
                    flt(r, "frp_max"), r.dblOr("frp_total", 0), conf.isEmpty() ? null : conf,
                    r.dbl("modis_conf_mean"), Sensors.nominalResolutionM(sensor));
        }).toList();
    }

    /** Cobertura: periodo e total de deteccoes por sensor (uma so resolucao, a mais grossa carregada). */
    public List<Coverage> findCoverage() {
        String sql = "SELECT sensor, min(day) AS min_date, max(day) AS max_date, sum(detections) AS n"
                + " FROM " + GRID
                + " WHERE resolution = " + COVERAGE_RESOLUTION
                + " GROUP BY sensor ORDER BY sensor";
        return athena.run(sql).stream().map(r -> new Coverage(r.str("sensor"), r.date("min_date"),
                r.date("max_date"), r.lngOr("n", 0))).toList();
    }

    /** Filtro de sensor canonico: sensor e coluna de texto na grade, sem repeticao e em ordem. */
    private static String sensorIn(List<String> sensors) {
        return "sensor IN (" + sensors.stream().distinct().sorted().map(TrinoFilters::text)
                .collect(Collectors.joining(", ")) + ")";
    }

    private static Float flt(AthenaQuery.Row row, String column) {
        Double value = row.dbl(column);
        return value == null ? null : value.floatValue();
    }

    /** Regiao sem celulas ou busca sem sensor nao vai ao data lake: nao existe linha possivel. */
    private static boolean empty(Region.Cover cover, List<String> sensors) {
        return sensors.isEmpty() || (cover.place() == null && cover.cells().isEmpty());
    }
}
