package com.fireatlas.gold.activity.repository;

import com.fireatlas.gold.activity.entity.CellActivity;
import com.fireatlas.gold.activity.entity.DayActivity;
import com.fireatlas.gold.activity.entity.MonthActivity;
import com.fireatlas.gold.activity.entity.SatelliteDay;
import com.fireatlas.gold.activity.entity.YearMonthSum;
import com.fireatlas.gold.activity.policy.TemperatureBins;
import com.fireatlas.gold.region.entity.Region;
import com.fireatlas.gold.region.repository.TrinoFilters;
import com.fireatlas.lake.integration.AthenaQuery;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Consultas da atividade de fogo harmonizada direto no data lake pelo Athena ({@code harmonized_cells} e
 * {@code harmonized_availability}, tabelas Delta registradas no Glue): nada e materializado no PostGIS.
 * Os niveis 0 a 5 ja trazem a soma das celulas filhas: cada consulta usa UMA resolucao, a da cobertura, e nunca
 * soma celulas r6.
 */
@Repository("goldActivityRepository")
public class ActivityRepository {

    static final String CELLS = "harmonized_cells";
    static final String AVAILABILITY = "harmonized_availability";

    private final AthenaQuery athena;

    public ActivityRepository(AthenaQuery athena) {
        this.athena = athena;
    }

    /**
     * Soma por dia local; contributing e o maior numero de satelites contribuintes entre as celulas. Cada faixa de
     * brilho e somada (nula quando nenhuma linha a trouxe) e basis e a base de temperatura do dia.
     */
    public List<DayActivity> findDaily(Region.Cover cover, LocalDate start, LocalDate end) {
        if (empty(cover)) return List.of();
        String sql = "SELECT day, sum(harmonized) AS harmonized, max(contributing) AS contributing, count(*) AS n,"
                + " max(basis) AS basis" + binSums()
                + " FROM " + CELLS
                + " WHERE " + TrinoFilters.where(cover, "h3_cell", start, end)
                + " GROUP BY day ORDER BY day";
        return athena.run(sql).stream().map(r -> {
            List<Double> bins = new ArrayList<>();
            for (TemperatureBins.Bin b : TemperatureBins.ALL) bins.add(r.dbl(b.column()));
            return new DayActivity(r.date("day"), r.dblOr("harmonized", 0), r.intOr("contributing", 0),
                    r.lngOr("n", 0), r.str("basis"), bins);
        }).toList();
    }

    /**
     * Soma do periodo por celula, na resolucao da cobertura, da maior para a menor atividade; so celulas com algum
     * satelite contribuindo. Pede limit+1 para o chamador poder sinalizar truncamento.
     */
    public List<CellActivity> findCells(Region.Cover cover, LocalDate start, LocalDate end, int limit) {
        if (empty(cover)) return List.of();
        String sql = "SELECT h3_cell, sum(harmonized) AS harmonized, max(contributing) AS contributing,"
                + " count(DISTINCT day) FILTER (WHERE contributing > 0) AS days_with_data,"
                + " sum(coalesce(detections_terra, 0) + coalesce(detections_aqua, 0)) AS modis,"
                + " sum(coalesce(detections_snpp, 0) + coalesce(detections_noaa20, 0)"
                + " + coalesce(detections_noaa21, 0)) AS viirs"
                + " FROM " + CELLS
                + " WHERE " + TrinoFilters.where(cover, "h3_cell", start, end)
                + " GROUP BY h3_cell HAVING max(contributing) > 0"
                + " ORDER BY 2 DESC, 1 LIMIT " + (limit + 1);
        return athena.run(sql).stream().map(r -> new CellActivity(r.lngOr("h3_cell", 0), r.dblOr("harmonized", 0),
                r.intOr("contributing", 0), r.intOr("days_with_data", 0), r.lngOr("modis", 0), r.lngOr("viirs", 0)))
                .toList();
    }

    private static String binSums() {
        StringBuilder sb = new StringBuilder();
        for (TemperatureBins.Bin b : TemperatureBins.ALL) {
            sb.append(", sum(").append(b.lakeColumn()).append(") AS ").append(b.column());
        }
        return sb.toString();
    }

    /** Soma por mes; dias com dado = dias em que ao menos uma celula teve satelite contribuindo. */
    public List<MonthActivity> findMonthly(Region.Cover cover, LocalDate start, LocalDate end) {
        if (empty(cover)) return List.of();
        String sql = "SELECT date_trunc('month', day) AS month, sum(harmonized) AS harmonized,"
                + " count(DISTINCT day) FILTER (WHERE contributing > 0) AS days_with_data"
                + " FROM " + CELLS
                + " WHERE " + TrinoFilters.where(cover, "h3_cell", start, end)
                + " GROUP BY 1 ORDER BY 1";
        return athena.run(sql).stream().map(r -> new MonthActivity(r.date("month"), r.dblOr("harmonized", 0),
                r.intOr("days_with_data", 0))).toList();
    }

    /** Soma do mesmo mes do calendario em cada ano do intervalo (so anos com linhas), do mais recente ao mais antigo. */
    public List<YearMonthSum> findSameMonthByYear(Region.Cover cover, int month, int fromYear, int toYear) {
        if (empty(cover)) return List.of();
        LocalDate start = LocalDate.of(fromYear, 1, 1);
        LocalDate end = LocalDate.of(toYear, 12, 31);
        String sql = "SELECT year, sum(harmonized) AS harmonized,"
                + " count(DISTINCT day) FILTER (WHERE contributing > 0) AS days_with_data"
                + " FROM " + CELLS
                + " WHERE " + TrinoFilters.where(cover, "h3_cell", start, end)
                + " AND month(day) = " + month
                + " GROUP BY year ORDER BY year DESC";
        return athena.run(sql).stream().map(r -> new YearMonthSum(r.intOr("year", 0), r.dblOr("harmonized", 0),
                r.intOr("days_with_data", 0))).toList();
    }

    /** Quantas celulas a regiao tem na resolucao da cobertura (base da estimativa de area). */
    public long countRegionCells(Region.Cover cover) {
        if (cover.place() == null) return cover.cells().size();
        String sql = "SELECT count(*) AS n FROM h3_places pl WHERE pl.resolution = " + cover.res()
                + " AND " + TrinoFilters.placeCondition(cover.place());
        return athena.run(sql).stream().findFirst().map(r -> r.lngOr("n", 0)).orElse(0L);
    }

    public List<SatelliteDay> findAvailability(LocalDate start, LocalDate end, String platform) {
        String sql = "SELECT day, platform, units, typical_units, available, contributes"
                + " FROM " + AVAILABILITY
                + " WHERE " + TrinoFilters.years(start, end) + " AND " + TrinoFilters.period(start, end)
                + (platform == null ? "" : " AND platform = " + TrinoFilters.text(platform))
                + " ORDER BY day, platform";
        return athena.run(sql).stream().map(r -> new SatelliteDay(r.date("day"), r.str("platform"), r.integer("units"),
                r.dbl("typical_units"), r.bool("available"), r.bool("contributes"))).toList();
    }

    /** Lista de celulas vazia nao vai ao data lake: nao existe linha possivel. */
    private static boolean empty(Region.Cover cover) {
        return cover.place() == null && cover.cells().isEmpty();
    }
}
