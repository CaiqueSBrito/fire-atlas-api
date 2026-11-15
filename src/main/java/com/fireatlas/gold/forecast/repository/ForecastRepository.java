package com.fireatlas.gold.forecast.repository;

import com.fireatlas.gold.forecast.entity.CellForecast;
import com.fireatlas.gold.forecast.entity.ForecastIssue;
import com.fireatlas.gold.forecast.entity.ForecastSummary;
import com.fireatlas.gold.region.entity.Region;
import com.fireatlas.gold.region.repository.TrinoFilters;
import com.fireatlas.lake.integration.AthenaQuery;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Consultas a previsao direto no data lake pelo Athena ({@code forecast}, tabela Delta registrada no Glue, mais
 * {@code h3_places} para pais e cidade): nada e materializado no PostGIS. So devolve entidades.
 * <p>
 * A tabela e particionada por {@code year} e {@code month} (derivados da emissao, unica data da tabela), entao toda
 * consulta leva a particao explicita: com a emissao em maos ela sai da propria data; na listagem de emissoes as
 * particoes existentes sao descobertas antes, num agrupamento que le so colunas de particao (metadado do Delta, sem
 * abrir arquivo de dados). A emissao mais recente e a primeira linha dessa listagem, nunca um {@code max} sobre a
 * tabela inteira.
 */
@Repository("goldForecastRepository")
public class ForecastRepository {

    static final String FORECAST = "forecast";
    static final String PLACES = "h3_places";

    /** A previsao vem por celula H3 de resolucao 6 (forecast.h3_r6). */
    private static final int CELL_RES = 6;

    private final AthenaQuery athena;

    public ForecastRepository(AthenaQuery athena) {
        this.athena = athena;
    }

    /** Datas de emissao disponiveis, a mais recente primeiro. */
    public List<ForecastIssue> findIssues() {
        String partitions = partitionsFilter();
        if (partitions == null) return List.of();
        String sql = "SELECT issue_date, count(*) AS cells, count(*) FILTER (WHERE active) AS active_cells"
                + " FROM " + FORECAST
                + " WHERE " + partitions
                + " GROUP BY issue_date ORDER BY issue_date DESC";
        return athena.run(sql).stream().map(r -> new ForecastIssue(r.date("issue_date"), r.lngOr("cells", 0),
                r.lngOr("active_cells", 0))).toList();
    }

    /**
     * Celulas da regiao (celulas H3 de qualquer resolucao ou lugar) na emissao, da maior para a menor probabilidade,
     * com o pais e a cidade mais proxima da celula quando h3_places os tem. Pede limit+1 para o chamador poder
     * sinalizar truncamento.
     */
    public List<CellForecast> findForecast(Region.Cover cover, LocalDate issueDate, double minProbability,
                                           boolean onlyActive, int limit) {
        if (empty(cover)) return List.of();
        String sql = "SELECT f.h3_r6, f.h3_r1, f.prob_fire_7d, f.det_24h, f.det_3d, f.det_7d, f.frp_mean_7d,"
                + " f.modis_brightness_mean_7d, f.viirs_brightness_mean_7d, f.ring1_det_24h, f.ring1_det_7d,"
                + " f.ring2_det_7d, f.active, pl.country_name_pt, pl.city_name, pl.city_distance_km"
                + " FROM (SELECT h3_r6, h3_r1, prob_fire_7d, det_24h, det_3d, det_7d, frp_mean_7d,"
                + " modis_brightness_mean_7d, viirs_brightness_mean_7d, ring1_det_24h, ring1_det_7d, ring2_det_7d,"
                + " active FROM " + FORECAST
                + " WHERE " + partition(issueDate) + " AND issue_date = " + TrinoFilters.date(issueDate)
                + " AND " + region(cover)
                + " AND prob_fire_7d >= " + probability(minProbability)
                + (onlyActive ? " AND active" : "")
                + " ORDER BY prob_fire_7d DESC, h3_r6 LIMIT " + (limit + 1) + ") f"
                + " LEFT JOIN (SELECT h3_cell, country_name_pt, city_name, city_distance_km FROM " + PLACES
                + " WHERE resolution = " + CELL_RES + ") pl ON pl.h3_cell = f.h3_r6"
                + " ORDER BY f.prob_fire_7d DESC, f.h3_r6";
        return athena.run(sql).stream().map(r -> new CellForecast(r.lngOr("h3_r6", 0), r.lng("h3_r1"),
                r.dbl("prob_fire_7d"), r.dbl("det_24h"), r.dbl("det_3d"), r.dbl("det_7d"), r.dbl("frp_mean_7d"),
                r.dbl("modis_brightness_mean_7d"), r.dbl("viirs_brightness_mean_7d"), r.dbl("ring1_det_24h"),
                r.dbl("ring1_det_7d"), r.dbl("ring2_det_7d"), r.bool("active"), r.str("country_name_pt"),
                r.str("city_name"), r.dbl("city_distance_km"))).toList();
    }

    /** Totais da regiao na emissao, sem os filtros da lista: celulas, ativas, maior e media da probabilidade. */
    public ForecastSummary findSummary(Region.Cover cover, LocalDate issueDate) {
        if (empty(cover)) return new ForecastSummary(0, 0, null, null);
        String sql = "SELECT count(*) AS cells, count(*) FILTER (WHERE active) AS active_cells,"
                + " max(prob_fire_7d) AS max_prob, avg(prob_fire_7d) AS mean_prob"
                + " FROM " + FORECAST
                + " WHERE " + partition(issueDate) + " AND issue_date = " + TrinoFilters.date(issueDate)
                + " AND " + region(cover);
        return athena.run(sql).stream().findFirst()
                .map(r -> new ForecastSummary(r.lngOr("cells", 0), r.lngOr("active_cells", 0), r.dbl("max_prob"),
                        r.dbl("mean_prob")))
                .orElse(new ForecastSummary(0, 0, null, null));
    }

    /**
     * Particoes existentes como condicao explicita ({@code year IN (...) AND month IN (...)}), ou nulo quando a
     * tabela nao tem nenhuma emissao. O agrupamento que as descobre projeta so colunas de particao, entao o Athena
     * responde pelo log do Delta sem ler arquivo de dados.
     */
    private String partitionsFilter() {
        String sql = "SELECT year, month FROM " + FORECAST + " GROUP BY year, month ORDER BY year, month";
        List<AthenaQuery.Row> rows = athena.run(sql);
        if (rows.isEmpty()) return null;
        String years = numbers(rows.stream().map(r -> r.intOr("year", 0)).toList());
        String months = numbers(rows.stream().map(r -> r.intOr("month", 0)).toList());
        return "year IN (" + years + ") AND month IN (" + months + ")";
    }

    /** Particao da emissao: year e month saem da propria data, a unica data da tabela. */
    private static String partition(LocalDate issueDate) {
        return "year = " + issueDate.getYear() + " AND month = " + issueDate.getMonthValue();
    }

    /**
     * Celulas da regiao sobre a coluna de resolucao fixa {@code h3_r6}. Cobertura mais grossa nao vira lista de
     * celulas filhas: compara o ancestral da celula na resolucao da cobertura, pelos mesmos bits de
     * {@code H3Index.parentSql} (que o PostGIS usava), aqui com as funcoes de bits do Trino — assim o resultado e
     * identico em qualquer resolucao de 0 a 6, inclusive para as coberturas por lugar.
     */
    private static String region(Region.Cover cover) {
        String cell = cover.res() < CELL_RES ? parentSql("h3_r6", cover.res()) : "h3_r6";
        Region.Place place = cover.place();
        if (place == null) return cell + " IN (" + TrinoFilters.cells(cover.cells()) + ")";
        return cell + " IN (SELECT pl.h3_cell FROM " + PLACES + " pl WHERE pl.resolution = " + cover.res()
                + " AND " + TrinoFilters.placeCondition(place) + ")";
    }

    /**
     * Ancestral na resolucao res de uma coluna com indice H3: digitos abaixo de res viram 7 (todos os bits em 1) e a
     * resolucao e regravada nos bits 52-55. Mesma formula de {@code H3Index.parentBits}, com as mascaras ja
     * calculadas em Java para o texto ficar canonico.
     */
    private static String parentSql(String column, int res) {
        long ones = (1L << (3 * (15 - res))) - 1;
        return "bitwise_or(bitwise_and(bitwise_or(" + column + ", " + ones + "), " + ~(15L << 52) + "), "
                + ((long) res << 52) + ")";
    }

    /** Literal de probabilidade estavel (sempre o mesmo texto para o mesmo valor) e tipado como o da coluna. */
    private static String probability(double value) {
        return "DOUBLE '" + value + "'";
    }

    private static String numbers(List<Integer> values) {
        return values.stream().distinct().sorted().map(String::valueOf).collect(Collectors.joining(", "));
    }

    /** Lista de celulas vazia nao vai ao data lake: nao existe linha possivel. */
    private static boolean empty(Region.Cover cover) {
        return cover.place() == null && cover.cells().isEmpty();
    }
}
