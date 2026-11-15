package com.fireatlas.gold.region.repository;

import com.fireatlas.gold.region.entity.Region;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Filtros SQL no dialeto do Athena (Trino) para as tabelas Delta da gold, equivalentes aos de {@link SqlFilters}
 * no PostGIS. Duas diferencas que valem dinheiro:
 * <ul>
 *   <li>o filtro de <b>particao</b> ({@code resolution} e {@code year}) e obrigatorio: sem ele o Athena varre a
 *       tabela inteira, porque os arquivos nao sao ordenados por dia;</li>
 *   <li>o texto gerado e <b>canonico</b> (celulas ordenadas, datas como literais, sem espaco variavel): o reuso de
 *       resultado do Athena tem como chave o texto exato da consulta, entao a mesma pergunta precisa gerar
 *       exatamente o mesmo SQL para nao escanear byte nenhum.</li>
 * </ul>
 * Os valores entram como literais (o Athena nao tem bind com reuso de resultado); todos vem validados de
 * {@link Region} (celulas numericas, pais com 2-3 letras, continente de lista fechada, cidade numerica) e as
 * cadeias ainda sao escapadas.
 */
public final class TrinoFilters {

    private TrinoFilters() {}

    /** Condicao completa da regiao: particao de resolucao mais as celulas (lista ou lugar). */
    public static String region(Region.Cover cover, String cellColumn) {
        String res = "resolution = " + cover.res();
        Region.Place place = cover.place();
        if (place == null) return res + " AND " + cellColumn + " IN (" + cells(cover.cells()) + ")";
        return res + " AND " + cellColumn + " IN (SELECT pl.h3_cell FROM h3_places pl WHERE pl.resolution = "
                + cover.res() + " AND " + placeCondition(place) + ")";
    }

    /** Particao de ano derivada do periodo: sem isso nao ha poda nenhuma. */
    public static String years(LocalDate start, LocalDate end) {
        List<Integer> list = java.util.stream.IntStream.rangeClosed(start.getYear(), end.getYear()).boxed().toList();
        return "year IN (" + list.stream().map(String::valueOf).collect(Collectors.joining(", ")) + ")";
    }

    /** Periodo pelo dia local de fogo. */
    public static String period(LocalDate start, LocalDate end) {
        return "day BETWEEN " + date(start) + " AND " + date(end);
    }

    /** Regiao + particao de ano + periodo, na ordem fixa que mantem o texto canonico. */
    public static String where(Region.Cover cover, String cellColumn, LocalDate start, LocalDate end) {
        return region(cover, cellColumn) + " AND " + years(start, end) + " AND " + period(start, end);
    }

    public static String date(LocalDate day) {
        return "DATE '" + day + "'";
    }

    /** Celulas sem repeticao e em ordem: duas requisicoes da mesma area geram o mesmo texto. */
    public static String cells(List<Long> cells) {
        if (cells.isEmpty()) return "NULL";
        return cells.stream().distinct().sorted().map(String::valueOf).collect(Collectors.joining(", "));
    }

    /** Condicao sobre h3_places (alias pl) para o lugar, com as colunas como estao no data lake. */
    public static String placeCondition(Region.Place place) {
        return switch (place.kind()) {
            case COUNTRY -> "(pl.country_iso_a2 = " + text(place.value()) + " OR pl.country_code = "
                    + text(place.value()) + ")";
            case CONTINENT -> "pl.continent = " + text(place.value());
            case CITY -> "pl.city_geonameid = " + Long.parseLong(place.value()) + " AND pl.city_distance_km <= "
                    + place.cityRadiusKm();
        };
    }

    public static String text(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
