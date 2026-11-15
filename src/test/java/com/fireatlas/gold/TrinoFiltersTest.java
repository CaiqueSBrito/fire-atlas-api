package com.fireatlas.gold;

import com.fireatlas.gold.region.entity.Region;
import com.fireatlas.gold.region.repository.TrinoFilters;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * As duas propriedades do SQL do Athena que custam dinheiro quando quebram: o filtro de particao (sem ele o Athena
 * varre a tabela inteira) e o texto canonico (o reuso de resultado tem como chave o texto exato da consulta).
 */
class TrinoFiltersTest {

    private static final LocalDate START = LocalDate.of(2025, 11, 10);
    private static final LocalDate END = LocalDate.of(2026, 1, 5);

    @Test
    void mesma_regiao_em_ordem_diferente_gera_o_mesmo_texto() {
        var a = new Region.Cover(5, List.of(600850295324409855L, 601238046045634559L, 600850292103184383L));
        var b = new Region.Cover(5, List.of(601238046045634559L, 600850292103184383L, 600850295324409855L));
        assertEquals(TrinoFilters.where(a, "h3_cell", START, END), TrinoFilters.where(b, "h3_cell", START, END));
    }

    @Test
    void celula_repetida_nao_duplica_no_texto() {
        var cover = new Region.Cover(5, List.of(600850295324409855L, 600850295324409855L));
        assertEquals("600850295324409855", TrinoFilters.cells(cover.cells()));
    }

    @Test
    void toda_condicao_leva_resolucao_ano_e_periodo() {
        String where = TrinoFilters.where(new Region.Cover(6, List.of(1L)), "h3_cell", START, END);
        assertTrue(where.contains("resolution = 6"), where);
        assertTrue(where.contains("year IN (2025, 2026)"), where);
        assertTrue(where.contains("day BETWEEN DATE '2025-11-10' AND DATE '2026-01-05'"), where);
    }

    @Test
    void lugar_vira_juncao_com_h3_places_na_mesma_resolucao() {
        var place = new Region.Place(Region.Kind.COUNTRY, "BR", 0);
        String where = TrinoFilters.where(new Region.Cover(4, List.of(), place), "h3_cell", START, END);
        assertTrue(where.contains("SELECT pl.h3_cell FROM h3_places pl WHERE pl.resolution = 4"), where);
        assertTrue(where.contains("pl.country_iso_a2 = 'BR'"), where);
    }

    @Test
    void texto_com_aspas_e_escapado() {
        assertEquals("'O''Higgins'", TrinoFilters.text("O'Higgins"));
    }
}
