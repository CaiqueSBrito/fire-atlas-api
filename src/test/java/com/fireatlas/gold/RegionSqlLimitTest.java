package com.fireatlas.gold;

import com.fireatlas.gold.h3.policy.H3Index;
import com.fireatlas.gold.region.entity.Region;
import com.fireatlas.gold.region.repository.TrinoFilters;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RegionSqlLimitTest {
    @Test
    void expansao_preserva_regiao_sem_ultrapassar_sql_do_athena() {
        long parent = H3Index.parent(H3Index.fromHex("85283473fffffff"), 1);
        Region region = Region.of(null, H3Index.toHex(parent));
        var before = new Region.Cover(6, H3Index.toResolution(List.of(parent), 6, Region.MAX_MAP_CELLS));
        var start = LocalDate.of(2026, 11, 1);
        var end = start.plusDays(6);
        assertTrue(TrinoFilters.where(before, "h3_cell", start, end).length() > 262_144);

        var cover = region.coverFitting(6, Region.MAX_MAP_CELLS);
        assertEquals(5, cover.res());
        assertEquals(H3Index.toResolution(List.of(parent), 5, Region.MAX_MAP_CELLS), cover.cells());
        assertTrue(TrinoFilters.where(cover, "h3_cell", start, end).length() < 262_144);
    }

    @Test
    void cobertura_que_cabe_mantem_resolucao_e_limite_menor_do_chamador() {
        Region region = Region.of(null, "85283473fffffff");
        var cover = region.coverFitting(6, Region.MAX_MAP_CELLS);
        assertEquals(6, cover.res());
        assertEquals(7, cover.cells().size());
        var limited = region.coverFitting(6, 1);
        assertEquals(5, limited.res());
        assertEquals(List.of(H3Index.fromHex("85283473fffffff")), limited.cells());
    }
}
