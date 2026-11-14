package com.fireatlas;

import com.fireatlas.activity.policy.ActivityLabels;
import com.fireatlas.activity.policy.AreaEstimate;
import com.fireatlas.activity.policy.BaselineResult;
import com.fireatlas.activity.policy.MonthBaseline;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regras da atividade harmonizada que nao dependem de banco: mediana, anomalia, area e avisos. */
class ActivityPolicyTest {

    @Test
    void medianOfOddAndEvenCounts() {
        assertEquals(60.0, MonthBaseline.median(List.of(90.0, 30.0, 60.0)));
        assertEquals(45.0, MonthBaseline.median(List.of(30.0, 60.0, 90.0, 10.0)));
    }

    @Test
    void anomalyNeedsAtLeastThreePreviousYears() {
        BaselineResult few = MonthBaseline.evaluate(List.of(30.0, 60.0), 54.0);
        assertFalse(few.sufficient());
        assertNull(few.median());
        assertNull(few.deviationPct());

        BaselineResult ok = MonthBaseline.evaluate(List.of(30.0, 60.0, 90.0), 54.0);
        assertTrue(ok.sufficient());
        assertEquals(60.0, ok.median());
        assertEquals(0.9, ok.ratioToMedian(), 1e-9);
        assertEquals(-10.0, ok.deviationPct(), 1e-9);
    }

    @Test
    void zeroMedianDoesNotInventARatio() {
        BaselineResult zero = MonthBaseline.evaluate(List.of(0.0, 0.0, 0.0), 5.0);
        assertTrue(zero.sufficient());
        assertEquals(0.0, zero.median());
        assertNull(zero.ratioToMedian());
        assertNull(zero.deviationPct());
    }

    @Test
    void areaComesFromTheAverageCellAreaOfEachResolution() {
        assertEquals(1770.3, AreaEstimate.avgCellKm2(4), 0.1);
        assertEquals(36.1, AreaEstimate.avgCellKm2(6), 0.1);
        assertEquals(3540.6, AreaEstimate.estimateKm2(4, 2), 0.1);
        assertEquals(36.1, AreaEstimate.avgCellKm2(99), 0.1); // fora da faixa: usa a mais fina
    }

    @Test
    void warningsAlwaysCarryTheUnitAndDependOnPeriodAndArea() {
        LocalDate today = LocalDate.of(2026, 11, 14);
        List<String> old = ActivityLabels.warnings(LocalDate.of(2020, 1, 1), LocalDate.of(2020, 1, 31), today,
                50_000);
        assertTrue(old.stream().anyMatch(w -> w.contains("nao e numero de focos")));
        assertTrue(old.stream().anyMatch(w -> w.contains("contributing = 0")));
        assertFalse(old.stream().anyMatch(w -> w.contains("corrente")));
        assertFalse(old.stream().anyMatch(w -> w.contains("NRT")));
        assertFalse(old.stream().anyMatch(w -> w.contains("Terra")));
        assertFalse(old.stream().anyMatch(w -> w.contains("Area pequena")));

        List<String> recent = ActivityLabels.warnings(LocalDate.of(2026, 11, 1), today, today, 1_800);
        assertTrue(recent.stream().anyMatch(w -> w.contains("corrente")));
        assertTrue(recent.stream().anyMatch(w -> w.contains("NRT")));
        assertTrue(recent.stream().anyMatch(w -> w.contains("Area pequena")));

        List<String> terra = ActivityLabels.warnings(LocalDate.of(2001, 1, 1), LocalDate.of(2001, 12, 31), today,
                50_000);
        assertTrue(terra.stream().anyMatch(w -> w.contains("Terra")));
    }

    @Test
    void labelsMatchTheOnesDefinedByTheDataTeam() {
        assertEquals("Harmonized fire activity (MODIS Aqua-equivalent active cells)", ActivityLabels.UNIT_EN);
        assertTrue(ActivityLabels.UNIT_PT.startsWith("Atividade de fogo harmonizada"));
        assertTrue(ActivityLabels.UNIT_PT.endsWith("equivalentes ao MODIS Aqua)"));
    }
}
