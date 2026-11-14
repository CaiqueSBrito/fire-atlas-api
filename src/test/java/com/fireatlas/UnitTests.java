package com.fireatlas;

import com.fireatlas.region.entity.BBox;
import com.fireatlas.detection.policy.GridMath;
import com.fireatlas.common.controller.QueryParams;
import com.fireatlas.analysis.policy.Anomaly;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class UnitTests {

    @Test
    void gridCellsAndBlocks() {
        assertEquals(0, GridMath.cx(-180));
        assertEquals(3599, GridMath.cx(180));
        assertEquals(1800 - 1, GridMath.cy(90));
        assertEquals(900, GridMath.cy(0));
        // celula 0,1: centro em lon 0.05 / lat 0.05 para cx=1800, cy=900
        assertEquals(0.05, GridMath.centerLon(1800, 1), 1e-9);
        assertEquals(0.05, GridMath.centerLat(900, 1), 1e-9);
        assertEquals(20, GridMath.blockFactor(2));
        assertEquals(1, GridMath.blockFactor(8));
        assertEquals(0.2, GridMath.cellSizeDeg(2), 1e-9);
    }

    @Test
    void bboxValidation() {
        assertEquals(-74.0, BBox.parse("-74,-34,-34,6").minLon());
        assertThrows(IllegalArgumentException.class, () -> BBox.parse("1,2,3"));
        assertThrows(IllegalArgumentException.class, () -> BBox.parse("10,0,5,1"));
        assertThrows(IllegalArgumentException.class, () -> BBox.parse("0,0,200,1"));
        assertThrows(IllegalArgumentException.class, () -> BBox.parse("a,b,c,d"));
        assertThrows(IllegalArgumentException.class, () -> BBox.parse("NaN,0,1,1"));
        assertThrows(IllegalArgumentException.class, () -> BBox.parse("0,Infinity,1,1"));
        assertThrows(IllegalArgumentException.class, () -> BBox.parse("-Infinity,0,1,1"));
    }

    @Test
    void queryParamsValidation() {
        var d1 = LocalDate.of(2024, 1, 1);
        var q = QueryParams.of("-74,-34,-34,6", d1, d1.plusDays(9), "both", 31);
        assertEquals(2, q.sensors().size());
        assertThrows(IllegalArgumentException.class,
                () -> QueryParams.of("-74,-34,-34,6", d1, d1.minusDays(1), "BOTH", 31));
        assertThrows(IllegalArgumentException.class,
                () -> QueryParams.of("-74,-34,-34,6", d1, d1.plusDays(100), "BOTH", 31));
        assertThrows(IllegalArgumentException.class,
                () -> QueryParams.of("-74,-34,-34,6", d1, d1, "LANDSAT", 31));
    }

    @Test
    void robustZFlagsSpike() {
        double[] counts = {10, 12, 11, 9, 10, 11, 10, 12, 10, 200};
        Double[] z = Anomaly.robustZ(counts);
        assertTrue(z[9] > Anomaly.THRESHOLD);
        assertTrue(z[0] < Anomaly.THRESHOLD);
        // poucos dados ou sem dispersao: nao sinaliza
        assertNull(Anomaly.robustZ(new double[]{1, 2, 3})[0]);
        assertNull(Anomaly.robustZ(new double[]{5, 5, 5, 5, 5, 5, 5, 5})[0]);
    }
}
