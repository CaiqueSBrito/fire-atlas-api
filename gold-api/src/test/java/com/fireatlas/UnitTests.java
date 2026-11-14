package com.fireatlas;

import com.fireatlas.h3.policy.H3Settings;
import com.fireatlas.analysis.policy.Anomaly;
import com.fireatlas.analysis.policy.Historical;
import com.fireatlas.common.controller.QueryParams;
import com.fireatlas.h3.policy.H3Index;
import com.fireatlas.region.entity.BBox;
import com.fireatlas.region.entity.Region;
import com.fireatlas.sensor.entity.Satellite;
import com.fireatlas.sensor.entity.Sensor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class UnitTests {

    @Test
    void h3AncestorByBitsMatchesLibrary() {
        double[][] points = {{-10.5, -55.5}, {0, 0}, {-33.99, 22.44}, {67.78, 101.81}, {-89.9, 179.9},
                {89.9, -179.9}, {-3.1, -60.0}, {35.68, 139.69}};
        for (double[] p : points) {
            long base = TestH3.cell(p[0], p[1]);
            assertEquals(8, H3Index.resolution(base));
            for (int res = 0; res <= 8; res++) {
                assertEquals(H3Index.parent(base, res), H3Index.parentBits(base, res),
                        "ancestral por bits difere da biblioteca na resolucao " + res);
                assertEquals(res, H3Index.resolution(H3Index.parentBits(base, res)));
            }
        }
    }

    @Test
    void h3HexRoundTripAndValidation() {
        long c = TestH3.cell(-10.5, -55.5, 6);
        assertEquals(c, H3Index.fromHex(H3Index.toHex(c)));
        assertThrows(IllegalArgumentException.class, () -> H3Index.fromHex("nao-e-h3"));
        assertThrows(IllegalArgumentException.class, () -> H3Index.fromHex("ffffffffffffffff"));
        assertEquals(TestH3.cell(-10.5, -55.5, 4), H3Index.parent(TestH3.cell(-10.5, -55.5, 6), 4));
    }

    @Test
    void bboxValidation() {
        assertEquals(-74.0, BBox.parse("-74,-34,-34,6").minLon());
        assertThrows(IllegalArgumentException.class, () -> BBox.parse("1,2,3"));
        assertThrows(IllegalArgumentException.class, () -> BBox.parse("10,0,5,1"));
        assertThrows(IllegalArgumentException.class, () -> BBox.parse("0,0,200,1"));
        assertThrows(IllegalArgumentException.class, () -> BBox.parse("a,b,c,d"));
    }

    @Test
    void queryParamsValidation() {
        var d1 = LocalDate.of(2024, 1, 1);
        var q = QueryParams.of("-74,-34,-34,6", null,d1, d1.plusDays(9), "both", 31);
        assertEquals(2, q.sensors().size());
        assertThrows(IllegalArgumentException.class,
                () -> QueryParams.of("-74,-34,-34,6", null,d1, d1.minusDays(1), "BOTH", 31));
        assertThrows(IllegalArgumentException.class,
                () -> QueryParams.of("-74,-34,-34,6", null,d1, d1.plusDays(100), "BOTH", 31));
        assertThrows(IllegalArgumentException.class,
                () -> QueryParams.of("-74,-34,-34,6", null,d1, d1, "LANDSAT", 31));
    }

    @Test
    void robustZFlagsSpike() {
        double[] counts = {10, 12, 11, 9, 10, 11, 10, 12, 10, 200};
        Double[] z = Anomaly.robustZ(counts);
        assertTrue(z[9] > Anomaly.THRESHOLD);
        assertTrue(z[0] < Anomaly.THRESHOLD);
        assertNull(Anomaly.robustZ(new double[]{1, 2, 3})[0]);
        assertNull(Anomaly.robustZ(new double[]{5, 5, 5, 5, 5, 5, 5, 5})[0]);
    }

    @Test
    void sensorAndSatelliteInvariants() {
        var sensor = new Sensor("MODIS", "Moderate Resolution Imaging Spectroradiometer", 1000, "d");
        assertEquals(1000, sensor.nominalResolutionMeters());
        assertThrows(IllegalArgumentException.class, () -> new Sensor("X", "x", 0, "d"));
        assertThrows(IllegalArgumentException.class, () -> new Satellite("", "MODIS", "p", "q"));
    }

    @Test
    void h3SettingsDefineBaseAndZoomMapping() {
        var gold = new H3Settings(6);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6}, gold.aggResolutions());
        assertEquals(3, gold.resolutionForZoom(2));
        assertEquals(4, gold.resolutionForZoom(4));
        assertEquals(6, gold.resolutionForZoom(7));
        assertEquals(6, gold.resolutionForZoom(20)); // a gold vai ate a resolucao 6
        assertEquals(2, new H3Settings(2).resolutionForZoom(2)); // limitado pela resolucao-base
        assertEquals(6, H3Settings.defaults().baseResolution());
        assertThrows(IllegalArgumentException.class, () -> new H3Settings(16));
    }

    @Test
    void aggregatedQueriesRequireCellsAndNeverConvertBBox() {
        var byBBox = Region.of("-56,-11,-55,-10", null);
        var ex = assertThrows(IllegalArgumentException.class, () -> byBBox.coverFitting(6, 20000));
        assertTrue(ex.getMessage().contains("cells"), ex.getMessage());
        assertThrows(IllegalArgumentException.class, () -> byBBox.coverNative(6, 20000));

        // uma celula grossa pedida em resolucao fina desce de resolucao ate caber no limite
        var byCell = Region.of(null, H3Index.toHex(TestH3.cell(-10.5, -55.5, 3)));
        var cover = byCell.coverFitting(8, 5000);
        assertEquals(7, cover.res());
        assertEquals(2401, cover.cells().size());
    }

    @Test
    void nativeCoverUsesTheResolutionOfTheCellsWithoutExpandingThem() {
        var c3 = TestH3.cell(-10.5, -55.5, 3);
        var byCell = Region.of(null, H3Index.toHex(c3));
        var cover = byCell.coverNative(6, 5000);
        assertEquals(3, cover.res());
        assertEquals(List.of(c3), cover.cells());

        // celula mais fina que a base: sobe para a base (a gold so tem ate a resolucao 6)
        var fine = Region.of(null, H3Index.toHex(TestH3.cell(-10.5, -55.5, 9)));
        var fineCover = fine.coverNative(6, 5000);
        assertEquals(6, fineCover.res());
        assertEquals(List.of(TestH3.cell(-10.5, -55.5, 6)), fineCover.cells());

        // resolucoes misturadas: usa a mais fina e expande as mais grossas
        var mixed = Region.of(null, H3Index.toHex(c3) + "," + H3Index.toHex(TestH3.cell(-20, -50, 4)));
        assertEquals(4, mixed.coverNative(6, 5000).res());
    }

    @Test
    void regionValidation() {
        assertTrue(Region.of("-56,-11,-55,-10", null).isBBox());
        assertThrows(IllegalArgumentException.class, () -> Region.of(null, null));
        assertThrows(IllegalArgumentException.class, () -> Region.of("-56,-11,-55,-10", "8a2a1072b59ffff"));
        assertFalse(Region.of(null, H3Index.toHex(TestH3.cell(-10.5, -55.5, 4))).isBBox());
    }

    @Test
    void historicalPolicyNeedsEnoughYearsAndFlagsDeviations() {
        assertFalse(Historical.evaluate(List.of(1.0, 1.0), 2.0).sufficient());
        assertNull(Historical.evaluate(List.of(1.0, 1.0), 2.0).mean());
        var r = Historical.evaluate(List.of(1.0, 1.0, 1.25), 2.0);
        assertTrue(r.sufficient());
        assertEquals(1.0833, r.mean(), 0.001);
        assertTrue(r.zScore() > Historical.Z_THRESHOLD);
        assertTrue(r.anomalous());
        assertEquals(84.6, r.changeVsMeanPct(), 0.1);
        assertFalse(Historical.evaluate(List.of(1.0, 1.1, 0.9), 1.0).anomalous());
        // sem variacao entre os anos: nao ha z-score (nao se inventa)
        var flat = Historical.evaluate(List.of(2.0, 2.0, 2.0), 3.0);
        assertTrue(flat.sufficient());
        assertNull(flat.zScore());
        assertNull(flat.anomalous());
    }
}
