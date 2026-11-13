package com.fireatlas;

import com.fireatlas.api.QueryParams;
import com.fireatlas.domain.BBox;
import com.fireatlas.domain.GridMath;
import com.fireatlas.ingestion.DetectionRowParser;
import com.fireatlas.query.Anomaly;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class UnitTests {

    private static final String VIIRS_HEADER =
            "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_t31,frp,daynight";

    private static DetectionRowParser parser() {
        return new DetectionRowParser(VIIRS_HEADER, "VIIRS", "VIIRS_S-NPP", "bronze/raw/x.csv");
    }

    @Test
    void parsesRealVIIRSNrtLine() {
        var res = parser().parse("-33.99026,22.43891,305.28,0.49,0.4,2026-08-01,0001,SNPP,SNPP,n,2.0NRT,281.04,1.44,N");
        assertTrue(res.accepted());
        assertEquals(LocalDate.of(2026, 8, 1), res.row().date());
        assertEquals(
                "VIIRS,SNPP,2026-08-01,2026-08-01T00:01:00Z,-33.99026,22.43891,305.28,281.04,0.49,0.4,1.44,n,n,N,2.0NRT,VIIRS_S-NPP,bronze/raw/x.csv",
                res.row().csvLine());
    }

    @Test
    void rejectsInvalidRowsWithReason() {
        var p = parser();
        String ok = "-10.5,-55.5,305.28,0.49,0.4,2026-09-02,1805,SNPP,SNPP,n,2.0NRT,281.04,1.44,N";
        assertTrue(p.parse(ok).accepted());

        assertEquals(DetectionRowParser.LATITUDE_INVALIDA,
                p.parse(ok.replace("-10.5,", "95,")).rejectReason());
        assertEquals(DetectionRowParser.LATITUDE_INVALIDA,
                p.parse(ok.replace("-10.5,", "abc,")).rejectReason());
        assertEquals(DetectionRowParser.LONGITUDE_INVALIDA,
                p.parse(ok.replace(",-55.5,", ",-200,")).rejectReason());
        assertEquals(DetectionRowParser.DATA_INVALIDA,
                p.parse(ok.replace("2026-09-02", "2026-13-45")).rejectReason());
        assertEquals(DetectionRowParser.HORA_INVALIDA,
                p.parse(ok.replace(",1805,", ",2575,")).rejectReason());
        assertEquals(DetectionRowParser.SATELITE_AUSENTE,
                p.parse(ok.replace(",SNPP,SNPP,", ",,SNPP,")).rejectReason());
        assertEquals(DetectionRowParser.FRP_INVALIDO,
                p.parse(ok.replace(",1.44,N", ",-3,N")).rejectReason());
        assertEquals(DetectionRowParser.FRP_INVALIDO,
                p.parse(ok.replace(",1.44,N", ",xx,N")).rejectReason());
        assertEquals(DetectionRowParser.CAMPO_NUMERICO_INVALIDO,
                p.parse(ok.replace("305.28", "quente")).rejectReason());
        assertEquals(DetectionRowParser.LATITUDE_INVALIDA, p.parse("lixo").rejectReason());
    }

    @Test
    void missingRequiredColumnFails() {
        assertThrows(IllegalArgumentException.class,
                () -> new DetectionRowParser("latitude,longitude", "MODIS", "MODIS_C6.1", "f"));
    }

    @Test
    void confidenceIsHarmonized() {
        assertEquals("l", DetectionRowParser.confidenceClass("10"));
        assertEquals("n", DetectionRowParser.confidenceClass("30"));
        assertEquals("n", DetectionRowParser.confidenceClass("79"));
        assertEquals("h", DetectionRowParser.confidenceClass("80"));
        assertEquals("h", DetectionRowParser.confidenceClass("h"));
        assertEquals("l", DetectionRowParser.confidenceClass("low"));
        assertEquals("", DetectionRowParser.confidenceClass(""));
    }

    @Test
    void gridCellsAndBlocks() {
        assertEquals(0, GridMath.cx(-180));
        assertEquals(3599, GridMath.cx(180));
        assertEquals(1800 - 1, GridMath.cy(90));
        assertEquals(900, GridMath.cy(0));
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
        assertNull(Anomaly.robustZ(new double[]{1, 2, 3})[0]);
        assertNull(Anomaly.robustZ(new double[]{5, 5, 5, 5, 5, 5, 5, 5})[0]);
    }
}
