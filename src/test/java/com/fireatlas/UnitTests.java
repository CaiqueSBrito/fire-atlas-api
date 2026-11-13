package com.fireatlas;

import com.fireatlas.api.QueryParams;
import com.fireatlas.domain.BBox;
import com.fireatlas.domain.Detection;
import com.fireatlas.domain.H3Index;
import com.fireatlas.domain.Location;
import com.fireatlas.domain.Region;
import com.fireatlas.domain.Satellite;
import com.fireatlas.domain.Sensor;
import com.fireatlas.ingestion.DetectionRowParser;
import com.fireatlas.query.Anomaly;
import org.junit.jupiter.api.Test;

import java.time.Instant;
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
        assertEquals(LocalDate.of(2026, 8, 1), res.detection().acqDate());
        assertEquals("VIIRS", res.detection().sensor());
        assertEquals(H3Index.cell(-33.99026, 22.43891), res.detection().location().h3());
        assertEquals("VIIRS_S-NPP", res.detection().source().product());
        assertEquals(
                "VIIRS,SNPP,2026-08-01,2026-08-01T00:01:00Z,-33.99026,22.43891,305.28,281.04,0.49,0.4,1.44,n,n,N,2.0NRT,VIIRS_S-NPP,bronze/raw/x.csv,"
                        + H3Index.cell(-33.99026, 22.43891),
                res.detection().toCopyLine());
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
    void domainEntitiesEnforceInvariants() {
        var loc = Location.of(-10.5, -55.5);
        assertEquals(H3Index.cell(-10.5, -55.5), loc.h3());
        assertEquals(H3Index.toHex(loc.h3()), loc.h3Hex());
        assertThrows(IllegalArgumentException.class, () -> Location.of(95, 0));
        assertThrows(IllegalArgumentException.class, () -> Location.of(0, -181));

        var src = new Detection.Source("MODIS_C6.1", "bronze/raw/a.csv");
        var at = Instant.parse("2026-09-02T18:05:00Z");
        var ok = new Detection("MODIS", "Aqua", at, loc, 330.1, 300.2, 1.0, 1.0, 25.5, "85", "h", "D", "6.1NRT", src);
        assertEquals(LocalDate.of(2026, 9, 2), ok.acqDate());
        assertThrows(IllegalArgumentException.class,
                () -> new Detection("MODIS", "Aqua", at, loc, null, null, null, null, -1.0, "", "", "", "", src));
        assertThrows(IllegalArgumentException.class,
                () -> new Detection("", "Aqua", at, loc, null, null, null, null, 1.0, "", "", "", "", src));
        assertThrows(IllegalArgumentException.class, () -> new Detection.Source("", "f"));

        // campos ausentes viram vazio (NULL no COPY); texto com virgula e protegido por aspas
        var sparse = new Detection("VIIRS", "SNPP", at, loc, null, null, null, null, null, null, null, null, null,
                new Detection.Source("VIIRS_S-NPP", "pasta,com virgula.csv"));
        assertTrue(sparse.toCopyLine().contains(",VIIRS_S-NPP,\"pasta,com virgula.csv\","));

        var sensor = new Sensor("MODIS", "Moderate Resolution Imaging Spectroradiometer", 1000, "d");
        assertEquals(1000, sensor.nominalResolutionMeters());
        assertThrows(IllegalArgumentException.class, () -> new Sensor("X", "x", 0, "d"));
        assertThrows(IllegalArgumentException.class, () -> new Satellite("", "MODIS", "p", "q"));
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
    void h3AncestorByBitsMatchesLibrary() {
        double[][] points = {{-10.5, -55.5}, {0, 0}, {-33.99, 22.44}, {67.78, 101.81}, {-89.9, 179.9},
                {89.9, -179.9}, {-3.1, -60.0}, {35.68, 139.69}};
        for (double[] p : points) {
            long base = H3Index.cell(p[0], p[1]);
            assertEquals(H3Index.BASE_RES, H3Index.resolution(base));
            for (int res : H3Index.AGG_RESOLUTIONS) {
                assertEquals(H3Index.parent(base, res), H3Index.parentBits(base, res),
                        "ancestral por bits difere da biblioteca na resolucao " + res);
                assertEquals(res, H3Index.resolution(H3Index.parentBits(base, res)));
            }
        }
    }

    @Test
    void h3HexRoundTripAndValidation() {
        long c = H3Index.cell(-10.5, -55.5, 6);
        assertEquals(c, H3Index.fromHex(H3Index.toHex(c)));
        assertThrows(IllegalArgumentException.class, () -> H3Index.fromHex("nao-e-h3"));
        assertThrows(IllegalArgumentException.class, () -> H3Index.fromHex("ffffffffffffffff"));
        double[] ctr = H3Index.center(c);
        assertEquals(c, H3Index.cell(ctr[0], ctr[1], 6));
    }

    @Test
    void h3ZoomMappingAndCover() {
        assertEquals(3, H3Index.resolutionForZoom(2));
        assertEquals(4, H3Index.resolutionForZoom(4));
        assertEquals(6, H3Index.resolutionForZoom(7));
        assertEquals(7, H3Index.resolutionForZoom(8));
        var b = new BBox(-56, -11, -55, -10);
        assertFalse(H3Index.cover(b, 6).isEmpty());
        assertTrue(H3Index.estimateCover(b, 8) > H3Index.estimateCover(b, 6));
        // bbox menor que uma celula ainda devolve a celula do centro
        assertEquals(1, H3Index.cover(new BBox(-55.5001, -10.5001, -55.5, -10.5), 3).size());
    }

    @Test
    void regionValidationAndAdaptiveCover() {
        assertTrue(Region.of("-56,-11,-55,-10", null).isBBox());
        assertThrows(IllegalArgumentException.class, () -> Region.of(null, null));
        assertThrows(IllegalArgumentException.class, () -> Region.of("-56,-11,-55,-10", "8a2a1072b59ffff"));
        String cell4 = H3Index.toHex(H3Index.cell(-10.5, -55.5, 4));
        Region byCells = Region.of(null, cell4);
        assertFalse(byCells.isBBox());
        // resolucao fina maior que a base e rejeitada
        assertThrows(IllegalArgumentException.class,
                () -> Region.of(null, H3Index.toHex(H3Index.cell(-10.5, -55.5, 10))));
        // regiao continental desce de resolucao ate caber no limite de celulas
        var brazil = Region.of("-74,-34,-34,6", null);
        var cover = brazil.coverFitting(H3Index.BASE_RES, 5000);
        assertTrue(cover.res() < H3Index.BASE_RES);
        assertTrue(cover.cells().size() <= 5000);
        // celulas: filhas na resolucao pedida
        assertEquals(49, byCells.coverFitting(6, 20000).cells().size());
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
}
