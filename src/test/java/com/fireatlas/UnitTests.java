package com.fireatlas;

import com.fireatlas.h3.policy.H3Settings;
import com.fireatlas.analysis.policy.Anomaly;
import com.fireatlas.common.controller.QueryParams;
import com.fireatlas.detection.entity.Detection;
import com.fireatlas.detection.entity.Location;
import com.fireatlas.h3.policy.H3Index;
import com.fireatlas.ingestion.integration.DetectionRowParser;
import com.fireatlas.ingestion.integration.H3Options;
import com.fireatlas.region.entity.BBox;
import com.fireatlas.region.entity.Region;
import com.fireatlas.sensor.entity.Satellite;
import com.fireatlas.sensor.entity.Sensor;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
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
        assertNull(res.detection().location().h3(), "o backend nao calcula H3: a fonte nao trouxe, fica ausente");
        assertEquals("VIIRS_S-NPP", res.detection().source().product());
        assertEquals(
                "VIIRS,SNPP,2026-08-01,2026-08-01T00:01:00Z,-33.99026,22.43891,305.28,281.04,0.49,0.4,1.44,n,n,N,2.0NRT,VIIRS_S-NPP,bronze/raw/x.csv,"
                        + ",SNPP,",
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
        var loc = new Location(-10.5, -55.5, TestH3.cell(-10.5, -55.5)); // H3 vindo da fonte
        assertEquals(H3Index.toHex(loc.h3()), loc.h3Hex());
        assertNull(Location.of(-10.5, -55.5).h3());                       // sem H3: nada e calculado
        assertNull(Location.of(-10.5, -55.5).h3Hex());
        assertThrows(IllegalArgumentException.class, () -> Location.of(95, 0));
        assertThrows(IllegalArgumentException.class, () -> Location.of(0, -181));

        var src = new Detection.Source("MODIS_C6.1", "bronze/raw/a.csv");
        var at = Instant.parse("2026-09-02T18:05:00Z");
        var ok = new Detection("MODIS", "Aqua", "MODIS", at, loc, 330.1, 300.2, 1.0, 1.0, 25.5, "85", "h", "D",
                "6.1NRT", 0, src);
        assertEquals(LocalDate.of(2026, 9, 2), ok.acqDate());
        assertThrows(IllegalArgumentException.class,
                () -> new Detection("MODIS", "Aqua", "MODIS", at, loc, null, null, null, null, -1.0, "", "", "", "",
                        null, src));
        assertThrows(IllegalArgumentException.class,
                () -> new Detection("", "Aqua", "MODIS", at, loc, null, null, null, null, 1.0, "", "", "", "",
                        null, src));
        assertThrows(IllegalArgumentException.class, () -> new Detection.Source("", "f"));

        // campos ausentes viram vazio (NULL no COPY); texto com virgula e protegido por aspas
        var sparse = new Detection("VIIRS", "SNPP", null, at, loc, null, null, null, null, null, null, null, null,
                null, null, new Detection.Source("VIIRS_S-NPP", "pasta,com virgula.csv"));
        assertTrue(sparse.toCopyLine().contains(",VIIRS_S-NPP,\"pasta,com virgula.csv\","));

        var sensor = new Sensor("MODIS", "Moderate Resolution Imaging Spectroradiometer", 1000, "d");
        assertEquals(1000, sensor.nominalResolutionMeters());
        assertThrows(IllegalArgumentException.class, () -> new Sensor("X", "x", 0, "d"));
        assertThrows(IllegalArgumentException.class, () -> new Satellite("", "MODIS", "p", "q"));
    }

    @Test
    void readsH3FromTheSourceAndNeverComputesIt() {
        long cell = TestH3.cell(-10.5, -55.5);
        String base = "-10.5,-55.5,305.28,0.49,0.4,2026-09-02,1805,SNPP,SNPP,n,2.0NRT,281.04,1.44,D";
        var p = new DetectionRowParser(VIIRS_HEADER + ",h3", "VIIRS", "VIIRS_S-NPP", "f.csv");

        // hexadecimal e inteiro de 64 bits sao aceitos e gravados exatamente como vieram
        assertEquals(cell, p.parse(base + "," + H3Index.toHex(cell)).detection().location().h3());
        assertEquals(cell, p.parse(base + "," + cell).detection().location().h3());
        // resolucao mais fina que a base tambem e aceita
        long fine = TestH3.cell(-10.5, -55.5, 9);
        assertEquals(TestH3.cell(-10.5, -55.5), p.parse(base + "," + H3Index.toHex(fine)).detection().location().h3(), "finer than the base is normalised to its ancestor at the base resolution");
        // valor vazio: fica ausente (nunca e calculado a partir de lat/lon)
        assertNull(p.parse(base + ",").detection().location().h3());
        // invalido ou com resolucao menor que a base: o registro e rejeitado com motivo
        assertEquals(DetectionRowParser.H3_INVALIDO, p.parse(base + ",abc").rejectReason());
        assertEquals(DetectionRowParser.H3_INVALIDO,
                p.parse(base + "," + H3Index.toHex(TestH3.cell(-10.5, -55.5, 5))).rejectReason());
        // sem a coluna no cabecalho: tambem nao calcula
        assertNull(parser().parse(base).detection().location().h3());
        // nome da coluna configuravel
        var q = new DetectionRowParser(VIIRS_HEADER + ",h3_index", "VIIRS", "VIIRS_S-NPP", "f.csv",
                new H3Options("H3_INDEX"));
        assertEquals(cell, q.parse(base + "," + H3Index.toHex(cell)).detection().location().h3());
        assertEquals(cell, H3Index.parseIndex(" " + cell + " "));
    }

    @Test
    void goldFormatOneColumnPerResolutionUsesTheFinestAsBaseAndChecksConsistency() {
        String base = "-10.5,-55.5,305.28,0.49,0.4,2026-09-02,1805,SNPP,SNPP,n,2.0NRT,281.04,1.44,D";
        String header = VIIRS_HEADER + ",h3_0,h3_1,h3_2,h3_3,h3_4,h3_5,h3_6";
        var p = new DetectionRowParser(header, "VIIRS", "VIIRS_S-NPP", "gold/x.parquet", new H3Options("h3", 6));
        long c6 = TestH3.cell(-10.5, -55.5, 6);
        java.util.function.Function<long[], String> join = cells -> java.util.Arrays.stream(cells)
                .mapToObj(H3Index::toHex).collect(java.util.stream.Collectors.joining(","));
        long[] parents = new long[7];
        for (int r = 0; r <= 6; r++) parents[r] = H3Index.parent(c6, r);

        var ok = p.parse(base + "," + join.apply(parents));
        assertTrue(ok.accepted(), ok.rejectReason());
        assertEquals(c6, ok.detection().location().h3()); // a coluna da resolucao-base (6) e a guardada

        // uma coluna mais grossa que nao e o "pai" da mais fina: registro inconsistente
        long[] wrong = parents.clone();
        wrong[3] = TestH3.cell(20.0, 10.0, 3);
        assertEquals(DetectionRowParser.H3_INCONSISTENTE, p.parse(base + "," + join.apply(wrong)).rejectReason());

        // resolucao-base que o arquivo nao traz: falha clara, em vez de rejeitar tudo em silencio
        var ex = assertThrows(IllegalArgumentException.class,
                () -> new DetectionRowParser(header, "VIIRS", "VIIRS_S-NPP", "f", new H3Options("h3", 8)));
        assertTrue(ex.getMessage().contains("resolucao-base"), ex.getMessage());

        // outras grafias de coluna e outra base (5): usa h3_r5 e confere de 0 a 4
        String header2 = VIIRS_HEADER + ",h3_r0,h3_r1,h3_r2,h3_r3,h3_r4,h3_r5";
        var q = new DetectionRowParser(header2, "VIIRS", "VIIRS_S-NPP", "f", new H3Options("h3", 5));
        long c5 = TestH3.cell(-10.5, -55.5, 5);
        long[] p5 = new long[6];
        for (int r = 0; r <= 5; r++) p5[r] = H3Index.parent(c5, r);
        assertEquals(c5, q.parse(base + "," + join.apply(p5)).detection().location().h3());
    }

    @Test
    void h3SettingsDefineBaseAggregationLevelsAndZoomMapping() {
        var gold = new H3Settings(6);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6}, gold.aggResolutions());
        assertEquals(3, gold.resolutionForZoom(2));
        assertEquals(6, gold.resolutionForZoom(7));
        assertEquals(6, gold.resolutionForZoom(8)); // limitado pela resolucao-base
        assertEquals(2, new H3Settings(2).resolutionForZoom(2));
        assertEquals(9, H3Settings.defaults().aggResolutions().length); // 0..8
        assertThrows(IllegalArgumentException.class, () -> new H3Settings(16));
        assertThrows(IllegalArgumentException.class, () -> new H3Options("h3", -1));
    }

    @Test
    void keepsOriginalInstrumentAndFireType() {
        String header = VIIRS_HEADER + ",type";
        var p = new DetectionRowParser(header, "VIIRS", "VIIRS_S-NPP", "bronze/raw/a.csv");
        var line = "-10.5,-55.5,305.28,0.49,0.4,2026-09-02,1805,N,VIIRS,n,2.0NRT,281.04,1.44,D,2";
        var res = p.parse(line);
        assertTrue(res.accepted());
        assertEquals("VIIRS", res.detection().instrument());
        assertEquals(2, res.detection().fireType());
        assertTrue(res.detection().toCopyLine().endsWith(",VIIRS,2"));
        assertEquals(DetectionRowParser.CAMPO_NUMERICO_INVALIDO,
                p.parse(line.substring(0, line.length() - 1) + "x").rejectReason());
        // sem coluna type (arquivos NRT): tipo ausente, nao rejeitado
        assertNull(parser().parse("-10.5,-55.5,305.28,0.49,0.4,2026-09-02,1805,SNPP,SNPP,n,2.0NRT,281.04,1.44,N")
                .detection().fireType());
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
            long base = TestH3.cell(p[0], p[1]);
            assertEquals(8, H3Index.resolution(base));
            for (int res : H3Settings.defaults().aggResolutions()) {
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
    void h3ZoomMapping() {
        assertEquals(3, H3Settings.defaults().resolutionForZoom(2));
        assertEquals(4, H3Settings.defaults().resolutionForZoom(4));
        assertEquals(6, H3Settings.defaults().resolutionForZoom(7));
        assertEquals(7, H3Settings.defaults().resolutionForZoom(8));
    }

    @Test
    void aggregatedQueriesRequireCellsAndNeverConvertBBox() {
        var byBBox = Region.of("-56,-11,-55,-10", null);
        var ex = assertThrows(IllegalArgumentException.class, () -> byBBox.coverFitting(6, 20000));
        assertTrue(ex.getMessage().contains("cells"), ex.getMessage());
        assertEquals(1.0, byBBox.maxAbsLat() / 11.0, 1e-9); // bbox: latitude vem do proprio retangulo

        // uma celula grossa pedida em resolucao fina desce de resolucao ate caber no limite
        var byCell = Region.of(null, H3Index.toHex(TestH3.cell(-10.5, -55.5, 3)));
        var cover = byCell.coverFitting(8, 5000);
        assertEquals(7, cover.res());
        assertEquals(2401, cover.cells().size());
        assertEquals(80.0, byCell.maxAbsLat()); // por celulas: limite seguro, sem converter H3 em coordenadas
    }

    @Test
    void regionValidationAndAdaptiveCover() {
        assertTrue(Region.of("-56,-11,-55,-10", null).isBBox());
        assertThrows(IllegalArgumentException.class, () -> Region.of(null, null));
        assertThrows(IllegalArgumentException.class, () -> Region.of("-56,-11,-55,-10", "8a2a1072b59ffff"));
        String cell4 = H3Index.toHex(TestH3.cell(-10.5, -55.5, 4));
        Region byCells = Region.of(null, cell4);
        assertFalse(byCells.isBBox());
        // celula mais fina que a base e aceita: ao consultar e reduzida ao ancestral na resolucao-base
        var fine = Region.of(null, H3Index.toHex(TestH3.cell(-10.5, -55.5, 10)));
        assertEquals(List.of(TestH3.cell(-10.5, -55.5, 8)), fine.detectionCells(8));
        assertEquals(List.of(TestH3.cell(-10.5, -55.5, 6)), fine.detectionCells(6));
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
