package com.fireatlas;

import com.fireatlas.domain.BBox;
import com.fireatlas.domain.GridMath;
import com.fireatlas.api.QueryParams;
import com.fireatlas.ingestion.DetectionRowParser;
import com.fireatlas.query.Anomaly;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class UnitTests {

    private static final String VIIRS_HEADER =
            "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_t31,frp,daynight";

    @Test
    void parsesRealVIIRSNrtLine() {
        var p = new DetectionRowParser(VIIRS_HEADER, "VIIRS", "VIIRS_S-NPP", "bronze/raw/x.csv");
        var row = p.parse("-33.99026,22.43891,305.28,0.49,0.4,2026-08-01,0001,SNPP,SNPP,n,2.0NRT,281.04,1.44,N");
        assertNotNull(row);
        assertEquals(LocalDate.of(2026, 8, 1), row.date());
        assertEquals(
                "VIIRS,SNPP,2026-08-01,2026-08-01T00:01:00Z,-33.99026,22.43891,305.28,281.04,0.49,0.4,1.44,n,n,N,2.0NRT,VIIRS_S-NPP,bronze/raw/x.csv",
                row.csvLine());
    }

    @Test
    void malformedLineReturnsNull() {
        var p = new DetectionRowParser(VIIRS_HEADER, "VIIRS", "VIIRS_S-NPP", "f");
        assertNull(p.parse("abc,def,1,1,1,not-a-date,0001,SNPP,SNPP,n,2.0NRT,1,1,N"));
    }

    @Test
    void missingRequiredColumnFails() {
        assertThrows(IllegalArgumentException.class,
                () -> new DetectionRowParser("latitude,longitude", "MODIS", "MODIS_C6.1", "f"));
    }

    @Test
    void confidenceIsHarmonizedBySensorWithoutCrossSensorCoercion() {
        // MODIS: escala numerica documentada 0..100
        assertEquals("l", DetectionRowParser.confidenceClass("MODIS", "0"));
        assertEquals("l", DetectionRowParser.confidenceClass("MODIS", "29"));
        assertEquals("n", DetectionRowParser.confidenceClass("MODIS", "30"));
        assertEquals("n", DetectionRowParser.confidenceClass("MODIS", "79"));
        assertEquals("h", DetectionRowParser.confidenceClass("MODIS", "80"));
        assertEquals("h", DetectionRowParser.confidenceClass("MODIS", "100"));
        assertEquals("", DetectionRowParser.confidenceClass("MODIS", "101"));
        assertEquals("", DetectionRowParser.confidenceClass("MODIS", "150"));
        assertEquals("", DetectionRowParser.confidenceClass("MODIS", "-1"));
        assertEquals("", DetectionRowParser.confidenceClass("MODIS", "hacked"));
        assertEquals("", DetectionRowParser.confidenceClass("MODIS", "h"));
        assertEquals("", DetectionRowParser.confidenceClass("MODIS", ""));

        // VIIRS: categorias documentadas l/n/h e low/nominal/high
        assertEquals("l", DetectionRowParser.confidenceClass("VIIRS", "l"));
        assertEquals("l", DetectionRowParser.confidenceClass("VIIRS", "low"));
        assertEquals("n", DetectionRowParser.confidenceClass("VIIRS", "n"));
        assertEquals("n", DetectionRowParser.confidenceClass("VIIRS", "nominal"));
        assertEquals("h", DetectionRowParser.confidenceClass("VIIRS", "h"));
        assertEquals("h", DetectionRowParser.confidenceClass("VIIRS", "HIGH"));
        assertEquals("", DetectionRowParser.confidenceClass("VIIRS", "hacked"));
        assertEquals("", DetectionRowParser.confidenceClass("VIIRS", "low_fake"));
        assertEquals("", DetectionRowParser.confidenceClass("VIIRS", "80"));
        assertEquals("", DetectionRowParser.confidenceClass("VIIRS", ""));

        assertEquals("", DetectionRowParser.confidenceClass("UNKNOWN", "80"));
    }

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
    void parserPreservesMissingFrpAndRejectsInvalidCoordsOrTime() {
        var p = new DetectionRowParser(VIIRS_HEADER, "VIIRS", "VIIRS_NOAA-20", "bronze/raw/file,1.csv");
        // FRP vazia permanece ausente (string vazia -> NULL no COPY)
        var missingFrp = p.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,nominal,2.0NRT,281.0,,N");
        assertNotNull(missingFrp);
        assertTrue(missingFrp.csvLine().contains(",,nominal,n,N,2.0NRT,VIIRS_NOAA-20,\"bronze/raw/file,1.csv\""));

        // Coordenadas fora da faixa, nao finitas, horario invalido ou FRP nao finita sao rejeitados como malformados
        assertNull(p.parse("NaN,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,n,2.0NRT,281.0,1.0,N"));
        assertNull(p.parse("95.0,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,n,2.0NRT,281.0,1.0,N"));
        assertNull(p.parse("-3.5,185.0,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,n,2.0NRT,281.0,1.0,N"));
        assertNull(p.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,2599,N20,VIIRS,n,2.0NRT,281.0,1.0,N"));
        assertNull(p.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,n,2.0NRT,281.0,NaN,N"));
    }

    @Test
    void parserRejectsNegativeFrpInvalidUtcTimeAndInvalidConfidenceBySensor() {
        var modisHeader = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight";
        var modis = new DetectionRowParser(modisHeader, "MODIS", "MODIS_C6.1", "m.csv");
        var viirs = new DetectionRowParser(VIIRS_HEADER, "VIIRS", "VIIRS_NOAA-20", "v.csv");

        // FRP negativa ou -0 rejeitada; 0.0 aceita; vazia preservada como ausente
        assertNull(viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,n,2.0NRT,281.0,-0.01,N"));
        assertNull(viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,n,2.0NRT,281.0,-5,N"));
        assertNull(viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,n,2.0NRT,281.0,-0.0,N"));
        assertNotNull(viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,n,2.0NRT,281.0,0.0,N"));

        // Horario UTC estritamente 0000..2359
        assertNull(viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,2400,N20,VIIRS,n,2.0NRT,281.0,1.0,N"));
        assertNull(viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,2360,N20,VIIRS,n,2.0NRT,281.0,1.0,N"));
        assertNull(viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,-100,N20,VIIRS,n,2.0NRT,281.0,1.0,N"));
        var midnight = viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,0,N20,VIIRS,n,2.0NRT,281.0,1.0,N");
        var endOfDay = viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,2359,N20,VIIRS,n,2.0NRT,281.0,1.0,N");
        assertNotNull(midnight);
        assertTrue(midnight.csvLine().contains("2026-11-12T00:00:00Z"));
        assertNotNull(endOfDay);
        assertTrue(endOfDay.csvLine().contains("2026-11-12T23:59:00Z"));

        // Confianca invalida ou fora da escala do sensor e rejeitada; vazia e preservada como ausente
        assertNull(modis.parse("-3.23,-53.91,308.0,1.1,1.1,2026-11-12,0709,A,150,6.1NRT,290.0,11.0,N"));
        assertNull(modis.parse("-3.23,-53.91,308.0,1.1,1.1,2026-11-12,0709,A,-1,6.1NRT,290.0,11.0,N"));
        assertNull(modis.parse("-3.23,-53.91,308.0,1.1,1.1,2026-11-12,0709,A,hacked,6.1NRT,290.0,11.0,N"));
        assertNull(modis.parse("-3.23,-53.91,308.0,1.1,1.1,2026-11-12,0709,A,h,6.1NRT,290.0,11.0,N"));
        assertNotNull(modis.parse("-3.23,-53.91,308.0,1.1,1.1,2026-11-12,0709,A,,6.1NRT,290.0,11.0,N"));
        var validModis = modis.parse("-3.23,-53.91,308.0,1.1,1.1,2026-11-12,0709,A,100,6.1NRT,290.0,11.0,N");
        assertNotNull(validModis);
        assertTrue(validModis.csvLine().contains(",100,h,N,"));
        assertEquals(64, validModis.sourceRowHash().length());
        assertEquals(64, validModis.sourceSemanticHash().length());
        assertTrue(validModis.toStagedCsvLine(2)
                .endsWith(",2," + validModis.sourceRowHash() + "," + validModis.sourceSemanticHash()));

        assertNull(viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,hacked,2.0NRT,281.0,1.0,N"));
        assertNull(viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,low_fake,2.0NRT,281.0,1.0,N"));
        assertNull(viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,80,2.0NRT,281.0,1.0,N"));
        assertNotNull(viirs.parse("-3.5,-53.5,305.0,0.4,0.4,2026-11-12,0001,N20,VIIRS,,2.0NRT,281.0,1.0,N"));
    }

    @Test
    void semanticHashReflectsHeaderInterpretationWhilePreservingRawRowHash() {
        String hOriginal = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,frp,confidence,version,bright_t31,daynight";
        String hSwapped = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,frp,version,bright_t31,daynight";
        String hWithExtraCol = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,frp,version,bright_t31,daynight,instrument";

        String sameRawLine = "-3.500,-53.500,310.0,1.0,1.0,2026-11-12,0709,A,10,80,6.1NRT,290.0,N";
        String equivalentValuesLine = "-3.5,-53.5,310.00,1.0,1.0,2026-11-12,709,A,80,10.00,6.1NRT,290.0,N,MODIS_IGNORED";

        var pOrig = new DetectionRowParser(hOriginal, "MODIS", "MODIS_C6.1", "f.csv");
        var pSwap = new DetectionRowParser(hSwapped, "MODIS", "MODIS_C6.1", "f.csv");
        var pExtra = new DetectionRowParser(hWithExtraCol, "MODIS", "MODIS_C6.1", "f.csv");

        var rOrig = pOrig.parse(sameRawLine);
        var rFlipped = pSwap.parse(sameRawLine);
        var rEquiv = pExtra.parse(equivalentValuesLine);

        assertNotNull(rOrig);
        assertNotNull(rFlipped);
        assertNotNull(rEquiv);

        // Mesmo texto bruto -> mesmo sourceRowHash, porem significados cientificos distintos -> sourceSemanticHash diferente
        assertEquals(rOrig.sourceRowHash(), rFlipped.sourceRowHash());
        assertNotEquals(rOrig.sourceSemanticHash(), rFlipped.sourceSemanticHash());
        assertNotEquals(rOrig.csvLine(), rFlipped.csvLine());

        // Valores efetivos equivalentes mesmo com colunas trocadas / zeros a esquerda / extra -> sourceRowHash distinto, semanticHash igual
        assertNotEquals(rOrig.sourceRowHash(), rEquiv.sourceRowHash());
        assertEquals(rOrig.sourceSemanticHash(), rEquiv.sourceSemanticHash());
    }

    @Test
    void c2f03Float32CanonicalRealAvoidsDoubleRounding() {
        String header = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,confidence,version,bright_t31,frp,daynight";
        var parser = new DetectionRowParser(header, "MODIS", "MODIS_C6.1", "c2f03.csv");

        var one = parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.0,N");
        var tieToEven = parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.000000059604644775390625,N");
        var adversarialUp = parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.0000000596046447753906251,N");
        var oddTieUp = parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.000000178813934326171875,N");
        var belowOddTie = parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.0000001788139343261718749,N");
        var subnormal = parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1e-40,N");
        var minSubnormal = parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.401298464e-45,N");
        var zeroFrp = parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,0.0,N");
        var nullFrp = parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,,N");
        var negZeroBrightness = parser.parse("-3.5,-53.5,-0.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.0,N");
        var posZeroBrightness = parser.parse("-3.5,-53.5,0.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.0,N");
        var firmsA = parser.parse("-3.41340,-53.52631,321.45,1.23,1.07,2026-11-12,0709,T,74,6.1NRT,295.12,18.64,N");
        var firmsB = parser.parse("-3.41340,-53.52631,321.45,1.23,1.07,2026-11-12,0709,T,74,6.1NRT,295.12,18.640002,N");

        assertNotNull(one);
        assertNotNull(tieToEven);
        assertNotNull(adversarialUp);
        assertNotNull(oddTieUp);
        assertNotNull(belowOddTie);
        assertNotNull(subnormal);
        assertNotNull(minSubnormal);
        assertNotNull(zeroFrp);
        assertNotNull(nullFrp);
        assertNotNull(negZeroBrightness);
        assertNotNull(posZeroBrightness);
        assertNotNull(firmsA);
        assertNotNull(firmsB);

        assertEquals(one.sourceSemanticHash(), tieToEven.sourceSemanticHash());
        assertNotEquals(one.sourceSemanticHash(), adversarialUp.sourceSemanticHash(),
                "C2-F03: 1.0000000596046447753906251 (real 3f800001) nao pode receber o mesmo hash semantico de 1.0 (3f800000)");
        assertEquals(adversarialUp.sourceSemanticHash(), belowOddTie.sourceSemanticHash());
        assertNotEquals(adversarialUp.sourceSemanticHash(), oddTieUp.sourceSemanticHash());
        assertNotEquals(zeroFrp.sourceSemanticHash(), subnormal.sourceSemanticHash());
        assertNotEquals(zeroFrp.sourceSemanticHash(), minSubnormal.sourceSemanticHash());
        assertNotEquals(zeroFrp.sourceSemanticHash(), nullFrp.sourceSemanticHash(), "FRP zero e FRP NULL devem permanecer distintos");
        assertEquals(negZeroBrightness.sourceSemanticHash(), posZeroBrightness.sourceSemanticHash());
        assertNotEquals(firmsA.sourceSemanticHash(), firmsB.sourceSemanticHash());

        // Overflow/underflow fora da faixa float32 e nao finitos devem ser rejeitados
        assertNull(parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1e39,N"));
        assertNull(parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1e-50,N"));
        assertNull(parser.parse("-3.5,-53.5,1e39,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,1.0,N"));
        assertNull(parser.parse("-3.5,-53.5,310.0,1.0,1.0,2026-11-12,0709,A,80,6.1NRT,290.0,Infinity,N"));
        assertEquals("v2-ieee754-direct", DetectionRowParser.SEMANTIC_HASH_VERSION);
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
