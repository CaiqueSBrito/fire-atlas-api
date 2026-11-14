package com.fireatlas;

import com.fireatlas.oracle.FirmsIndependentOracle;
import org.junit.jupiter.api.Test;

import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

class FirmsIndependentOracleTest {

    @Test
    void independentOracleVerifiesHashesStreamAndAll58ExpectedPairs() throws Exception {
        var report = FirmsIndependentOracle.verifyAndCompute(
                FirmsIndependentOracle.FIXTURE_DIR,
                FirmsIndependentOracle.DEFAULT_MAX_DISTANCE_METERS,
                FirmsIndependentOracle.DEFAULT_MAX_SECONDS
        );

        assertEquals(3, report.sources().size());
        assertEquals(31, report.modisTotal());
        assertEquals(165, report.viirsTotal());
        assertEquals(12, report.modisMatched());
        assertEquals(44, report.distinctViirsMatched());
        assertEquals(58, report.pairs().size());

        // N5: Diferenciacao explicita dos fluxos documentados FIRMS sem presumir STANDARD por exclusao
        assertEquals(FirmsIndependentOracle.ProcessingStream.URT,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("6.1URT"));
        assertEquals(FirmsIndependentOracle.ProcessingStream.URT,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("2.0URT"));
        assertEquals(FirmsIndependentOracle.ProcessingStream.RT,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("6.1RT"));
        assertEquals(FirmsIndependentOracle.ProcessingStream.RT,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("2.0RT"));
        assertEquals(FirmsIndependentOracle.ProcessingStream.NRT,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("6.1NRT"));
        assertEquals(FirmsIndependentOracle.ProcessingStream.NRT,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("2.0NRT"));
        assertEquals(FirmsIndependentOracle.ProcessingStream.STANDARD,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("6.1"));
        assertEquals(FirmsIndependentOracle.ProcessingStream.STANDARD,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("6.0"));
        assertEquals(FirmsIndependentOracle.ProcessingStream.STANDARD,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("2.0"));
        assertEquals(FirmsIndependentOracle.ProcessingStream.STANDARD,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("2"));
        assertEquals(FirmsIndependentOracle.ProcessingStream.STANDARD,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("1.0"));
        assertEquals(FirmsIndependentOracle.ProcessingStream.STANDARD,
                FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("1"));

        assertThrows(IllegalArgumentException.class,
                () -> FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("UNKNOWN"));
        assertThrows(IllegalArgumentException.class,
                () -> FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("2.0XRT"));
        assertThrows(IllegalArgumentException.class,
                () -> FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("2.0BETA"));
        assertThrows(IllegalArgumentException.class,
                () -> FirmsIndependentOracle.ProcessingStream.fromFirmsVersion("99.9"));
        assertThrows(IllegalArgumentException.class,
                () -> FirmsIndependentOracle.ProcessingStream.fromFirmsVersion(""));
        assertThrows(IllegalArgumentException.class,
                () -> FirmsIndependentOracle.ProcessingStream.fromFirmsVersion(null));

        // Verifica limites geodesicos e temporais de referencia independentes do PostgreSQL
        var closest = report.pairs().stream()
                .min(Comparator.comparingDouble(FirmsIndependentOracle.ExpectedPair::vincentyDistanceMeters))
                .orElseThrow();
        assertEquals(39.808, closest.vincentyDistanceMeters(), 0.01);
        assertEquals(-8580L, closest.timeOffsetSeconds());

        var farthest = report.pairs().stream()
                .max(Comparator.comparingDouble(FirmsIndependentOracle.ExpectedPair::vincentyDistanceMeters))
                .orElseThrow();
        assertEquals(747.800, farthest.vincentyDistanceMeters(), 0.01);
        assertEquals(-6000L, farthest.timeOffsetSeconds());
    }
}
