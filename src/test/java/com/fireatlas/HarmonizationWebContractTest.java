package com.fireatlas;

import com.fireatlas.api.AnalysisController;
import com.fireatlas.api.DetectionController;
import com.fireatlas.api.Dto;
import com.fireatlas.api.MetaController;
import com.fireatlas.query.DetectionRepository;
import com.fireatlas.query.HarmonizationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = {AnalysisController.class, DetectionController.class, MetaController.class})
class HarmonizationWebContractTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private DetectionRepository repo;

    @MockitoBean
    private HarmonizationService harmonizationService;

    private static Dto.HarmonizationResponse sampleResponse(boolean includeAssociations, List<Dto.HarmonizationResponse.CandidateAssociation> associations) {
        Map<String, Dto.HarmonizationResponse.IndicatorStatus> unavailable = new LinkedHashMap<>();
        unavailable.put("viirsToModisFrpRatio", new Dto.HarmonizationResponse.IndicatorStatus(
                "METHOD_NOT_APPROVED", "Razao nao validada cientificamente."));
        unavailable.put("derivedClassification", new Dto.HarmonizationResponse.IndicatorStatus(
                "NOT_EVALUATED", "Classificacao nao avaliada."));

        long returned = associations == null ? 0 : associations.size();
        return new Dto.HarmonizationResponse(
                "-54.0,-4.0,-53.0,-3.0",
                LocalDate.of(2026, 11, 12),
                LocalDate.of(2026, 11, 13),
                new Dto.HarmonizationResponse.Params(750, 180, 20000, includeAssociations, 500),
                new Dto.HarmonizationResponse.Result(1, 1, 1.0, null, null, null, null, null, false),
                new Dto.HarmonizationResponse.Method("Exploratorio", "Descricao", List.of("Limitacao")),
                null,
                "exploratory-spatial-temporal-v0.2",
                new Dto.HarmonizationResponse.ScientificState(
                        "EXPLORATORY_ASSOCIATION", "METHOD_NOT_APPROVED", false,
                        "NOT_EVALUATED", "PRODUCT_NATIVE_NOT_EQUIVALENT",
                        "BOTH_SENSORS_WITH_CANDIDATE_PAIRS", List.of("Nao aprovado")),
                new Dto.HarmonizationResponse.DescriptiveSummary(1, 1, 1, false, 1, 1, 1, returned, false, 0, 1, 1, 0, false),
                associations,
                unavailable
        );
    }

    @Test
    void harmonizedDefaultsToSummaryOnlyOptInIncludeAssociationsFalse() throws Exception {
        when(harmonizationService.harmonize(any(), any(), any(), anyInt(), anyInt(), anyInt(), eq(false), anyInt()))
                .thenReturn(sampleResponse(false, null));

        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.params.includeAssociations").value(false))
                .andExpect(jsonPath("$.summary.candidateAssociationsTotal").value(1))
                .andExpect(jsonPath("$.summary.associationsReturned").value(0))
                .andExpect(jsonPath("$.associations").doesNotExist());

        verify(harmonizationService).harmonize(any(), any(), any(), eq(750), eq(180), eq(20000), eq(false), eq(500));
    }

    @Test
    void harmonizedExposesScientificStateUnavailableIndicatorsAndCompositeIdentityWhenOptedIn() throws Exception {
        var modisObs = new Dto.HarmonizationResponse.DetectionObservation(
                101L, LocalDate.of(2026, 11, 12), "2026-11-12:101", 2L,
                "local:modis.csv#L2:raw=abcdef012345:sem=fedcba987654",
                "MODIS", "A", "MODIS_C6.1", "6.1NRT", "local:modis.csv",
                Instant.parse("2026-11-12T07:09:00Z"), -3.4134, -53.52631,
                null, "MW", "68", "n", "MODIS_NUMERIC_0_100_NATIVE", "N",
                "local:modis.csv#L2", "abcdef012345".repeat(5) + "abcd",
                "fedcba987654".repeat(5) + "fedc", "0123456789ab".repeat(5) + "0123");
        var viirsObs = new Dto.HarmonizationResponse.DetectionObservation(
                202L, LocalDate.of(2026, 11, 12), "2026-11-12:202", null, null,
                "VIIRS", "N20", "VIIRS_NOAA-20", null, "local:viirs.csv",
                Instant.parse("2026-11-12T04:46:00Z"), -3.41376, -53.52631,
                6.78f, "MW", "nominal", "n", "VIIRS_CATEGORICAL_L_N_H_NATIVE", "N");
        var pair = new Dto.HarmonizationResponse.CandidateAssociation(
                "EXPLORATORY_CANDIDATE", 39.81, -8580L, 143.0, modisObs, viirsObs);

        when(harmonizationService.harmonize(any(), any(), any(), anyInt(), anyInt(), anyInt(), eq(true), anyInt()))
                .thenReturn(sampleResponse(true, List.of(pair)));

        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13")
                        .param("includeAssociations", "true")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.params.includeAssociations").value(true))
                .andExpect(jsonPath("$.exploratoryMethodVersion").value("exploratory-spatial-temporal-v0.2"))
                .andExpect(jsonPath("$.scientificState.correspondenceMode").value("EXPLORATORY_ASSOCIATION"))
                .andExpect(jsonPath("$.scientificState.harmonizationStatus").value("METHOD_NOT_APPROVED"))
                .andExpect(jsonPath("$.scientificState.scientificallyApproved").value(false))
                .andExpect(jsonPath("$.scientificState.derivedClassificationStatus").value("NOT_EVALUATED"))
                .andExpect(jsonPath("$.result.modisEvaluated").value(1))
                .andExpect(jsonPath("$.result.meanModisFrp").value(nullValue()))
                .andExpect(jsonPath("$.result.meanViirsFrpSum").value(nullValue()))
                .andExpect(jsonPath("$.result.viirsToModisFrpRatio").value(nullValue()))
                .andExpect(jsonPath("$.result.frpCorrelation").value(nullValue()))
                .andExpect(jsonPath("$.result.medianViirsPixelsPerModis").value(nullValue()))
                .andExpect(jsonPath("$.unavailableIndicators.viirsToModisFrpRatio.status").value("METHOD_NOT_APPROVED"))
                .andExpect(jsonPath("$.associations[0].modis.id").value(101))
                .andExpect(jsonPath("$.associations[0].modis.acquiredDate").value("2026-11-12"))
                .andExpect(jsonPath("$.associations[0].modis.observationKey").value("2026-11-12:101"))
                .andExpect(jsonPath("$.associations[0].modis.sourceLineNumber").value(2))
                .andExpect(jsonPath("$.associations[0].modis.sourceRecordKey").value("local:modis.csv#L2:raw=abcdef012345:sem=fedcba987654"))
                .andExpect(jsonPath("$.associations[0].modis.sourceRawLocator").value("local:modis.csv#L2"))
                .andExpect(jsonPath("$.associations[0].modis.sourceRowHash").exists())
                .andExpect(jsonPath("$.associations[0].modis.sourceSemanticHash").exists())
                .andExpect(jsonPath("$.associations[0].modis.sourceFileSha256").exists())
                .andExpect(jsonPath("$.associations[0].modis.sourceSemanticHashStatus").value("CURRENT"))
                .andExpect(jsonPath("$.associations[0].modis.sourceSemanticHashVersion").value("v2-ieee754-direct"))
                .andExpect(jsonPath("$.associations[0].modis.legacySourceSemanticHash").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].modis.frp").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].viirs.id").value(202))
                .andExpect(jsonPath("$.associations[0].viirs.observationKey").value("2026-11-12:202"))
                .andExpect(jsonPath("$.associations[0].viirs.sourceLineNumber").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].viirs.sourceRecordKey").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].viirs.sourceRawLocator").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].viirs.sourceRowHash").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].viirs.sourceSemanticHash").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].viirs.sourceFileSha256").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].viirs.sourceSemanticHashStatus").value("UNKNOWN"))
                .andExpect(jsonPath("$.associations[0].viirs.sourceSemanticHashVersion").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].viirs.legacySourceSemanticHash").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].viirs.version").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].viirs.frp").value(6.78))
                .andExpect(jsonPath("$.summary.exclusiveAssignmentApplied").value(false));

        // C3-F01: valida tambem estados LEGACY_UNVERIFIED e UPDATED_FROM_LEGACY no contrato JSON
        var legacyUnverifiedModis = new Dto.HarmonizationResponse.DetectionObservation(
                303L, LocalDate.of(2026, 11, 12), "2026-11-12:303", 2L,
                "local:legacy.csv#L2:raw=abcdef012345:legacySem=999988887777",
                "MODIS", "A", "MODIS_C6.1", "6.1NRT", "local:legacy.csv",
                Instant.parse("2026-11-12T07:09:00Z"), -3.4134, -53.52631,
                1.0000001f, "MW", "80", "h", "MODIS_NUMERIC_0_100_NATIVE", "N",
                "local:legacy.csv#L2", "abcdef012345".repeat(5) + "abcd",
                null, "0123456789ab".repeat(5) + "0123",
                "LEGACY_UNVERIFIED", "v1-legacy-unverified", "999988887777".repeat(5) + "9999");
        var upgradedViirs = new Dto.HarmonizationResponse.DetectionObservation(
                404L, LocalDate.of(2026, 11, 12), "2026-11-12:404", 2L,
                "local:legacy_v.csv#L2:raw=111122223333:sem=444455556666",
                "VIIRS", "N20", "VIIRS_NOAA-20", "2.0NRT", "local:legacy_v.csv",
                Instant.parse("2026-11-12T07:10:00Z"), -3.4137, -53.52631,
                6.78f, "MW", "high", "h", "VIIRS_CATEGORICAL_L_N_H_NATIVE", "N",
                "local:legacy_v.csv#L2", "111122223333".repeat(5) + "1111",
                "444455556666".repeat(5) + "4444", "0123456789ab".repeat(5) + "0123",
                "UPDATED_FROM_LEGACY", "v2-ieee754-direct", "777766665555".repeat(5) + "7777");
        var legacyPair = new Dto.HarmonizationResponse.CandidateAssociation(
                "EXPLORATORY_CANDIDATE", 33.0, 60L, 1.0, legacyUnverifiedModis, upgradedViirs);
        when(harmonizationService.harmonize(any(), any(), any(), anyInt(), anyInt(), anyInt(), eq(true), anyInt()))
                .thenReturn(sampleResponse(true, List.of(legacyPair)));

        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13")
                        .param("includeAssociations", "true")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.associations[0].modis.sourceSemanticHash").value(nullValue()))
                .andExpect(jsonPath("$.associations[0].modis.sourceSemanticHashStatus").value("LEGACY_UNVERIFIED"))
                .andExpect(jsonPath("$.associations[0].modis.sourceSemanticHashVersion").value("v1-legacy-unverified"))
                .andExpect(jsonPath("$.associations[0].modis.legacySourceSemanticHash").value("999988887777".repeat(5) + "9999"))
                .andExpect(jsonPath("$.associations[0].viirs.sourceSemanticHashStatus").value("UPDATED_FROM_LEGACY"))
                .andExpect(jsonPath("$.associations[0].viirs.sourceSemanticHashVersion").value("v2-ieee754-direct"))
                .andExpect(jsonPath("$.associations[0].viirs.legacySourceSemanticHash").value("777766665555".repeat(5) + "7777"));
    }

    @Test
    void harmonizedRejectsInvalidInputsWithRfc7807ProblemDetail() throws Exception {
        // NaN / Infinity bbox
        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "NaN,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.detail").exists());

        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-Infinity,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));

        // Out of range maxDistanceMeters / maxMinutes / modisSampleCap / associationsLimit
        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13")
                        .param("maxDistanceMeters", "99"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));

        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13")
                        .param("maxMinutes", "721"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));

        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13")
                        .param("modisSampleCap", "50"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));

        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13")
                        .param("associationsLimit", "5001"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));

        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13")
                        .param("includeAssociations", "maybe"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));

        // Date window > 31 days and end < start
        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-10-01")
                        .param("end", "2026-11-15"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));

        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-13")
                        .param("end", "2026-11-12"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void harmonizedTimeoutReturns504WithoutPartialResults() throws Exception {
        when(harmonizationService.harmonize(any(), any(), any(), anyInt(), anyInt(), anyInt(), anyBoolean(), anyInt()))
                .thenThrow(new QueryTimeoutException("statement timeout"));

        mockMvc.perform(get("/api/v1/harmonized")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13"))
                .andExpect(status().isGatewayTimeout())
                .andExpect(jsonPath("$.status").value(504))
                .andExpect(jsonPath("$.detail").exists())
                .andExpect(jsonPath("$.result").doesNotExist())
                .andExpect(jsonPath("$.summary").doesNotExist())
                .andExpect(jsonPath("$.associations").doesNotExist());
    }

    @Test
    void existingEndpointsRemainCompatible() throws Exception {
        when(repo.points(any(), any(), any(), any(), anyInt())).thenReturn(List.of());
        when(repo.dailyTotals(any(), any(), any(), any())).thenReturn(List.of());
        when(repo.sensorStats(any(), any(), any(), any())).thenReturn(List.of());
        when(repo.sources()).thenReturn(List.of());
        when(repo.coverage()).thenReturn(List.of());

        mockMvc.perform(get("/api/v1/detections")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13")
                        .param("zoom", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.mode").value("points"));

        mockMvc.perform(get("/api/v1/calendar")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("month", "2026-11"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.month").value("2026-11"));

        mockMvc.perform(get("/api/v1/compare")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.notice").exists());

        mockMvc.perform(get("/api/v1/timeseries")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-12")
                        .param("end", "2026-11-13"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.threshold").value(3.5));

        mockMvc.perform(get("/api/v1/meta/sources")).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/meta/coverage")).andExpect(status().isOk());
    }
}
