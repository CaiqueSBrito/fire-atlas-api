package com.fireatlas.harmonization.controller;

import com.fireatlas.common.controller.HttpCache;
import com.fireatlas.common.controller.QueryParams;
import com.fireatlas.harmonization.dto.HarmonizationDto;
import com.fireatlas.harmonization.service.HarmonizationService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

/** Camada de apresentacao da harmonizacao MODIS x VIIRS. */
@RestController
@RequestMapping("/api/v1")
public class HarmonizationController {

    private final HarmonizationService service;
    private final int maxDaysHarmonize;

    public HarmonizationController(HarmonizationService service,
                                   @Value("${fireatlas.limits.max-days-harmonize}") int maxDaysHarmonize) {
        this.service = service;
        this.maxDaysHarmonize = maxDaysHarmonize;
    }

    @Operation(summary = "Harmonizacao MODIS x VIIRS (pareamento espaco-temporal, metodo v0)")
    @GetMapping("/harmonized")
    public ResponseEntity<HarmonizationDto.Response> harmonized(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @RequestParam(defaultValue = "750") int maxDistanceMeters,
            @RequestParam(defaultValue = "180") int maxMinutes,
            @RequestParam(defaultValue = "20000") int modisSampleCap) {
        QueryParams q = QueryParams.of(bbox, cells, start, end, "BOTH", maxDaysHarmonize);
        return HttpCache.ok(service.harmonize(q.region(), q.start(), q.end(), maxDistanceMeters, maxMinutes,
                modisSampleCap), q.end());
    }

    @Operation(summary = "Correspondencias MODIS x VIIRS (camada de pares), do par mais proximo ao mais distante")
    @GetMapping("/harmonized/matches")
    public ResponseEntity<HarmonizationDto.MatchesResponse> matches(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @RequestParam(defaultValue = "750") int maxDistanceMeters,
            @RequestParam(defaultValue = "180") int maxMinutes,
            @RequestParam(defaultValue = "20000") int modisSampleCap,
            @RequestParam(defaultValue = "500") int limit) {
        QueryParams q = QueryParams.of(bbox, cells, start, end, "BOTH", maxDaysHarmonize);
        return HttpCache.ok(service.matches(q.region(), q.start(), q.end(), maxDistanceMeters, maxMinutes,
                modisSampleCap, limit), q.end());
    }
}
