package com.fireatlas.forecast.controller;

import com.fireatlas.common.controller.HttpCache;
import com.fireatlas.forecast.dto.ForecastResponse;
import com.fireatlas.forecast.dto.IssueList;
import com.fireatlas.forecast.service.ForecastService;
import com.fireatlas.region.entity.Region;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

/** Previsao de fogo por celula H3 (gold/forecast): lista as celulas da regiao da maior para a menor probabilidade. */
@RestController
@RequestMapping("/api/v1/forecast")
public class ForecastController {

    private final ForecastService service;

    public ForecastController(ForecastService service) {
        this.service = service;
    }

    @Operation(summary = "Previsao de fogo por celula H3 (resolucao 6) na regiao",
            description = "Regiao por cells, country, continent ou city. Sem issueDate usa a emissao mais recente.")
    @GetMapping
    public ResponseEntity<ForecastResponse> forecast(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam(required = false) String country,
            @RequestParam(required = false) String continent,
            @RequestParam(required = false) String city,
            @RequestParam(required = false) Double cityRadiusKm,
            @Parameter(description = "data de emissao (YYYY-MM-DD); padrao: a mais recente")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate issueDate,
            @RequestParam(defaultValue = "0") double minProbability,
            @RequestParam(defaultValue = "false") boolean onlyActive,
            @RequestParam(defaultValue = "500") int limit) {
        Region region = Region.of(bbox, cells, country, continent, city, cityRadiusKm);
        // previsao muda poucas vezes por dia: cache curto
        return HttpCache.ok(service.forecast(region, issueDate, minProbability, onlyActive, limit), LocalDate.now());
    }

    @Operation(summary = "Datas de emissao da previsao disponiveis")
    @GetMapping("/issues")
    public ResponseEntity<IssueList> issues() {
        return HttpCache.ok(service.issues(), LocalDate.now());
    }
}
