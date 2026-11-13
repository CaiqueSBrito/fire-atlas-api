package com.fireatlas.analysis.controller;

import com.fireatlas.analysis.dto.AnalysisDto;
import com.fireatlas.analysis.service.AnalysisService;
import com.fireatlas.common.controller.HttpCache;
import com.fireatlas.common.controller.QueryParams;
import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.common.policy.ParameterError;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

/** Camada de apresentacao das analises (comparacoes e serie temporal): le a requisicao e delega ao servico. */
@RestController
@RequestMapping("/api/v1")
public class AnalysisController {

    private final AnalysisService service;
    private final int maxDaysCompare;

    public AnalysisController(AnalysisService service,
                              @Value("${fireatlas.limits.max-days-compare}") int maxDaysCompare) {
        this.service = service;
        this.maxDaysCompare = maxDaysCompare;
    }

    @Operation(summary = "Comparacao por sensor: deteccoes, FRP, confianca (sem comparacao ingenua de contagens)")
    @GetMapping("/compare")
    public ResponseEntity<AnalysisDto.CompareResponse> compare(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @RequestParam(defaultValue = "BOTH") String sensor) {
        QueryParams q = QueryParams.of(bbox, cells, start, end, sensor, maxDaysCompare);
        return HttpCache.ok(service.compare(q.region(), q.start(), q.end(), q.sensors()), q.end());
    }

    @Operation(summary = "Comparacao entre dois periodos (A e B) por sensor, com variacao percentual",
            description = "Exige cells (indices H3). Compara medias por dia; sem dados em um dos periodos, a "
                    + "variacao nao e calculada (sufficient=false).")
    @GetMapping("/compare/periods")
    public ResponseEntity<AnalysisDto.PeriodsResponse> comparePeriods(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startA,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endA,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startB,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endB,
            @RequestParam(defaultValue = "BOTH") String sensor) {
        // valida os dois periodos e a regiao de uma vez, para listar todos os parametros rejeitados
        List<ParameterError> errors = QueryParams.merge(
                QueryParams.errorsOf(() -> QueryParams.of(bbox, cells, startA, endA, sensor, maxDaysCompare,
                        "startA", "endA")),
                QueryParams.errorsOf(() -> QueryParams.of(bbox, cells, startB, endB, sensor, maxDaysCompare,
                        "startB", "endB")));
        if (!errors.isEmpty()) throw new InvalidParameterException(errors);
        QueryParams a = QueryParams.of(bbox, cells, startA, endA, sensor, maxDaysCompare, "startA", "endA");
        return HttpCache.ok(service.comparePeriods(a.region(), startA, endA, startB, endB, a.sensors()),
                endA.isAfter(endB) ? endA : endB);
    }

    @Operation(summary = "Serie temporal diaria com sinalizacao de atividade incomum (z-score robusto, mediana/MAD)",
            description = "Contagens diarias vem das celulas H3 informadas em cells (obrigatorio: a API nao "
                    + "converte bbox).")
    @GetMapping("/timeseries")
    public ResponseEntity<AnalysisDto.TimeseriesResponse> timeseries(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @RequestParam(defaultValue = "BOTH") String sensor) {
        QueryParams q = QueryParams.of(bbox, cells, start, end, sensor, 3700);
        return HttpCache.ok(service.timeseries(q.region(), q.start(), q.end(), q.sensors()), q.end());
    }

    @Operation(summary = "Comparacao do periodo com o historico (mesmas datas em anos anteriores) e anomalia",
            description = "Exige cells. years = quantos anos anteriores considerar (1-10, padrao 5). Sem pelo "
                    + "menos 3 anos com registro nao calcula media nem anomalia (sufficient=false).")
    @GetMapping("/compare/historical")
    public ResponseEntity<AnalysisDto.HistoricalResponse> compareHistorical(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @RequestParam(defaultValue = "BOTH") String sensor,
            @RequestParam(defaultValue = "5") int years) {
        List<ParameterError> errors = new java.util.ArrayList<>(QueryParams.errorsOf(
                () -> QueryParams.of(bbox, cells, start, end, sensor, maxDaysCompare)));
        if (years < 1 || years > 10) errors.add(new ParameterError("years", "years deve estar entre 1 e 10"));
        if (!errors.isEmpty()) throw new InvalidParameterException(errors);
        QueryParams q = QueryParams.of(bbox, cells, start, end, sensor, maxDaysCompare);
        return HttpCache.ok(service.compareHistorical(q.region(), q.start(), q.end(), q.sensors(), years), q.end());
    }
}
