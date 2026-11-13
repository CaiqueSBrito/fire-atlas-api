package com.fireatlas.detection.controller;

import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.common.policy.ParameterError;
import com.fireatlas.common.controller.HttpCache;
import com.fireatlas.common.controller.QueryParams;
import com.fireatlas.detection.dto.DetectionDto;
import com.fireatlas.detection.service.DetectionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/** Camada de apresentacao das deteccoes: interpreta a requisicao HTTP e delega ao servico. */
@RestController
@RequestMapping("/api/v1")
public class DetectionController {

    private final DetectionService service;
    private final int maxDaysPoints;

    public DetectionController(DetectionService service,
                               @Value("${fireatlas.limits.max-days-points}") int maxDaysPoints) {
        this.service = service;
        this.maxDaysPoints = maxDaysPoints;
    }

    @Operation(summary = "Deteccoes por regiao/periodo/sensor",
            description = "zoom >= 9 devolve pontos individuais (ate o limite) e aceita bbox OU cells. Abaixo disso "
                    + "devolve celulas H3 agregadas na resolucao do zoom e EXIGE cells (indices H3 em hexadecimal): "
                    + "a API nao converte bbox em celulas.")
    @GetMapping("/detections")
    public ResponseEntity<DetectionDto.DetectionsResponse> detections(
            @Parameter(description = "minLon,minLat,maxLon,maxLat (alternativa a cells)")
            @RequestParam(required = false) String bbox,
            @Parameter(description = "indices H3 em hexadecimal, separados por virgula (alternativa a bbox)")
            @RequestParam(required = false) String cells,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @Parameter(description = "MODIS | VIIRS | BOTH") @RequestParam(defaultValue = "BOTH") String sensor,
            @RequestParam(defaultValue = "6") int zoom) {
        List<ParameterError> errors = new ArrayList<>(QueryParams.errorsOf(
                () -> QueryParams.of(bbox, cells, start, end, sensor, maxDaysPoints)));
        if (zoom < 0 || zoom > 22) errors.add(new ParameterError("zoom", "zoom deve estar entre 0 e 22"));
        if (!errors.isEmpty()) throw new InvalidParameterException(errors);
        QueryParams q = QueryParams.of(bbox, cells, start, end, sensor, maxDaysPoints);
        return HttpCache.ok(service.detections(q.region(), q.start(), q.end(), q.sensors(), zoom), q.end());
    }

    @Operation(summary = "Contagem de deteccoes por sensor numa regiao e periodo",
            description = "Aceita bbox OU cells. Contagem exata, com fonte, unidade e metodo em meta.")
    @GetMapping("/detections/count")
    public ResponseEntity<DetectionDto.CountResponse> count(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @RequestParam(defaultValue = "BOTH") String sensor) {
        QueryParams q = QueryParams.of(bbox, cells, start, end, sensor, maxDaysPoints);
        return HttpCache.ok(service.count(q.region(), q.start(), q.end(), q.sensors()), q.end());
    }
}
