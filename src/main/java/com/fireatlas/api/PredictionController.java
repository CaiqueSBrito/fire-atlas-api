package com.fireatlas.api;

import com.fireatlas.query.PredictionService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

@RestController
@RequestMapping("/api/v1")
public class PredictionController {

    private final PredictionService predictionService;
    private final int maxDaysPredictions;

    public PredictionController(PredictionService predictionService,
                                @Value("${fireatlas.limits.max-days-predictions:31}") int maxDaysPredictions) {
        if (maxDaysPredictions <= 0) {
            throw new IllegalArgumentException("fireatlas.limits.max-days-predictions deve ser positivo");
        }
        this.predictionService = predictionService;
        this.maxDaysPredictions = maxDaysPredictions;
    }

    @Operation(
            summary = "Previsoes de focos de calor (condicionado a modelo cientificamente validado)",
            description = "Valida bbox, start e end como janela solicitada pelo cliente (nao horizonte aprovado). "
                    + "Retorna indisponibilidade explicita (MODEL_NOT_VALIDATED) enquanto o provedor preditivo "
                    + "estiver desativado, ou delega ao provedor HTTP v1 quando houver manifesto aprovado.")
    @GetMapping("/predictions")
    public ResponseEntity<Dto.PredictionResponse> predictions(
            @RequestParam String bbox,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end) {
        QueryParams q = QueryParams.of(bbox, start, end, "BOTH", maxDaysPredictions);
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(predictionService.predict(q.bbox(), q.start(), q.end()));
    }
}
