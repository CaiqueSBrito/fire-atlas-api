package com.fireatlas.gold.bootstrap.controller;

import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.bootstrap.service.ForecastCacheService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v2/bootstrap/forecast")
public class ForecastBootstrapController {
    private final ForecastCacheService service;
    public ForecastBootstrapController(ForecastCacheService service) { this.service = service; }

    @Operation(summary = "Progresso persistente da carga nativa de previsoes pelo Athena")
    @GetMapping
    public ResponseEntity<BootstrapStatus> status() {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(service.status());
    }

    @Operation(summary = "Inicia ou retoma a carga de previsoes em segundo plano")
    @PostMapping
    public ResponseEntity<BootstrapStatus> start() {
        BootstrapStatus status = service.start();
        return ResponseEntity.status(status.state().equals("ready") ? 200 : 202)
                .cacheControl(CacheControl.noStore()).body(status);
    }
}
