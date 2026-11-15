package com.fireatlas.gold.bootstrap.controller;

import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.bootstrap.service.BootstrapService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Rota fixa, sem SQL, caminho S3 ou ano arbitrario controlado pelo visitante. */
@RestController
@RequestMapping("/api/v2/bootstrap/2026")
public class BootstrapController {
    private final BootstrapService service;

    public BootstrapController(BootstrapService service) { this.service = service; }

    @Operation(summary = "Estado passivo do cache mundial compartilhado de 2026")
    @GetMapping
    public ResponseEntity<BootstrapStatus> status() {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(service.status());
    }

    @Operation(summary = "Inicia ou acompanha a carga mundial; retoma arquivos confirmados e atualiza manifestos antigos")
    @PostMapping
    public ResponseEntity<BootstrapStatus> start() {
        BootstrapStatus status = service.start();
        return ResponseEntity.status(status.state().equals("ready") ? 200 : 202)
                .cacheControl(CacheControl.noStore()).body(status);
    }
}
