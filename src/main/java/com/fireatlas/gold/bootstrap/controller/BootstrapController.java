package com.fireatlas.gold.bootstrap.controller;

import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.bootstrap.service.BootstrapService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.http.HttpStatus;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Apenas o POST de 2026 inicia a fila; a consulta por ano e sempre passiva. */
@RestController
@RequestMapping("/api/v2/bootstrap")
public class BootstrapController {
    private final BootstrapService service;

    public BootstrapController(BootstrapService service) { this.service = service; }

    @Operation(summary = "Estado passivo do cache mundial compartilhado de 2026")
    @GetMapping("/2026")
    public ResponseEntity<BootstrapStatus> status() {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(service.status());
    }

    @Operation(summary = "Carrega 2026 e continua pelos anos anteriores em segundo plano, do mais recente ao mais antigo")
    @PostMapping("/2026")
    public ResponseEntity<BootstrapStatus> start() {
        BootstrapStatus status = service.start();
        return ResponseEntity.status(status.state().equals("ready") ? 200 : 202)
                .cacheControl(CacheControl.noStore()).body(status);
    }
    @Operation(summary = "Progresso passivo de um ano da carga historica compartilhada")
    @GetMapping("/{year}")
    public ResponseEntity<BootstrapStatus> historicalStatus(@PathVariable int year) {
        if (year < 1 || year >= 2026) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Ano historico deve ser anterior a 2026");
        }
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(service.status(year));
    }
}
