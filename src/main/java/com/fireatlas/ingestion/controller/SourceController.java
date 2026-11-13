package com.fireatlas.ingestion.controller;

import com.fireatlas.ingestion.entity.ImportRecord;
import com.fireatlas.ingestion.service.ImportHistoryService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Camada de apresentacao da rastreabilidade das importacoes. */
@RestController
@RequestMapping("/api/v1/meta")
public class SourceController {

    private final ImportHistoryService service;

    public SourceController(ImportHistoryService service) {
        this.service = service;
    }

    @Operation(summary = "Rastreabilidade: importacoes do data lake (concluidas e falhas), regiao e periodo")
    @GetMapping("/sources")
    public List<ImportRecord> sources() {
        return service.imports();
    }
}
