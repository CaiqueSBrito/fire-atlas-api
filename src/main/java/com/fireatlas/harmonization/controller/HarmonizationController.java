package com.fireatlas.harmonization.controller;

import com.fireatlas.harmonization.service.HarmonizationService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Camada de apresentacao da harmonizacao MODIS x VIIRS: responde 501 explicando por que nao ha fonte de dados. */
@RestController
@RequestMapping("/api/v1")
public class HarmonizationController {

    private final HarmonizationService service;

    public HarmonizationController(HarmonizationService service) {
        this.service = service;
    }

    @Operation(summary = "Harmonizacao MODIS x VIIRS (indisponivel com a gold: 501)")
    @GetMapping("/harmonized")
    public Object harmonized() {
        return service.unavailable();
    }

    @Operation(summary = "Correspondencias MODIS x VIIRS (indisponivel com a gold: 501)")
    @GetMapping("/harmonized/matches")
    public Object matches() {
        return service.unavailable();
    }
}
