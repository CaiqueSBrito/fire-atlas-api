package com.fireatlas.gold.detection.controller;

import com.fireatlas.gold.detection.entity.Coverage;
import com.fireatlas.gold.detection.service.DetectionService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Cobertura dos dados carregados. */
@RestController("goldCoverageController")
@RequestMapping("/api/v2/meta")
public class CoverageController {

    private final DetectionService service;

    public CoverageController(DetectionService service) {
        this.service = service;
    }

    @Operation(summary = "Cobertura de dados por sensor (periodo e total de deteccoes)")
    @GetMapping("/coverage")
    public List<Coverage> coverage() {
        return service.coverage();
    }
}
