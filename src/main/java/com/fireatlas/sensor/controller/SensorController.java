package com.fireatlas.sensor.controller;

import com.fireatlas.sensor.dto.SensorDto;
import com.fireatlas.sensor.service.SensorService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Camada de apresentacao do catalogo de sensores. */
@RestController
@RequestMapping("/api/v1/meta")
public class SensorController {

    private final SensorService service;

    public SensorController(SensorService service) {
        this.service = service;
    }

    @Operation(summary = "Catalogo de sensores (MODIS, VIIRS), resolucao nominal e satelites")
    @GetMapping("/sensors")
    public List<SensorDto.CatalogEntry> sensors() {
        return service.sensors();
    }
}
