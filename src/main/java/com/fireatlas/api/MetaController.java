package com.fireatlas.api;

import com.fireatlas.domain.Satellite;
import com.fireatlas.query.DetectionRepository;
import com.fireatlas.query.SensorRepository;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/meta")
public class MetaController {

    private final DetectionRepository repo;
    private final SensorRepository sensors;

    public MetaController(DetectionRepository repo, SensorRepository sensors) {
        this.repo = repo;
        this.sensors = sensors;
    }

    @Operation(summary = "Rastreabilidade: arquivos do data lake carregados, regiao e periodo")
    @GetMapping("/sources")
    public List<Dto.SourceInfo> sources() {
        return repo.sources();
    }

    @Operation(summary = "Cobertura de dados por sensor (periodo e total de deteccoes)")
    @GetMapping("/coverage")
    public List<Dto.Coverage> coverage() {
        return repo.coverage();
    }

    @Operation(summary = "Catalogo de sensores (MODIS, VIIRS), resolucao nominal e satelites")
    @GetMapping("/sensors")
    public List<Dto.SensorCatalogEntry> sensors() {
        var bySensor = sensors.satellites().stream().collect(Collectors.groupingBy(Satellite::sensor));
        return sensors.sensors().stream()
                .map(s -> new Dto.SensorCatalogEntry(s.code(), s.name(), s.nominalResolutionMeters(),
                        s.description(), bySensor.getOrDefault(s.code(), List.of())))
                .toList();
    }
}
