package com.fireatlas.api;

import com.fireatlas.query.DetectionRepository;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/meta")
public class MetaController {

    private final DetectionRepository repo;

    public MetaController(DetectionRepository repo) {
        this.repo = repo;
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
}
