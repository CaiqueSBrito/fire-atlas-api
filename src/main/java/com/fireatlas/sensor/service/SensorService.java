package com.fireatlas.sensor.service;

import com.fireatlas.sensor.dto.SensorDto;
import com.fireatlas.sensor.entity.Satellite;
import com.fireatlas.sensor.mapper.SensorMapper;
import com.fireatlas.sensor.repository.SensorRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Caso de uso do catalogo: sensores (MODIS, VIIRS) com resolucao nominal e os satelites que os carregam. */
@Service
public class SensorService {

    private final SensorRepository repository;
    private final SensorMapper mapper;

    public SensorService(SensorRepository repository, SensorMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    public List<SensorDto.CatalogEntry> sensors() {
        Map<String, List<Satellite>> bySensor = repository.satellites().stream()
                .collect(Collectors.groupingBy(Satellite::sensor));
        return repository.sensors().stream()
                .map(s -> mapper.toEntry(s, bySensor.getOrDefault(s.code(), List.of())))
                .toList();
    }
}
