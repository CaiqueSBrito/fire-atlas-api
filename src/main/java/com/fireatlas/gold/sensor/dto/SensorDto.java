package com.fireatlas.gold.sensor.dto;

import com.fireatlas.gold.sensor.entity.Satellite;

import java.util.List;

/** Contratos de resposta do catalogo de sensores. */
public final class SensorDto {

    private SensorDto() {}

    /** Entrada do catalogo: sensor, resolucao nominal e satelites que o carregam. */
    public record CatalogEntry(String code, String name, int nominalResolutionMeters, String description,
                               List<Satellite> satellites) {}
}
