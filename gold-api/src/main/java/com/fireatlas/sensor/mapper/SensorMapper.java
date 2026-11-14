package com.fireatlas.sensor.mapper;

import com.fireatlas.sensor.dto.SensorDto;
import com.fireatlas.sensor.entity.Satellite;
import com.fireatlas.sensor.entity.Sensor;
import org.springframework.stereotype.Component;

import java.util.List;

/** Converte o catalogo de sensores (sensor + satelites) no DTO de resposta. */
@Component
public class SensorMapper {

    public SensorDto.CatalogEntry toEntry(Sensor sensor, List<Satellite> satellites) {
        return new SensorDto.CatalogEntry(sensor.code(), sensor.name(), sensor.nominalResolutionMeters(),
                sensor.description(), satellites);
    }
}
