package com.fireatlas.gold.sensor.mapper;

import com.fireatlas.gold.sensor.dto.SensorDto;
import com.fireatlas.gold.sensor.entity.Satellite;
import com.fireatlas.gold.sensor.entity.Sensor;
import org.springframework.stereotype.Component;

import java.util.List;

/** Converte o catalogo de sensores (sensor + satelites) no DTO de resposta. */
@Component("goldSensorMapper")
public class SensorMapper {

    public SensorDto.CatalogEntry toEntry(Sensor sensor, List<Satellite> satellites) {
        return new SensorDto.CatalogEntry(sensor.code(), sensor.name(), sensor.nominalResolutionMeters(),
                sensor.description(), satellites);
    }
}
