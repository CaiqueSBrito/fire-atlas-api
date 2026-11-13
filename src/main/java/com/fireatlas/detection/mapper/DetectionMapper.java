package com.fireatlas.detection.mapper;

import com.fireatlas.detection.dto.DetectionDto;
import com.fireatlas.detection.entity.CellAggregate;
import com.fireatlas.detection.entity.Detection;
import com.fireatlas.h3.policy.H3Index;
import org.springframework.stereotype.Component;

/** Converte entidades e agregados de deteccao nos DTOs de resposta do mapa. */
@Component
public class DetectionMapper {

    public DetectionDto.Point toPoint(Detection d) {
        return new DetectionDto.Point(d.sensor(), d.satellite(), d.instrument(), d.acquiredAt(),
                d.location().lat(), d.location().lon(), toFloat(d.frp()), d.confidenceClass(), d.dayNight(),
                toFloat(d.brightness()), d.fireType(), d.source().product(), d.source().file());
    }

    /** A celula H3 sai como veio (em hexadecimal); o centro nao e calculado: quem desenha usa o proprio H3. */
    public DetectionDto.Cell toCell(CellAggregate c, int resolution) {
        return new DetectionDto.Cell(c.sensor(), H3Index.toHex(c.cell()), resolution, c.count(), c.frpSum(),
                c.frpMax());
    }

    private static Float toFloat(Double v) {
        return v == null ? null : v.floatValue();
    }
}
