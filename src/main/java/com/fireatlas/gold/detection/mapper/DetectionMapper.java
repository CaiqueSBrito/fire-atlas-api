package com.fireatlas.gold.detection.mapper;

import com.fireatlas.gold.detection.dto.DetectionDto;
import com.fireatlas.gold.detection.entity.CellAggregate;
import com.fireatlas.gold.h3.policy.H3Index;
import org.springframework.stereotype.Component;

/** Converte agregados da grade H3 nos DTOs de resposta do mapa. */
@Component("goldDetectionMapper")
public class DetectionMapper {

    /** A celula H3 sai como veio (em hexadecimal); o centro nao e calculado: quem desenha usa o proprio H3. */
    public DetectionDto.Cell toCell(CellAggregate c, int resolution) {
        return new DetectionDto.Cell(c.sensor(), H3Index.toHex(c.cell()), resolution, c.count(), c.frpSum(),
                c.frpMax());
    }
}
