package com.fireatlas.detection.mapper;

import com.fireatlas.detection.dto.DetectionDto;
import com.fireatlas.detection.entity.CellAggregate;
import com.fireatlas.h3.policy.H3Index;
import org.springframework.stereotype.Component;

/** Converte agregados da grade H3 nos DTOs de resposta do mapa. */
@Component
public class DetectionMapper {

    /** A celula H3 sai como veio (em hexadecimal); o centro nao e calculado: quem desenha usa o proprio H3. */
    public DetectionDto.Cell toCell(CellAggregate c, int resolution) {
        return new DetectionDto.Cell(c.sensor(), H3Index.toHex(c.cell()), resolution, c.count(), c.frpSum(),
                c.frpMax());
    }
}
