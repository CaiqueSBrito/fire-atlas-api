package com.fireatlas.harmonization.mapper;

import com.fireatlas.harmonization.dto.HarmonizationDto;
import com.fireatlas.harmonization.entity.MatchedPair;
import org.springframework.stereotype.Component;

/** Converte pares da harmonizacao no DTO de resposta, mantendo a origem de cada lado (RN02). */
@Component
public class HarmonizationMapper {

    public HarmonizationDto.Match toMatch(MatchedPair p) {
        return new HarmonizationDto.Match(
                new HarmonizationDto.Side("MODIS", null, p.modisLat(), p.modisLon(), p.modisAcquiredAt(),
                        p.modisFrp(), p.modisSourceFile()),
                new HarmonizationDto.Side("VIIRS", p.viirsSatellite(), p.viirsLat(), p.viirsLon(),
                        p.viirsAcquiredAt(), p.viirsFrp(), p.viirsSourceFile()),
                Math.round(p.distanceMeters() * 10) / 10.0, Math.round(p.minutesApart() * 10) / 10.0);
    }
}
