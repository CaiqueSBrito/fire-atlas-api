package com.fireatlas.harmonization.service;

import com.fireatlas.common.policy.UnavailableException;
import org.springframework.stereotype.Service;

/**
 * Harmonizacao MODIS x VIIRS (pareamento espaco-temporal): indisponivel com a gold. O pareamento exige o foco
 * individual (posicao e hora de cada deteccao), e a gold (map_grid) traz so agregados por celula H3, dia e sensor.
 * O codigo do metodo v0 esta no historico do Git (commit 1689dc3) e pode voltar se a gold passar a ter focos.
 */
@Service
public class HarmonizationService {

    static final String UNAVAILABLE =
            "Harmonizacao e correspondencias MODIS x VIIRS indisponiveis: dependem de deteccoes individuais "
                    + "(posicao e hora), e a camada ouro so traz agregados por celula H3, dia e sensor. "
                    + "Para comparar os sensores use /compare (FRP, confianca e contagens, sem comparar contagens "
                    + "brutas entre sensores).";

    public <T> T unavailable() {
        throw new UnavailableException(UNAVAILABLE);
    }
}
