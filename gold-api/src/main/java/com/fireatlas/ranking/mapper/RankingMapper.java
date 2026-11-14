package com.fireatlas.ranking.mapper;

import com.fireatlas.detection.entity.RankedCell;
import com.fireatlas.h3.policy.H3Index;
import com.fireatlas.ranking.dto.RankingDto;
import org.springframework.stereotype.Component;

/** Converte celulas ranqueadas (entidade) no item do ranking (DTO). O H3 sai como veio, em hexadecimal. */
@Component
public class RankingMapper {

    public RankingDto.Item toItem(int rank, RankedCell c, int resolution, boolean critical) {
        return new RankingDto.Item(rank, H3Index.toHex(c.cell()), resolution, c.count(), c.frpSum(), c.frpMax(),
                c.modisCount(), c.viirsCount(), critical);
    }
}
