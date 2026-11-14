package com.fireatlas.gold.ranking.mapper;

import com.fireatlas.gold.detection.entity.RankedCell;
import com.fireatlas.gold.h3.policy.H3Index;
import com.fireatlas.gold.ranking.dto.RankingDto;
import org.springframework.stereotype.Component;

/** Converte celulas ranqueadas (entidade) no item do ranking (DTO). O H3 sai como veio, em hexadecimal. */
@Component("goldRankingMapper")
public class RankingMapper {

    public RankingDto.Item toItem(int rank, RankedCell c, int resolution, boolean critical) {
        return new RankingDto.Item(rank, H3Index.toHex(c.cell()), resolution, c.count(), c.frpSum(), c.frpMax(),
                c.modisCount(), c.viirsCount(), critical);
    }
}
