package com.fireatlas.ranking.service;

import com.fireatlas.common.dto.IndicatorMeta;
import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.common.policy.Messages;
import com.fireatlas.common.policy.ParameterError;
import com.fireatlas.detection.entity.CellDistribution;
import com.fireatlas.detection.entity.RankedCell;
import com.fireatlas.detection.repository.DetectionRepository;
import com.fireatlas.h3.policy.H3Settings;
import com.fireatlas.ranking.dto.RankingDto;
import com.fireatlas.ranking.mapper.RankingMapper;
import com.fireatlas.region.entity.Region;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/** Caso de uso dos rankings: celulas H3 mais ativas e regioes criticas (percentil alto da contagem por celula). */
@Service
public class RankingService {

    /** Menos celulas que isso nao permitem definir um limiar de regiao critica com sentido (RN05). */
    public static final int MIN_CELLS_FOR_CRITICAL = 20;
    public static final int MAX_LIMIT = 200;

    private final DetectionRepository repository;
    private final RankingMapper mapper;
    private final H3Settings h3;

    public RankingService(DetectionRepository repository, RankingMapper mapper, H3Settings h3) {
        this.repository = repository;
        this.mapper = mapper;
        this.h3 = h3;
    }

    /** Resolucao padrao do ranking: 4 (celulas de ~22 km de aresta), limitada pela resolucao-base. */
    public int defaultResolution() {
        return Math.min(4, h3.baseResolution());
    }

    /** Valida os parametros proprios do ranking e devolve TODOS os rejeitados (lista vazia se tudo certo). */
    public List<ParameterError> validate(String metric, Integer resolution, int limit, int criticalPercentile) {
        List<ParameterError> errors = new ArrayList<>();
        if (!"count".equals(metric) && !"frp".equals(metric)) {
            errors.add(new ParameterError("metric", "metric deve ser count ou frp"));
        }
        if (resolution != null && (resolution < 0 || resolution > h3.baseResolution())) {
            errors.add(new ParameterError("resolution",
                    "resolution deve estar entre 0 e " + h3.baseResolution()));
        }
        if (limit < 1 || limit > MAX_LIMIT) {
            errors.add(new ParameterError("limit", "limit deve estar entre 1 e " + MAX_LIMIT));
        }
        if (criticalPercentile < 50 || criticalPercentile > 99) {
            errors.add(new ParameterError("criticalPercentile", "criticalPercentile deve estar entre 50 e 99"));
        }
        return errors;
    }

    public RankingDto.Response rank(Region region, LocalDate start, LocalDate end, List<String> sensors,
                                    String metric, Integer resolution, int limit, int criticalPercentile) {
        List<ParameterError> errors = validate(metric, resolution, limit, criticalPercentile);
        if (!errors.isEmpty()) throw new InvalidParameterException(errors);

        boolean byFrp = "frp".equals(metric);
        int requested = resolution == null ? defaultResolution() : resolution;
        Region.Cover cover = region.coverFitting(requested, Region.MAX_MAP_CELLS);
        List<RankedCell> ranked = repository.findRanking(cover, start, end, sensors, byFrp, limit);
        CellDistribution distribution = repository.findCellDistribution(cover, start, end, sensors,
                criticalPercentile / 100.0);

        boolean enough = distribution.cells() >= MIN_CELLS_FOR_CRITICAL && distribution.percentileValue() != null;
        Double threshold = enough ? distribution.percentileValue() : null;
        List<RankingDto.Item> items = new ArrayList<>();
        for (int i = 0; i < ranked.size(); i++) {
            RankedCell c = ranked.get(i);
            items.add(mapper.toItem(i + 1, c, cover.res(), enough && c.count() > threshold));
        }

        String message = null;
        if (items.isEmpty()) {
            message = Messages.NO_DETECTIONS;
        } else if (!enough) {
            message = "poucas celulas com registro (" + distribution.cells() + "; minimo " + MIN_CELLS_FOR_CRITICAL
                    + ") para definir regioes criticas: o limiar nao e calculado e nenhuma celula e marcada";
        }
        String method = "Ranking das celulas H3 de resolucao " + cover.res() + " por "
                + (byFrp ? "soma de FRP (MW)" : "contagem de deteccoes") + ", somando os sensores selecionados. "
                + "Regiao critica = celula com contagem de deteccoes ESTRITAMENTE maior que o percentil " + criticalPercentile
                + " (interpolado) das contagens de todas as celulas da regiao no periodo; so e calculada com pelo "
                + "menos " + MIN_CELLS_FOR_CRITICAL + " celulas com registro.";
        return new RankingDto.Response(metric, cover.res(), distribution.cells(), criticalPercentile, threshold,
                items, IndicatorMeta.of(region.describe(), start, end, method), message);
    }
}
