package com.fireatlas.forecast.service;

import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.common.policy.ParameterError;
import com.fireatlas.forecast.dto.ForecastItem;
import com.fireatlas.forecast.dto.ForecastResponse;
import com.fireatlas.forecast.dto.IssueItem;
import com.fireatlas.forecast.dto.IssueList;
import com.fireatlas.forecast.entity.CellForecast;
import com.fireatlas.forecast.entity.ForecastIssue;
import com.fireatlas.forecast.mapper.ForecastMapper;
import com.fireatlas.forecast.repository.ForecastRepository;
import com.fireatlas.region.entity.Region;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Casos de uso da previsao de fogo (gold/forecast). O valor previsto vem pronto do pipeline do time de dados: a API
 * so filtra, ordena e informa a emissao usada. Sem previsao para o pedido, responde vazio com a explicacao.
 */
@Service
public class ForecastService {

    public static final int MAX_LIMIT = 5000;
    public static final int RES = 6;

    static final String SOURCE = "gold/forecast (time de dados): previsao por celula H3 de resolucao 6";
    static final String METHOD = "probability7d (prob_fire_7d) e a probabilidade de fogo na celula nos proximos 7 "
            + "dias, calculada pelo modelo do pipeline de dados a partir da prata na data de emissao; os demais "
            + "campos sao as entradas resumidas (deteccoes em 24 h, 3 e 7 dias, FRP medio, brilho e vizinhanca). "
            + "A API nao recalcula nem ajusta a previsao.";

    private final ForecastRepository repository;
    private final ForecastMapper mapper;

    public ForecastService(ForecastRepository repository, ForecastMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    public IssueList issues() {
        List<IssueItem> items = repository.findIssues().stream().map(mapper::toItem).toList();
        return new IssueList(items.size(), items, SOURCE,
                items.isEmpty() ? "Nenhuma previsao importada (rode a importacao da forecast)." : null);
    }

    /** issueDate nulo = a emissao mais recente. */
    public ForecastResponse forecast(Region region, LocalDate issueDate, double minProbability, boolean onlyActive,
                                     int limit) {
        List<ParameterError> errors = new ArrayList<>();
        if (minProbability < 0 || minProbability > 1) {
            errors.add(new ParameterError("minProbability", "minProbability deve estar entre 0 e 1"));
        }
        if (limit < 1 || limit > MAX_LIMIT) {
            errors.add(new ParameterError("limit", "limit deve estar entre 1 e " + MAX_LIMIT));
        }
        if (!errors.isEmpty()) throw new InvalidParameterException(errors);

        Region.Cover cover = region.coverFitting(RES, Region.MAX_MAP_CELLS);
        List<ForecastIssue> issues = repository.findIssues();
        if (issues.isEmpty()) {
            return empty(null, null, region, "Nenhuma previsao importada (rode a importacao da forecast).");
        }
        LocalDate latest = issues.get(0).issueDate();
        LocalDate used = issueDate == null ? latest : issueDate;
        if (issues.stream().noneMatch(i -> i.issueDate().equals(used))) {
            return empty(used, latest, region, "Nao ha previsao para a emissao " + used + "; a mais recente e "
                    + latest + " (veja /api/v1/forecast/issues).");
        }
        List<CellForecast> found = repository.findForecast(cover, used, minProbability, onlyActive, limit);
        boolean truncated = found.size() > limit;
        List<ForecastItem> items = found.stream().limit(limit).map(mapper::toItem).toList();
        return new ForecastResponse(used, latest, region.describe(), items.size(), truncated, items, SOURCE, METHOD,
                items.isEmpty() ? "Nenhuma celula da regiao atende aos filtros nesta emissao." : null);
    }

    private ForecastResponse empty(LocalDate used, LocalDate latest, Region region, String message) {
        return new ForecastResponse(used, latest, region.describe(), 0, false, List.of(), SOURCE, METHOD, message);
    }
}
