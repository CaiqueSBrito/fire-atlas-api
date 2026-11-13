package com.fireatlas.ranking.controller;

import com.fireatlas.common.controller.HttpCache;
import com.fireatlas.common.controller.QueryParams;
import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.common.policy.ParameterError;
import com.fireatlas.ranking.dto.RankingDto;
import com.fireatlas.ranking.service.RankingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

/** Camada de apresentacao dos rankings e regioes criticas. */
@RestController
@RequestMapping("/api/v1/rankings")
public class RankingController {

    private final RankingService service;
    private final int maxDays;

    public RankingController(RankingService service,
                             @Value("${fireatlas.limits.max-days-compare}") int maxDays) {
        this.service = service;
        this.maxDays = maxDays;
    }

    @Operation(summary = "Ranking de celulas H3 e regioes criticas",
            description = "Exige cells (indices H3). Ordena por contagem ou FRP e marca como criticas as celulas "
                    + "no percentil alto da contagem; o limiar so e calculado com celulas suficientes.")
    @GetMapping("/cells")
    public ResponseEntity<RankingDto.Response> cells(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @RequestParam(defaultValue = "BOTH") String sensor,
            @Parameter(description = "count (padrao) ou frp") @RequestParam(defaultValue = "count") String metric,
            @Parameter(description = "resolucao H3 das celulas do ranking (padrao 4)")
            @RequestParam(required = false) Integer resolution,
            @RequestParam(defaultValue = "20") int limit,
            @Parameter(description = "percentil (50-99) que define regiao critica; padrao 95")
            @RequestParam(defaultValue = "95") int criticalPercentile) {
        // junta os erros da regiao/periodo/sensor com os do proprio ranking (CA04)
        List<ParameterError> errors = QueryParams.merge(
                QueryParams.errorsOf(() -> QueryParams.of(bbox, cells, start, end, sensor, maxDays)),
                service.validate(metric, resolution, limit, criticalPercentile));
        if (!errors.isEmpty()) throw new InvalidParameterException(errors);
        QueryParams q = QueryParams.of(bbox, cells, start, end, sensor, maxDays);
        return HttpCache.ok(service.rank(q.region(), q.start(), q.end(), q.sensors(), metric, resolution, limit,
                criticalPercentile), q.end());
    }
}
