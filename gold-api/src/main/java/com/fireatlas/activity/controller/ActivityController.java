package com.fireatlas.activity.controller;

import com.fireatlas.activity.dto.ActivityAnomalyResponse;
import com.fireatlas.activity.dto.ActivityAvailabilityResponse;
import com.fireatlas.activity.dto.ActivityCalendarResponse;
import com.fireatlas.activity.dto.ActivityMonthlyResponse;
import com.fireatlas.activity.service.ActivityService;
import com.fireatlas.common.controller.HttpCache;
import com.fireatlas.common.controller.QueryParams;
import com.fireatlas.common.policy.InvalidParameterException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.YearMonth;

/**
 * Atividade de fogo harmonizada (gold/harmonized), em "celulas ativas equivalentes ao MODIS Aqua": nao e numero de
 * focos nem de incendios. Toda resposta traz o rotulo da unidade (meta.unitLabelPt e meta.unitLabelEn) e os avisos.
 */
@RestController
@RequestMapping("/api/v1/activity")
public class ActivityController {

    private final ActivityService service;

    public ActivityController(ActivityService service) {
        this.service = service;
    }

    @Operation(summary = "Calendario da atividade harmonizada: um item por dia do mes (dia sem dado fica sem numero)",
            description = "Regiao por cells, country, continent ou city. O dia e o dia local de fogo (nao UTC).")
    @GetMapping("/calendar")
    public ResponseEntity<ActivityCalendarResponse> calendar(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam(required = false) String country,
            @RequestParam(required = false) String continent,
            @RequestParam(required = false) String city,
            @RequestParam(required = false) Double cityRadiusKm,
            @Parameter(description = "YYYY-MM") @RequestParam String month) {
        YearMonth ym = parse("month", month);
        QueryParams q = QueryParams.of(bbox, cells, country, continent, city, cityRadiusKm, ym.atDay(1),
                ym.atEndOfMonth(), "BOTH", 31);
        return HttpCache.ok(service.calendar(q.region(), ym), q.end());
    }

    @Operation(summary = "Soma mensal da atividade harmonizada na regiao",
            description = "start e end no formato YYYY-MM (ate 120 meses).")
    @GetMapping("/monthly")
    public ResponseEntity<ActivityMonthlyResponse> monthly(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam(required = false) String country,
            @RequestParam(required = false) String continent,
            @RequestParam(required = false) String city,
            @RequestParam(required = false) Double cityRadiusKm,
            @Parameter(description = "YYYY-MM") @RequestParam String start,
            @Parameter(description = "YYYY-MM") @RequestParam String end) {
        YearMonth from = parse("start", start);
        YearMonth to = parse("end", end);
        QueryParams q = QueryParams.of(bbox, cells, country, continent, city, cityRadiusKm, from.atDay(1),
                to.isBefore(from) ? from.atEndOfMonth() : to.atEndOfMonth(), "BOTH", 3700);
        return HttpCache.ok(service.monthly(q.region(), from, to), q.end());
    }

    @Operation(summary = "Anomalia mensal: o mes contra a mediana do mesmo mes nos anos anteriores",
            description = "Sem pelo menos 3 anos anteriores com dado, nao calcula mediana nem desvio (sufficient=false).")
    @GetMapping("/anomaly")
    public ResponseEntity<ActivityAnomalyResponse> anomaly(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @RequestParam(required = false) String country,
            @RequestParam(required = false) String continent,
            @RequestParam(required = false) String city,
            @RequestParam(required = false) Double cityRadiusKm,
            @Parameter(description = "YYYY-MM") @RequestParam String month,
            @RequestParam(defaultValue = "5") int years) {
        YearMonth ym = parse("month", month);
        QueryParams q = QueryParams.of(bbox, cells, country, continent, city, cityRadiusKm, ym.atDay(1),
                ym.atEndOfMonth(), "BOTH", 31);
        return HttpCache.ok(service.anomaly(q.region(), ym, years), q.end());
    }

    @Operation(summary = "Disponibilidade dos satelites por dia (terra, aqua, snpp, noaa20, noaa21)")
    @GetMapping("/availability")
    public ResponseEntity<ActivityAvailabilityResponse> availability(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @RequestParam(required = false) String platform) {
        if (end.isBefore(start)) throw new InvalidParameterException("end", "end deve ser igual ou posterior a start");
        if (java.time.temporal.ChronoUnit.DAYS.between(start, end) + 1 > 3700) {
            throw new InvalidParameterException("end", "periodo maximo para esta consulta: 3700 dias");
        }
        return HttpCache.ok(service.availability(start, end, platform), end);
    }

    private static YearMonth parse(String name, String value) {
        try {
            return YearMonth.parse(value);
        } catch (Exception e) {
            throw new InvalidParameterException(name, name + " deve estar no formato YYYY-MM");
        }
    }
}
