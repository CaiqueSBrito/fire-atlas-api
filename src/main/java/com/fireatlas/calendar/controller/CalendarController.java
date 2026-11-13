package com.fireatlas.calendar.controller;

import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.calendar.dto.CalendarDto;
import com.fireatlas.calendar.service.CalendarService;
import com.fireatlas.common.controller.HttpCache;
import com.fireatlas.common.controller.QueryParams;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.YearMonth;

/** Camada de apresentacao do calendario (as "cartas" por dia). */
@RestController
@RequestMapping("/api/v1")
public class CalendarController {

    private final CalendarService service;

    public CalendarController(CalendarService service) {
        this.service = service;
    }

    @Operation(summary = "Calendario: totais por dia e sensor (alimenta as cartas)",
            description = "Os totais vem das celulas H3 informadas em cells (obrigatorio: a API nao converte bbox).")
    @GetMapping("/calendar")
    public ResponseEntity<CalendarDto.CalendarResponse> calendar(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @Parameter(description = "YYYY-MM") @RequestParam String month,
            @RequestParam(defaultValue = "BOTH") String sensor) {
        YearMonth ym;
        try {
            ym = YearMonth.parse(month);
        } catch (Exception e) {
            throw new InvalidParameterException("month", "month deve estar no formato YYYY-MM");
        }
        QueryParams q = QueryParams.of(bbox, cells, ym.atDay(1), ym.atEndOfMonth(), sensor, 31);
        return HttpCache.ok(service.calendar(q.region(), ym, q.sensors()), q.end());
    }
}
