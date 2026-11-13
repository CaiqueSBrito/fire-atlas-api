package com.fireatlas.api;

import com.fireatlas.domain.H3Index;
import com.fireatlas.domain.Region;
import com.fireatlas.query.DetectionRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@RestController
@RequestMapping("/api/v1")
public class DetectionController {

    /** Maximo de celulas H3 na cobertura de uma consulta de mapa (limita tamanho da consulta). */
    static final int MAX_MAP_CELLS = 20_000;
    /** Maximo de celulas H3 na cobertura de calendario/serie (a regiao e aproximada pelas celulas). */
    static final int MAX_AGG_CELLS = 5_000;
    private static final String EMPTY = "Nenhum foco de calor encontrado para os filtros selecionados.";

    private final DetectionRepository repo;
    private final int maxDaysPoints;
    private final int maxPoints;

    public DetectionController(DetectionRepository repo,
                               @Value("${fireatlas.limits.max-days-points}") int maxDaysPoints,
                               @Value("${fireatlas.limits.max-points}") int maxPoints) {
        this.repo = repo;
        this.maxDaysPoints = maxDaysPoints;
        this.maxPoints = maxPoints;
    }

    @Operation(summary = "Deteccoes por regiao/periodo/sensor",
            description = "Regiao: bbox OU cells (indices H3 em hexadecimal). zoom >= 9 devolve pontos individuais "
                    + "(ate o limite); abaixo disso devolve celulas H3 agregadas na resolucao do zoom.")
    @GetMapping("/detections")
    public ResponseEntity<Dto.DetectionsResponse> detections(
            @Parameter(description = "minLon,minLat,maxLon,maxLat (alternativa a cells)")
            @RequestParam(required = false) String bbox,
            @Parameter(description = "indices H3 em hexadecimal, separados por virgula (alternativa a bbox)")
            @RequestParam(required = false) String cells,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @Parameter(description = "MODIS | VIIRS | BOTH") @RequestParam(defaultValue = "BOTH") String sensor,
            @RequestParam(defaultValue = "6") int zoom) {

        QueryParams q = QueryParams.of(bbox, cells, start, end, sensor, maxDaysPoints);
        Dto.DetectionsResponse body;
        if (zoom >= H3Index.POINTS_MIN_ZOOM) {
            List<Dto.Point> pts = repo.points(q.region(), q.start(), q.end(), q.sensors(), maxPoints);
            boolean truncated = pts.size() > maxPoints;
            pts = DetectionRepository.trim(pts, maxPoints);
            body = new Dto.DetectionsResponse("points", pts.size(), truncated, null, pts, null,
                    pts.isEmpty() ? EMPTY : null);
        } else {
            Region.Cover cover = q.region().coverFitting(H3Index.resolutionForZoom(zoom), MAX_MAP_CELLS);
            List<Dto.Cell> list = repo.cells(cover, q.start(), q.end(), q.sensors(), MAX_MAP_CELLS);
            boolean truncated = list.size() > MAX_MAP_CELLS;
            list = DetectionRepository.trim(list, MAX_MAP_CELLS);
            int total = list.stream().mapToInt(Dto.Cell::count).sum();
            body = new Dto.DetectionsResponse("grid", total, truncated, cover.res(), null, list,
                    list.isEmpty() ? EMPTY : null);
        }
        return cached(body, q.end());
    }

    @Operation(summary = "Calendario: totais por dia e sensor (alimenta as cartas)",
            description = "Os totais vem das celulas H3 que cobrem a regiao (bbox ou cells).")
    @GetMapping("/calendar")
    public ResponseEntity<Dto.CalendarResponse> calendar(
            @RequestParam(required = false) String bbox,
            @RequestParam(required = false) String cells,
            @Parameter(description = "YYYY-MM") @RequestParam String month,
            @RequestParam(defaultValue = "BOTH") String sensor) {

        YearMonth ym;
        try {
            ym = YearMonth.parse(month);
        } catch (Exception e) {
            throw new IllegalArgumentException("month deve estar no formato YYYY-MM");
        }
        QueryParams q = QueryParams.of(bbox, cells, ym.atDay(1), ym.atEndOfMonth(), sensor, 31);
        Region.Cover cover = q.region().coverFitting(H3Index.BASE_RES, MAX_AGG_CELLS);
        Map<LocalDate, int[]> counts = new TreeMap<>();
        Map<LocalDate, double[]> frp = new TreeMap<>();
        for (DetectionRepository.DayRow r : repo.dailyTotals(cover, q.start(), q.end(), q.sensors())) {
            int idx = "MODIS".equals(r.sensor()) ? 0 : 1;
            counts.computeIfAbsent(r.date(), d -> new int[2])[idx] = r.n();
            frp.computeIfAbsent(r.date(), d -> new double[2])[idx] = r.frpSum();
        }
        List<Dto.CalendarDay> days = new ArrayList<>();
        for (LocalDate d = q.start(); !d.isAfter(q.end()); d = d.plusDays(1)) {
            int[] c = counts.getOrDefault(d, new int[2]);
            double[] f = frp.getOrDefault(d, new double[2]);
            days.add(new Dto.CalendarDay(d, new Dto.SensorDay(c[0], f[0]), new Dto.SensorDay(c[1], f[1])));
        }
        int withFire = counts.size();
        return cached(new Dto.CalendarResponse(month, cover.res(), withFire, days,
                withFire == 0 ? EMPTY : null), q.end());
    }

    /** Dados historicos mudam pouco: cache curto no cliente/CDN; dados recentes, ainda menor. */
    static <T> ResponseEntity<T> cached(T body, LocalDate periodEnd) {
        long ageDays = ChronoUnit.DAYS.between(periodEnd, LocalDate.now(ZoneOffset.UTC));
        long seconds = ageDays > 7 ? 600 : 60;
        return ResponseEntity.ok().cacheControl(CacheControl.maxAge(java.time.Duration.ofSeconds(seconds)).cachePublic())
                .body(body);
    }
}
