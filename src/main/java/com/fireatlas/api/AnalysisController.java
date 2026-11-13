package com.fireatlas.api;

import com.fireatlas.query.Anomaly;
import com.fireatlas.query.DetectionRepository;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@RestController
@RequestMapping("/api/v1")
public class AnalysisController {

    static final String COMPARE_NOTICE =
            "MODIS (~1 km) e VIIRS (375 m) tem resolucao e sensibilidade diferentes: o VIIRS detecta focos "
                    + "menores e, por isso, contagens brutas NAO sao diretamente comparaveis. Use FRP e a "
                    + "harmonizacao para comparar.";

    private final DetectionRepository repo;
    private final int maxDaysCompare;
    private final int maxDaysHarmonize;

    public AnalysisController(DetectionRepository repo,
                              @Value("${fireatlas.limits.max-days-compare}") int maxDaysCompare,
                              @Value("${fireatlas.limits.max-days-harmonize}") int maxDaysHarmonize) {
        this.repo = repo;
        this.maxDaysCompare = maxDaysCompare;
        this.maxDaysHarmonize = maxDaysHarmonize;
    }

    @Operation(summary = "Comparacao por sensor: deteccoes, FRP, confianca (sem comparacao ingenua de contagens)")
    @GetMapping("/compare")
    public ResponseEntity<Dto.CompareResponse> compare(
            @RequestParam String bbox,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @RequestParam(defaultValue = "BOTH") String sensor) {
        QueryParams q = QueryParams.of(bbox, start, end, sensor, maxDaysCompare);
        List<Dto.SensorStats> stats = repo.sensorStats(q.bbox(), q.start(), q.end(), q.sensors());
        String msg = stats.isEmpty() ? "Nenhum foco de calor encontrado para os filtros selecionados." : null;
        return DetectionController.cached(new Dto.CompareResponse(q.bbox().toString(), q.start(), q.end(),
                stats, COMPARE_NOTICE, msg), q.end());
    }

    @Operation(summary = "Serie temporal diaria com sinalizacao de atividade incomum (z-score robusto, mediana/MAD)")
    @GetMapping("/timeseries")
    public ResponseEntity<Dto.TimeseriesResponse> timeseries(
            @RequestParam String bbox,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @RequestParam(defaultValue = "BOTH") String sensor) {
        QueryParams q = QueryParams.of(bbox, start, end, sensor, 3700);
        List<DetectionRepository.DayRow> rows = repo.dailyTotals(q.bbox(), q.start(), q.end(), q.sensors());

        // Serie completa por sensor (dias sem foco = 0), para a mediana nao ficar enviesada.
        List<Dto.TimeseriesPoint> out = new ArrayList<>();
        for (String s : q.sensors()) {
            Map<LocalDate, DetectionRepository.DayRow> byDay = new TreeMap<>();
            rows.stream().filter(r -> r.sensor().equals(s)).forEach(r -> byDay.put(r.date(), r));
            List<LocalDate> dates = new ArrayList<>();
            for (LocalDate d = q.start(); !d.isAfter(q.end()); d = d.plusDays(1)) dates.add(d);
            double[] counts = dates.stream().mapToDouble(d -> byDay.containsKey(d) ? byDay.get(d).n() : 0).toArray();
            Double[] z = Anomaly.robustZ(counts);
            for (int i = 0; i < dates.size(); i++) {
                DetectionRepository.DayRow r = byDay.get(dates.get(i));
                out.add(new Dto.TimeseriesPoint(dates.get(i), s, (int) counts[i], r == null ? 0 : r.frpSum(),
                        z[i], z[i] != null && z[i] > Anomaly.THRESHOLD));
            }
        }
        boolean any = out.stream().anyMatch(p -> p.count() > 0);
        return DetectionController.cached(new Dto.TimeseriesResponse(
                "z-score robusto (mediana/MAD) sobre contagens diarias do proprio periodo; "
                        + "sem baseline sazonal (limitacao)", Anomaly.THRESHOLD, out,
                any ? null : "Nenhum foco de calor encontrado para os filtros selecionados."), q.end());
    }

    @Operation(summary = "Harmonizacao MODIS x VIIRS (pareamento espaco-temporal, metodo v0)")
    @GetMapping("/harmonized")
    public ResponseEntity<Dto.HarmonizationResponse> harmonized(
            @RequestParam String bbox,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end,
            @RequestParam(defaultValue = "750") int maxDistanceMeters,
            @RequestParam(defaultValue = "180") int maxMinutes,
            @RequestParam(defaultValue = "20000") int modisSampleCap) {
        QueryParams q = QueryParams.of(bbox, start, end, "BOTH", maxDaysHarmonize);
        if (maxDistanceMeters < 100 || maxDistanceMeters > 5000) {
            throw new IllegalArgumentException("maxDistanceMeters deve estar entre 100 e 5000");
        }
        if (maxMinutes < 1 || maxMinutes > 720) {
            throw new IllegalArgumentException("maxMinutes deve estar entre 1 e 720");
        }
        if (modisSampleCap < 100 || modisSampleCap > 100_000) {
            throw new IllegalArgumentException("modisSampleCap deve estar entre 100 e 100000");
        }
        Dto.HarmonizationResponse.Result r = repo.harmonize(q.bbox(), q.start(), q.end(),
                maxDistanceMeters, maxMinutes, modisSampleCap);
        Dto.HarmonizationResponse.Method method = new Dto.HarmonizationResponse.Method(
                "Pareamento espaco-temporal MODIS->VIIRS (v0)",
                "Para cada deteccao MODIS, conta deteccoes VIIRS a ate maxDistanceMeters e maxMinutes. "
                        + "Compara FRP do MODIS com a soma do FRP VIIRS dentro do mesmo pixel e reporta taxa de "
                        + "correspondencia, razao de FRP e correlacao. Campos originais e origem sao preservados.",
                List.of("Metodo v0: precisa de validacao com a equipe (limiares e razao de FRP).",
                        "Passagens de satelites diferentes ocorrem em horarios distintos (em 09/2026 sobre o Brasil: "
                                + "Aqua-VIIRS ~2 h, Terra-VIIRS ~4-5 h; nenhum par a menos de 30 min): "
                                + "o pareamento e de 'mesmo foco no mesmo periodo', nao de observacao simultanea.",
                        "Nuvens, angulo de varredura e area do pixel afetam a deteccao nos dois sensores.",
                        "Amostra limitada a modisSampleCap deteccoes MODIS (campo sampled)."));
        String msg = r.modisEvaluated() == 0 ? "Nenhum foco MODIS encontrado para os filtros selecionados." : null;
        return DetectionController.cached(new Dto.HarmonizationResponse(q.bbox().toString(), q.start(), q.end(),
                new Dto.HarmonizationResponse.Params(maxDistanceMeters, maxMinutes, modisSampleCap), r, method, msg),
                q.end());
    }
}
