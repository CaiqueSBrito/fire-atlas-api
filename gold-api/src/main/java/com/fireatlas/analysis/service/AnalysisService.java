package com.fireatlas.analysis.service;

import com.fireatlas.analysis.dto.AnalysisDto;
import com.fireatlas.analysis.policy.Anomaly;
import com.fireatlas.analysis.policy.Historical;
import com.fireatlas.common.dto.IndicatorMeta;
import com.fireatlas.common.policy.Messages;
import com.fireatlas.detection.entity.DayTotal;
import com.fireatlas.detection.entity.PeriodTotal;
import com.fireatlas.detection.entity.SensorStats;
import com.fireatlas.detection.repository.DetectionRepository;
import com.fireatlas.h3.policy.H3Settings;
import com.fireatlas.region.entity.Region;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Casos de uso de analise: comparacao entre sensores, serie temporal e comparacao entre periodos. */
@Service
public class AnalysisService {

    static final String COMPARE_NOTICE =
            "MODIS (~1 km) e VIIRS (375 m) tem resolucao e sensibilidade diferentes: o VIIRS detecta focos "
                    + "menores e, por isso, contagens brutas NAO sao diretamente comparaveis. Use FRP e a "
                    + "harmonizacao para comparar.";

    static final String COMPARE_METHOD =
            "Estatisticas por sensor somando as celulas H3 da regiao na grade diaria da gold: frpMean = soma do FRP "
                    + "dividida pelo numero de deteccoes; frpMax = maior FRP; frpTotal = soma; confidence = contagem "
                    + "por classe l/n/h (so VIIRS); confidenceMean = media da confianca 0-100 (so MODIS), ponderada "
                    + "pelas deteccoes; daysWithData = dias com ao menos uma deteccao. Nao ha mediana de FRP.";

    static final String TIMESERIES_METHOD =
            "Contagens diarias por sensor somando as celulas H3 informadas; dia atipico = z-score robusto "
                    + "(mediana/MAD, limiar 3,5) das contagens do proprio periodo, calculado so com 7 ou mais dias "
                    + "e variacao nao nula; sem baseline sazonal.";

    static final String PERIODS_METHOD =
            "Compara o periodo A com o periodo B por sensor, somando as celulas H3 informadas. Como os periodos "
                    + "podem ter tamanhos diferentes, a variacao (B em relacao a A, em %) e calculada sobre a media "
                    + "por dia de calendario (deteccoes/dia e FRP/dia). Dia sem registro conta como zero: o sistema "
                    + "nao distingue ausencia de fogo de ausencia de dado. Sem registros em um dos periodos, a "
                    + "variacao nao e calculada.";

    static final String HISTORICAL_METHOD =
            "Compara as deteccoes por dia do periodo informado com as mesmas datas (mesmo dia/mes) dos N anos "
                    + "anteriores, somando as celulas H3 informadas. Media e desvio-padrao amostral sao calculados "
                    + "sobre os anos com ao menos um registro; z = (atual - media) / desvio; anomalo se |z| >= 2. "
                    + "Exige ao menos 3 anos com registro; senao nao calcula media nem anomalia (sufficient=false). "
                    + "Contagens de MODIS e VIIRS nao sao comparaveis entre si.";

    private final DetectionRepository repository;
    private final H3Settings h3;

    public AnalysisService(DetectionRepository repository, H3Settings h3) {
        this.repository = repository;
        this.h3 = h3;
    }

    public AnalysisDto.CompareResponse compare(Region region, LocalDate start, LocalDate end,
                                               List<String> sensors) {
        Region.Cover cover = region.coverNative(h3.baseResolution(), Region.MAX_AGG_CELLS);
        List<SensorStats> stats = repository.findSensorStats(cover, start, end, sensors);
        String message = stats.isEmpty() ? Messages.NO_DETECTIONS : null;
        return new AnalysisDto.CompareResponse(region.describe(), start, end, stats, COMPARE_NOTICE,
                IndicatorMeta.of(region.describe(), start, end, COMPARE_METHOD), message);
    }

    public AnalysisDto.TimeseriesResponse timeseries(Region region, LocalDate start, LocalDate end,
                                                     List<String> sensors) {
        Region.Cover cover = region.coverNative(h3.baseResolution(), Region.MAX_AGG_CELLS);
        List<DayTotal> rows = repository.findDailyTotals(cover, start, end, sensors);

        // Serie completa por sensor (dias sem foco = 0), para a mediana nao ficar enviesada.
        List<AnalysisDto.TimeseriesPoint> series = new ArrayList<>();
        for (String sensor : sensors) {
            Map<LocalDate, DayTotal> byDay = new TreeMap<>();
            rows.stream().filter(r -> r.sensor().equals(sensor)).forEach(r -> byDay.put(r.date(), r));
            List<LocalDate> dates = new ArrayList<>();
            for (LocalDate d = start; !d.isAfter(end); d = d.plusDays(1)) dates.add(d);
            double[] counts = dates.stream().mapToDouble(d -> byDay.containsKey(d) ? byDay.get(d).count() : 0)
                    .toArray();
            Double[] z = Anomaly.robustZ(counts);
            for (int i = 0; i < dates.size(); i++) {
                DayTotal row = byDay.get(dates.get(i));
                series.add(new AnalysisDto.TimeseriesPoint(dates.get(i), sensor, (int) counts[i],
                        row == null ? 0 : row.frpSum(), z[i], z[i] != null && z[i] > Anomaly.THRESHOLD));
            }
        }
        boolean any = series.stream().anyMatch(p -> p.count() > 0);
        return new AnalysisDto.TimeseriesResponse(
                "z-score robusto (mediana/MAD) sobre contagens diarias do proprio periodo; "
                        + "sem baseline sazonal (limitacao)", Anomaly.THRESHOLD, cover.res(), series,
                IndicatorMeta.of(region.describe(), start, end, TIMESERIES_METHOD),
                any ? null : Messages.NO_DETECTIONS);
    }

    /** Compara dois periodos (A e B) por sensor. So calcula variacoes quando ha dados nos dois (RN05). */
    public AnalysisDto.PeriodsResponse comparePeriods(Region region, LocalDate startA, LocalDate endA,
                                                      LocalDate startB, LocalDate endB, List<String> sensors) {
        Region.Cover cover = region.coverNative(h3.baseResolution(), Region.MAX_AGG_CELLS);
        Map<String, PeriodTotal> a = bySensor(repository.findPeriodTotals(cover, startA, endA, sensors));
        Map<String, PeriodTotal> b = bySensor(repository.findPeriodTotals(cover, startB, endB, sensors));
        int daysA = (int) ChronoUnit.DAYS.between(startA, endA) + 1;
        int daysB = (int) ChronoUnit.DAYS.between(startB, endB) + 1;

        List<AnalysisDto.SensorPeriodComparison> out = new ArrayList<>();
        for (String sensor : sensors) {
            PeriodTotal ta = a.get(sensor);
            PeriodTotal tb = b.get(sensor);
            var pa = periodStats(startA, endA, daysA, ta);
            var pb = periodStats(startB, endB, daysB, tb);
            boolean sufficient = ta != null && tb != null;
            Double detectionsChange = null;
            Double frpChange = null;
            String note = null;
            if (sufficient) {
                detectionsChange = changePct(pa.detectionsPerDay(), pb.detectionsPerDay());
                frpChange = changePct(pa.frpPerDay(), pb.frpPerDay());
            } else {
                note = "dados insuficientes: sem registros "
                        + (ta == null && tb == null ? "nos dois periodos" : ta == null ? "no periodo A"
                        : "no periodo B");
            }
            out.add(new AnalysisDto.SensorPeriodComparison(sensor, pa, pb, detectionsChange, frpChange,
                    sufficient, note));
        }
        boolean any = !a.isEmpty() || !b.isEmpty();
        LocalDate from = startA.isBefore(startB) ? startA : startB;
        LocalDate to = endA.isAfter(endB) ? endA : endB;
        return new AnalysisDto.PeriodsResponse(cover.res(), out,
                IndicatorMeta.of(region.describe(), from, to, PERIODS_METHOD), any ? null : Messages.NO_DETECTIONS);
    }

    private static Map<String, PeriodTotal> bySensor(List<PeriodTotal> totals) {
        Map<String, PeriodTotal> map = new LinkedHashMap<>();
        totals.forEach(t -> map.put(t.sensor(), t));
        return map;
    }

    private static AnalysisDto.PeriodStats periodStats(LocalDate start, LocalDate end, int days, PeriodTotal t) {
        if (t == null) return new AnalysisDto.PeriodStats(start, end, days, 0, 0, 0.0, null, null);
        return new AnalysisDto.PeriodStats(start, end, days, t.daysWithData(), t.count(), t.frpSum(),
                t.count() / (double) days, t.frpSum() / days);
    }

    /** Variacao percentual de a para b; nula se a for nulo ou zero (nao ha base para comparar). */
    static Double changePct(Double a, Double b) {
        if (a == null || b == null || a == 0) return null;
        return (b - a) / a * 100.0;
    }

    /** Compara o periodo com as mesmas datas de anos anteriores (media historica e anomalia), so com dados suficientes. */
    public AnalysisDto.HistoricalResponse compareHistorical(Region region, LocalDate start, LocalDate end,
                                                            List<String> sensors, int years) {
        Region.Cover cover = region.coverNative(h3.baseResolution(), Region.MAX_AGG_CELLS);
        int days = (int) ChronoUnit.DAYS.between(start, end) + 1;
        Map<String, PeriodTotal> current = bySensor(repository.findPeriodTotals(cover, start, end, sensors));
        Map<String, List<AnalysisDto.YearValue>> baseline = new LinkedHashMap<>();
        sensors.forEach(sn -> baseline.put(sn, new ArrayList<>()));
        LocalDate earliest = start;
        for (int k = 1; k <= years; k++) {
            LocalDate ys = start.minusYears(k);
            LocalDate ye = end.minusYears(k);
            earliest = ys;
            int yDays = (int) ChronoUnit.DAYS.between(ys, ye) + 1;
            for (PeriodTotal t : repository.findPeriodTotals(cover, ys, ye, sensors)) {
                baseline.get(t.sensor()).add(new AnalysisDto.YearValue(ys.getYear(), ys, ye, t.count(),
                        t.count() / (double) yDays));
            }
        }
        List<AnalysisDto.SensorHistorical> out = new ArrayList<>();
        for (String sensor : sensors) {
            PeriodTotal cur = current.get(sensor);
            List<AnalysisDto.YearValue> base = baseline.get(sensor);
            var stats = periodStats(start, end, days, cur);
            if (cur == null) {
                out.add(new AnalysisDto.SensorHistorical(sensor, stats, base, base.size(), null, null, null, null,
                        null, false, "dados insuficientes: sem registros no periodo atual"));
                continue;
            }
            var r = Historical.evaluate(base.stream().map(AnalysisDto.YearValue::detectionsPerDay).toList(),
                    stats.detectionsPerDay());
            String note = r.sufficient() ? null : "dados insuficientes: " + base.size() + " ano(s) anterior(es) com "
                    + "registro; sao necessarios " + Historical.MIN_YEARS;
            out.add(new AnalysisDto.SensorHistorical(sensor, stats, base, base.size(), r.mean(), r.stdDev(),
                    r.changeVsMeanPct(), r.zScore(), r.anomalous(), r.sufficient(), note));
        }
        boolean any = !current.isEmpty() || out.stream().anyMatch(o -> !o.baseline().isEmpty());
        return new AnalysisDto.HistoricalResponse(cover.res(), years, Historical.MIN_YEARS, Historical.Z_THRESHOLD,
                out, IndicatorMeta.of(region.describe(), earliest, end, HISTORICAL_METHOD),
                any ? null : Messages.NO_DETECTIONS);
    }
}
