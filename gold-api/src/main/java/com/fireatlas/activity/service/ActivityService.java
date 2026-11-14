package com.fireatlas.activity.service;

import com.fireatlas.activity.dto.ActivityAnomalyResponse;
import com.fireatlas.activity.dto.ActivityAvailabilityResponse;
import com.fireatlas.activity.dto.ActivityBaselineYear;
import com.fireatlas.activity.dto.ActivityCalendarResponse;
import com.fireatlas.activity.dto.ActivityDayItem;
import com.fireatlas.activity.dto.ActivityMeta;
import com.fireatlas.activity.dto.ActivityMonthItem;
import com.fireatlas.activity.dto.ActivityMonthlyResponse;
import com.fireatlas.activity.dto.SatelliteDayItem;
import com.fireatlas.activity.entity.MonthActivity;
import com.fireatlas.activity.entity.YearMonthSum;
import com.fireatlas.activity.mapper.ActivityMapper;
import com.fireatlas.activity.policy.ActivityLabels;
import com.fireatlas.activity.policy.AreaEstimate;
import com.fireatlas.activity.policy.BaselineResult;
import com.fireatlas.activity.policy.MonthBaseline;
import com.fireatlas.activity.repository.ActivityRepository;
import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.h3.policy.H3Settings;
import com.fireatlas.region.entity.Region;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

/**
 * Casos de uso da atividade de fogo harmonizada (gold/harmonized): calendario, somas mensais, anomalia contra a
 * mediana do mesmo mes em anos anteriores e disponibilidade dos satelites. O numero vem pronto do pipeline de dados;
 * a API so filtra a regiao, soma por mes e compara. Toda resposta traz o rotulo da unidade e os avisos de uso.
 */
@Service
public class ActivityService {

    public static final Set<String> PLATFORMS = Set.of("terra", "aqua", "snpp", "noaa20", "noaa21");

    static final String METHOD = "Soma de harmonized (celulas ativas equivalentes ao MODIS Aqua) nas celulas da regiao, "
            + "na resolucao das proprias celulas (os niveis 0 a 5 ja trazem a soma dos filhos); dia = dia local de "
            + "fogo. Mes = soma dos dias com dado. Anomalia = mes consultado contra a mediana do mesmo mes nos anos "
            + "anteriores com dado (minimo de " + MonthBaseline.MIN_YEARS + " anos).";

    private final ActivityRepository repository;
    private final ActivityMapper mapper;
    private final H3Settings h3;

    public ActivityService(ActivityRepository repository, ActivityMapper mapper, H3Settings h3) {
        this.repository = repository;
        this.mapper = mapper;
        this.h3 = h3;
    }

    public ActivityCalendarResponse calendar(Region region, YearMonth month) {
        Region.Cover cover = region.coverNative(h3.baseResolution(), Region.MAX_AGG_CELLS);
        List<ActivityDayItem> days = mapper.toDays(month, repository.findDaily(cover, month.atDay(1),
                month.atEndOfMonth()));
        int withData = (int) days.stream().filter(ActivityDayItem::hasData).count();
        return new ActivityCalendarResponse(month.toString(), withData, days,
                meta(region, cover, month.atDay(1), month.atEndOfMonth()), withData == 0 ? ActivityLabels.NO_DATA : null);
    }

    public ActivityMonthlyResponse monthly(Region region, YearMonth start, YearMonth end) {
        if (end.isBefore(start)) throw new InvalidParameterException("end", "end deve ser igual ou posterior a start");
        if (java.time.temporal.ChronoUnit.MONTHS.between(start, end) + 1 > 120) {
            throw new InvalidParameterException("end", "periodo maximo para esta consulta: 120 meses");
        }
        Region.Cover cover = region.coverNative(h3.baseResolution(), Region.MAX_AGG_CELLS);
        List<ActivityMonthItem> months = repository.findMonthly(cover, start.atDay(1), end.atEndOfMonth()).stream()
                .map(mapper::toItem).toList();
        boolean any = months.stream().anyMatch(m -> m.daysWithData() > 0);
        return new ActivityMonthlyResponse(months, meta(region, cover, start.atDay(1), end.atEndOfMonth()),
                any ? null : ActivityLabels.NO_DATA);
    }

    /** Mes consultado contra a mediana do mesmo mes nos anos anteriores com dado; sem anos suficientes nao calcula. */
    public ActivityAnomalyResponse anomaly(Region region, YearMonth month, int years) {
        if (years < 1 || years > 10) throw new InvalidParameterException("years", "years deve estar entre 1 e 10");
        Region.Cover cover = region.coverNative(h3.baseResolution(), Region.MAX_AGG_CELLS);
        int fromYear = month.getYear() - years;
        MonthActivity current = repository.findMonthly(cover, month.atDay(1), month.atEndOfMonth()).stream()
                .findFirst().orElse(null);
        boolean hasCurrent = current != null && current.daysWithData() > 0;
        List<YearMonthSum> previous = repository.findSameMonthByYear(cover, month.getMonthValue(), fromYear,
                month.getYear() - 1).stream().filter(s -> s.daysWithData() > 0).toList();
        List<ActivityBaselineYear> baseline = previous.stream().map(mapper::toBaseline).toList();

        BaselineResult r = hasCurrent
                ? MonthBaseline.evaluate(previous.stream().map(YearMonthSum::harmonized).toList(),
                        current.harmonized())
                : new BaselineResult(false, null, null, null);
        String note = null;
        if (!hasCurrent) {
            note = "dados insuficientes: sem dado no mes consultado";
        } else if (!r.sufficient()) {
            note = "dados insuficientes: " + previous.size() + " ano(s) anterior(es) com dado; sao necessarios "
                    + MonthBaseline.MIN_YEARS;
        } else if (r.median() == 0) {
            note = "a mediana do mesmo mes nos anos anteriores e zero: razao e desvio nao calculados";
        }
        boolean complete = month.atEndOfMonth().isBefore(LocalDate.now(ZoneOffset.UTC));
        return new ActivityAnomalyResponse(month.toString(), hasCurrent ? current.harmonized() : null,
                hasCurrent ? current.daysWithData() : 0, complete, baseline, previous.size(),
                MonthBaseline.MIN_YEARS, r.median(), r.ratioToMedian(), r.deviationPct(), r.sufficient(), note,
                meta(region, cover, LocalDate.of(fromYear, month.getMonthValue(), 1), month.atEndOfMonth()));
    }

    public ActivityAvailabilityResponse availability(LocalDate start, LocalDate end, String platform) {
        String p = platform == null || platform.isBlank() ? null : platform.trim().toLowerCase();
        if (p != null && !PLATFORMS.contains(p)) {
            throw new InvalidParameterException("platform", "platform deve ser um entre: terra, aqua, snpp, noaa20, "
                    + "noaa21");
        }
        List<SatelliteDayItem> days = repository.findAvailability(start, end, p).stream().map(mapper::toItem)
                .toList();
        return new ActivityAvailabilityResponse(start, end, days.size(), days, ActivityLabels.SOURCE,
                days.isEmpty() ? "Sem registro de disponibilidade dos satelites no periodo." : null);
    }

    private ActivityMeta meta(Region region, Region.Cover cover, LocalDate start, LocalDate end) {
        double area = AreaEstimate.estimateKm2(cover.res(), repository.countRegionCells(cover));
        return new ActivityMeta(region.describe(), start, end, cover.res(), ActivityLabels.UNIT_PT,
                ActivityLabels.UNIT_EN, ActivityLabels.SOURCE, METHOD, Math.round(area * 10) / 10.0,
                area > 0 && area < ActivityLabels.SMALL_AREA_KM2,
                ActivityLabels.warnings(start, end, LocalDate.now(ZoneOffset.UTC), area));
    }
}
