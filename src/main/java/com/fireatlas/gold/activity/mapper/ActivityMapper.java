package com.fireatlas.gold.activity.mapper;

import com.fireatlas.gold.activity.dto.ActivityBaselineYear;
import com.fireatlas.gold.activity.dto.ActivityDayItem;
import com.fireatlas.gold.activity.dto.ActivityMonthItem;
import com.fireatlas.gold.activity.dto.SatelliteDayItem;
import com.fireatlas.gold.activity.dto.TemperatureBin;
import com.fireatlas.gold.activity.entity.DayActivity;
import com.fireatlas.gold.activity.entity.MonthActivity;
import com.fireatlas.gold.activity.entity.SatelliteDay;
import com.fireatlas.gold.activity.entity.YearMonthSum;
import com.fireatlas.gold.activity.policy.TemperatureBins;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converte as entidades de atividade harmonizada nos DTOs de resposta. */
@Component("goldActivityMapper")
public class ActivityMapper {

    /** Um item por dia do mes; dia sem linha ou com contributing = 0 vira "sem dado" (sem numero). */
    public List<ActivityDayItem> toDays(YearMonth month, List<DayActivity> found) {
        Map<LocalDate, DayActivity> byDay = new HashMap<>();
        found.forEach(d -> byDay.put(d.day(), d));
        List<ActivityDayItem> out = new ArrayList<>();
        for (int d = 1; d <= month.lengthOfMonth(); d++) {
            LocalDate date = month.atDay(d);
            DayActivity a = byDay.get(date);
            boolean hasData = a != null && a.contributing() > 0;
            out.add(new ActivityDayItem(date, hasData ? a.harmonized() : null, a == null ? 0 : a.contributing(),
                    hasData, hasData ? a.basis() : null, hasData ? temperature(a.bins()) : null));
        }
        return out;
    }

    /** As 7 faixas na ordem do contrato; nulo se nenhuma veio importada. Faixa nula num conjunto presente vale 0. */
    private static List<TemperatureBin> temperature(List<Double> sums) {
        if (sums == null || sums.stream().allMatch(java.util.Objects::isNull)) return null;
        List<TemperatureBin> out = new ArrayList<>();
        for (int i = 0; i < TemperatureBins.ALL.size(); i++) {
            TemperatureBins.Bin b = TemperatureBins.ALL.get(i);
            Double v = i < sums.size() ? sums.get(i) : null;
            out.add(new TemperatureBin(b.key(), b.fromK(), b.toK(), v == null ? 0.0 : v));
        }
        return out;
    }

    public ActivityMonthItem toItem(MonthActivity m) {
        YearMonth ym = YearMonth.from(m.month());
        boolean complete = ym.atEndOfMonth().isBefore(LocalDate.now(ZoneOffset.UTC));
        return new ActivityMonthItem(ym.toString(), m.daysWithData() > 0 ? m.harmonized() : null, m.daysWithData(),
                ym.lengthOfMonth(), complete);
    }

    public ActivityBaselineYear toBaseline(YearMonthSum s) {
        return new ActivityBaselineYear(s.year(), s.harmonized(), s.daysWithData());
    }

    public SatelliteDayItem toItem(SatelliteDay s) {
        return new SatelliteDayItem(s.day(), s.platform(), s.units(), s.typicalUnits(), s.available(),
                s.contributes());
    }
}
