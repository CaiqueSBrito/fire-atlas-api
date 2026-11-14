package com.fireatlas.activity.mapper;

import com.fireatlas.activity.dto.ActivityBaselineYear;
import com.fireatlas.activity.dto.ActivityDayItem;
import com.fireatlas.activity.dto.ActivityMonthItem;
import com.fireatlas.activity.dto.SatelliteDayItem;
import com.fireatlas.activity.entity.DayActivity;
import com.fireatlas.activity.entity.MonthActivity;
import com.fireatlas.activity.entity.SatelliteDay;
import com.fireatlas.activity.entity.YearMonthSum;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converte as entidades de atividade harmonizada nos DTOs de resposta. */
@Component
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
                    hasData));
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
