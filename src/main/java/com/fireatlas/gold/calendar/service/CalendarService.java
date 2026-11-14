package com.fireatlas.gold.calendar.service;

import com.fireatlas.gold.h3.policy.H3Settings;
import com.fireatlas.gold.calendar.dto.CalendarDto;
import com.fireatlas.gold.calendar.mapper.CalendarMapper;
import com.fireatlas.gold.common.policy.Messages;
import com.fireatlas.gold.detection.entity.DayTotal;
import com.fireatlas.gold.detection.repository.DetectionRepository;
import com.fireatlas.gold.h3.policy.H3Index;
import com.fireatlas.gold.region.entity.Region;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.List;

/** Caso de uso do calendario: totais por dia e sensor, somando as celulas H3 informadas. */
@Service("goldCalendarService")
public class CalendarService {

    private final DetectionRepository repository;
    private final H3Settings h3;
    private final CalendarMapper mapper;

    public CalendarService(DetectionRepository repository, CalendarMapper mapper, H3Settings h3) {
        this.repository = repository;
        this.mapper = mapper;
        this.h3 = h3;
    }

    public CalendarDto.CalendarResponse calendar(Region region, YearMonth month, List<String> sensors) {
        LocalDate start = month.atDay(1);
        LocalDate end = month.atEndOfMonth();
        Region.Cover cover = region.coverNative(h3.baseResolution(), Region.MAX_AGG_CELLS);
        List<DayTotal> totals = repository.findDailyTotals(cover, start, end, sensors);
        int daysWithFire = (int) totals.stream().map(DayTotal::date).distinct().count();
        return new CalendarDto.CalendarResponse(month.toString(), cover.res(), daysWithFire,
                mapper.toDays(start, end, totals), daysWithFire == 0 ? Messages.NO_DETECTIONS : null);
    }
}
