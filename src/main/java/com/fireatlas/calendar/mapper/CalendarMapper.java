package com.fireatlas.calendar.mapper;

import com.fireatlas.calendar.dto.CalendarDto;
import com.fireatlas.detection.entity.DayTotal;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/** Converte totais diarios por sensor nas "cartas" do calendario (um dia por posicao, dias sem foco = 0). */
@Component
public class CalendarMapper {

    public List<CalendarDto.CalendarDay> toDays(LocalDate start, LocalDate end, List<DayTotal> totals) {
        List<CalendarDto.CalendarDay> days = new ArrayList<>();
        for (LocalDate d = start; !d.isAfter(end); d = d.plusDays(1)) {
            days.add(new CalendarDto.CalendarDay(d, sensorDay(totals, d, "MODIS"), sensorDay(totals, d, "VIIRS")));
        }
        return days;
    }

    private static CalendarDto.SensorDay sensorDay(List<DayTotal> totals, LocalDate date, String sensor) {
        return totals.stream()
                .filter(t -> t.date().equals(date) && t.sensor().equals(sensor))
                .findFirst()
                .map(t -> new CalendarDto.SensorDay(t.count(), t.frpSum()))
                .orElseGet(() -> new CalendarDto.SensorDay(0, 0.0));
    }
}
