package com.fireatlas.calendar.dto;

import java.time.LocalDate;
import java.util.List;

/** Contratos de resposta do calendario (as "cartas" por dia). */
public final class CalendarDto {

    private CalendarDto() {}

    public record SensorDay(int count, double frpSum) {}

    public record CalendarDay(LocalDate date, SensorDay modis, SensorDay viirs) {}

    public record CalendarResponse(String month, int resolution, int daysWithFire, List<CalendarDay> days,
                                   String message) {}
}
