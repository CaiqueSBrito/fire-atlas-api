package com.fireatlas.activity.dto;

import java.util.List;

/** Calendario harmonizado de um mes: um item por dia do mes (dias sem dado ficam sem numero). */
public record ActivityCalendarResponse(String month, int daysWithData, List<ActivityDayItem> days, ActivityMeta meta,
                                       String message) {}
