package com.fireatlas.activity.dto;

import java.util.List;

/**
 * Anomalia mensal: a soma do mes consultado contra a MEDIANA do mesmo mes nos anos anteriores com dado. Com menos de
 * minYears anos, sufficient = false e nao se calcula mediana nem desvio (RN05).
 */
public record ActivityAnomalyResponse(String month, Double current, int currentDaysWithData,
                                      boolean currentMonthComplete, List<ActivityBaselineYear> baseline,
                                      int yearsWithData, int minYears, Double baselineMedian, Double ratioToMedian,
                                      Double deviationPct, boolean sufficient, String note, ActivityMeta meta) {}
