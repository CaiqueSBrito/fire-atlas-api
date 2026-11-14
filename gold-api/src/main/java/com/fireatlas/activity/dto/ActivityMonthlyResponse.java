package com.fireatlas.activity.dto;

import java.util.List;

/** Somas mensais da atividade harmonizada na regiao, em ordem cronologica. */
public record ActivityMonthlyResponse(List<ActivityMonthItem> months, ActivityMeta meta, String message) {}
