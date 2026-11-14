package com.fireatlas.activity.dto;

/**
 * Soma mensal da atividade harmonizada. complete = o mes ja terminou (o corrente e incompleto); daysWithData = dias
 * com ao menos um satelite contribuindo.
 */
public record ActivityMonthItem(String month, Double harmonized, int daysWithData, int daysInMonth,
                                boolean complete) {}
