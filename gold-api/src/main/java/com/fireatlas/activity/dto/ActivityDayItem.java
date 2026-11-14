package com.fireatlas.activity.dto;

import java.time.LocalDate;

/**
 * Um dia do calendario. harmonized e nulo quando nao ha dado (hasData = false): contributing = 0 significa sem dado,
 * nunca zero fogo.
 */
public record ActivityDayItem(LocalDate date, Double harmonized, int contributing, boolean hasData) {}
