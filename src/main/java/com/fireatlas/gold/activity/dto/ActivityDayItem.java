package com.fireatlas.gold.activity.dto;

import java.time.LocalDate;
import java.util.List;

/**
 * Um dia do calendario. harmonized e nulo quando nao ha dado (hasData = false): contributing = 0 significa sem dado,
 * nunca zero fogo. temperature traz as 7 faixas de brilho, ou nulo quando nao ha dado ou faixas importadas.
 */
public record ActivityDayItem(LocalDate date, Double harmonized, int contributing, boolean hasData, String basis,
                              List<TemperatureBin> temperature) {}
