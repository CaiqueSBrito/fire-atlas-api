package com.fireatlas.detection.entity;

import java.time.LocalDate;

/** Total diario de um sensor em uma regiao. */
public record DayTotal(LocalDate date, String sensor, int count, double frpSum) {}
