package com.fireatlas.activity.entity;

import java.time.LocalDate;

/** Atividade harmonizada de um mes na regiao; month e o primeiro dia do mes. */
public record MonthActivity(LocalDate month, double harmonized, int daysWithData) {}
