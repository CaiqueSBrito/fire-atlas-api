package com.fireatlas.gold.activity.entity;

import java.time.LocalDate;

/** Atividade harmonizada de um dia local na regiao: soma, maior numero de satelites contribuintes e linhas. */
public record DayActivity(LocalDate day, double harmonized, int contributing, long rows) {}
