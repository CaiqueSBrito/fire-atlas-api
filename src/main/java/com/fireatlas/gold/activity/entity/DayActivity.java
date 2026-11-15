package com.fireatlas.gold.activity.entity;

import java.time.LocalDate;
import java.util.List;

/**
 * Atividade harmonizada de um dia local na regiao: soma, maior numero de satelites contribuintes e linhas. bins traz
 * a soma de cada faixa de brilho na ordem de TemperatureBins.ALL (nulo = faixa sem valor importado).
 */
public record DayActivity(LocalDate day, double harmonized, int contributing, long rows, String basis,
                          List<Double> bins) {}
