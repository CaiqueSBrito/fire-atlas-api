package com.fireatlas.detection.entity;

/** Total de um sensor em um periodo (somando as celulas H3 da regiao); daysWithData = dias com ao menos um registro. */
public record PeriodTotal(String sensor, long count, double frpSum, int daysWithData) {}
