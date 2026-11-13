package com.fireatlas.detection.entity;

/** Agregado de uma celula H3 para um sensor no periodo consultado. */
public record CellAggregate(String sensor, long cell, int count, double frpSum, Float frpMax) {}
