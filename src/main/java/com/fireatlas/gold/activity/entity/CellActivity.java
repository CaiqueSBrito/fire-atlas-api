package com.fireatlas.gold.activity.entity;

/**
 * Atividade harmonizada de uma celula H3 somada no periodo: harmonized (celulas ativas equivalentes ao MODIS Aqua),
 * maior numero de satelites contribuintes, dias com dado e as deteccoes brutas por sensor (Terra+Aqua = MODIS;
 * S-NPP+NOAA-20+NOAA-21 = VIIRS) que entraram na harmonizacao.
 */
public record CellActivity(long cell, double harmonized, int contributing, int daysWithData, long modisDetections,
                           long viirsDetections) {}
