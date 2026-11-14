package com.fireatlas.detection.entity;

/** Celula H3 com os totais de todos os sensores selecionados no periodo (base dos rankings). */
public record RankedCell(long cell, long count, double frpSum, Float frpMax, long modisCount, long viirsCount) {}
