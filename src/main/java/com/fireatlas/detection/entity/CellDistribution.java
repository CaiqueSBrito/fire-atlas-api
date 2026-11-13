package com.fireatlas.detection.entity;

/** Distribuicao das contagens por celula: quantas celulas tem registro e o valor no percentil pedido (ou null). */
public record CellDistribution(long cells, Double percentileValue) {}
