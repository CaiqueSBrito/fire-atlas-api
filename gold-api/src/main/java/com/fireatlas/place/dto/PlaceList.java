package com.fireatlas.place.dto;

import java.util.List;

/** Lista de lugares com a fonte e um aviso sobre como o lugar e atribuido as celulas. */
public record PlaceList<T>(int count, List<T> items, String source, String note) {}
