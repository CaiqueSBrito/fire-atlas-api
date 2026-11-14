package com.fireatlas.place.entity;

/** Pais da gold: codigos ISO de 2 e 3 letras, nomes, continente e quantas celulas H3 ele tem (na resolucao 4). */
public record Country(String isoA2, String isoA3, String name, String namePt, String continent, long cells) {}
