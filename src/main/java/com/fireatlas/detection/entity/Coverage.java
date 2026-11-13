package com.fireatlas.detection.entity;

import java.time.LocalDate;

/** Cobertura dos dados carregados para um sensor. */
public record Coverage(String sensor, LocalDate minDate, LocalDate maxDate, long detections) {}
