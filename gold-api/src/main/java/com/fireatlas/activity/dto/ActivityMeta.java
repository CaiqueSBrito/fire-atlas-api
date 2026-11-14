package com.fireatlas.activity.dto;

import java.time.LocalDate;
import java.util.List;

/**
 * Identificacao do indicador (recorte, fonte, unidade e metodo) e os avisos de uso. O rotulo da unidade
 * (unitLabelPt / unitLabelEn) e obrigatorio na interface: o numero nao e quantidade de focos nem de incendios.
 */
public record ActivityMeta(String region, LocalDate start, LocalDate end, int resolution, String unitLabelPt,
                           String unitLabelEn, String source, String method, double areaKm2Estimate,
                           boolean smallArea, List<String> warnings) {}
