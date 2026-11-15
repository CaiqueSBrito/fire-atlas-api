package com.fireatlas.gold.ingestion.entity;

import java.time.LocalDate;
import java.util.Map;

/** Registro de uma importacao concluida de um arquivo (para lake_import_log). */
public record ImportLogEntry(String sourceKey, String product, String sensor, String bbox, LocalDate periodStart,
                             LocalDate periodEnd, long rowsScanned, long rowsLoaded, LocalDate minDate,
                             LocalDate maxDate, long rowsRejected, Map<String, Long> rejects, Long expectedRows,
                             String sourceSha256, boolean verified) {}
