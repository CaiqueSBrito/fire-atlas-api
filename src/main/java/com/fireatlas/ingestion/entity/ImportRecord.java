package com.fireatlas.ingestion.entity;

import java.time.Instant;
import java.time.LocalDate;

/** Uma importacao registrada: concluida ou falha, com contagens, rejeicoes e conferencia com a fonte. */
public record ImportRecord(String product, String sensor, String sourceKey, String bbox, LocalDate periodStart,
                           LocalDate periodEnd, String status, String error, long rowsLoaded, long rowsRejected,
                           Long expectedRows, boolean verified, String sourceSha256, LocalDate minDate,
                           LocalDate maxDate, Instant loadedAt) {}
