package com.fireatlas.common.controller;

import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/** Politica de cache HTTP das respostas de consulta (preocupacao da camada de apresentacao). */
public final class HttpCache {

    private HttpCache() {}

    /** Dados historicos mudam pouco: cache curto no cliente/CDN; dados recentes, ainda menor. */
    public static <T> ResponseEntity<T> ok(T body, LocalDate periodEnd) {
        long ageDays = ChronoUnit.DAYS.between(periodEnd, LocalDate.now(ZoneOffset.UTC));
        long seconds = ageDays > 7 ? 600 : 60;
        return ResponseEntity.ok().cacheControl(CacheControl.maxAge(Duration.ofSeconds(seconds)).cachePublic())
                .body(body);
    }
}
