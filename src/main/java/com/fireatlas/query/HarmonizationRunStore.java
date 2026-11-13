package com.fireatlas.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fireatlas.domain.BBox;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;

/**
 * Persistencia de resultados derivados (RN05). Grava apenas em harmonization_run:
 * as deteccoes originais nunca sao alteradas (o banco bloqueia UPDATE em detection).
 */
@Repository
public class HarmonizationRunStore {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public HarmonizationRunStore(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /** @return id da execucao gravada */
    public long save(String method, String methodVersion, BBox bbox, LocalDate start, LocalDate end,
                     Object params, Object result) {
        try {
            Long id = jdbc.queryForObject("""
                    INSERT INTO harmonization_run (method, method_version, bbox, period_start, period_end, params, result)
                    VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb) RETURNING id
                    """, Long.class, method, methodVersion, bbox.toString(), start, end,
                    mapper.writeValueAsString(params), mapper.writeValueAsString(result));
            return id == null ? -1 : id;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("resultado derivado nao serializavel", e);
        }
    }
}
