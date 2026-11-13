package com.fireatlas.preference.repository;

import com.fireatlas.preference.entity.UserPreference;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Persistencia do perfil anonimo (tabela user_preference, JSON em jsonb). */
@Repository
public class PreferenceRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public PreferenceRepository(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    public Optional<UserPreference> find(UUID anonId) {
        List<String> rows = jdbc.queryForList("SELECT prefs::text FROM user_preference WHERE anon_id = ?",
                String.class, anonId);
        if (rows.isEmpty()) return Optional.empty();
        try {
            return Optional.of(mapper.readValue(rows.get(0), UserPreference.class));
        } catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }

    public void upsert(UUID anonId, UserPreference prefs) throws JsonProcessingException {
        jdbc.update("""
                INSERT INTO user_preference (anon_id, prefs, updated_at) VALUES (?, ?::jsonb, now())
                ON CONFLICT (anon_id) DO UPDATE SET prefs = EXCLUDED.prefs, updated_at = now()
                """, anonId, mapper.writeValueAsString(prefs));
    }
}
