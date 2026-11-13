package com.fireatlas.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/** Perfil anonimo: o navegador recebe um UUID em cookie HttpOnly e a API guarda a regiao favorita. */
@RestController
@RequestMapping("/api/v1/preferences")
public class PreferenceController {

    static final String COOKIE = "fa_uid";

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final String sameSite;
    private final boolean secure;

    public PreferenceController(JdbcTemplate jdbc, ObjectMapper mapper,
                                @Value("${fireatlas.cookie.same-site}") String sameSite,
                                @Value("${fireatlas.cookie.secure}") boolean secure) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.sameSite = sameSite;
        this.secure = secure;
    }

    @Operation(summary = "Le a regiao favorita do usuario anonimo (204 se nao houver)")
    @GetMapping
    public ResponseEntity<Dto.Preferences> get(@CookieValue(name = COOKIE, required = false) String uid) {
        UUID id = parse(uid);
        if (id == null) return ResponseEntity.noContent().build();
        List<String> rows = jdbc.queryForList("SELECT prefs::text FROM user_preference WHERE anon_id = ?",
                String.class, id);
        if (rows.isEmpty()) return ResponseEntity.noContent().build();
        try {
            return ResponseEntity.ok(mapper.readValue(rows.get(0), Dto.Preferences.class));
        } catch (JsonProcessingException e) {
            return ResponseEntity.noContent().build();
        }
    }

    @Operation(summary = "Salva a regiao favorita; cria o cookie anonimo se ainda nao existir")
    @PutMapping
    public ResponseEntity<Dto.Preferences> put(@CookieValue(name = COOKIE, required = false) String uid,
                                               @RequestBody Dto.Preferences prefs) throws JsonProcessingException {
        validate(prefs);
        UUID id = parse(uid);
        boolean isNew = id == null;
        if (isNew) id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO user_preference (anon_id, prefs, updated_at) VALUES (?, ?::jsonb, now())
                ON CONFLICT (anon_id) DO UPDATE SET prefs = EXCLUDED.prefs, updated_at = now()
                """, id, mapper.writeValueAsString(prefs));
        ResponseEntity.BodyBuilder ok = ResponseEntity.ok();
        if (isNew) {
            ResponseCookie c = ResponseCookie.from(COOKIE, id.toString())
                    .httpOnly(true).secure(secure).sameSite(sameSite).path("/")
                    .maxAge(Duration.ofDays(365)).build();
            ok.header(HttpHeaders.SET_COOKIE, c.toString());
        }
        return ok.body(prefs);
    }

    private static void validate(Dto.Preferences p) {
        if (p == null) throw new IllegalArgumentException("corpo obrigatorio");
        if (p.bbox() != null) {
            if (p.bbox().size() != 4) throw new IllegalArgumentException("bbox deve ter 4 valores");
            com.fireatlas.domain.BBox.parse(p.bbox().get(0) + "," + p.bbox().get(1) + ","
                    + p.bbox().get(2) + "," + p.bbox().get(3));
        }
        if (p.cells() != null && !p.cells().isEmpty()) {
            com.fireatlas.domain.Region.of(null, String.join(",", p.cells())); // valida indices H3 e limite
        }
        if (p.sensor() != null) QueryParams.parseSensors(p.sensor());
        if (p.name() != null && p.name().length() > 100) throw new IllegalArgumentException("name muito longo");
        if (p.zoom() != null && (p.zoom() < 0 || p.zoom() > 22)) throw new IllegalArgumentException("zoom invalido");
    }

    private static UUID parse(String s) {
        if (s == null) return null;
        try {
            return UUID.fromString(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
