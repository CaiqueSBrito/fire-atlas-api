package com.fireatlas.preference.controller;

import com.fireatlas.preference.entity.UserPreference;
import com.fireatlas.preference.service.PreferenceService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.UUID;

/**
 * Perfil anonimo: o navegador recebe um UUID em cookie HttpOnly e a API guarda a regiao favorita.
 * O cookie e preocupacao desta camada; a validacao e a persistencia ficam no servico.
 */
@RestController
@RequestMapping("/api/v1/preferences")
public class PreferenceController {

    static final String COOKIE = "fa_uid";

    private final PreferenceService service;
    private final String sameSite;
    private final boolean secure;

    public PreferenceController(PreferenceService service,
                                @Value("${fireatlas.cookie.same-site}") String sameSite,
                                @Value("${fireatlas.cookie.secure}") boolean secure) {
        this.service = service;
        this.sameSite = sameSite;
        this.secure = secure;
    }

    @Operation(summary = "Le a regiao favorita do usuario anonimo (204 se nao houver)")
    @GetMapping
    public ResponseEntity<UserPreference> get(@CookieValue(name = COOKIE, required = false) String uid) {
        UUID id = parse(uid);
        if (id == null) return ResponseEntity.noContent().build();
        return service.find(id).map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.noContent().build());
    }

    @Operation(summary = "Salva a regiao favorita; cria o cookie anonimo se ainda nao existir")
    @PutMapping
    public ResponseEntity<UserPreference> put(@CookieValue(name = COOKIE, required = false) String uid,
                                              @RequestBody UserPreference prefs) {
        UUID id = parse(uid);
        boolean isNew = id == null;
        if (isNew) id = UUID.randomUUID();
        UserPreference saved = service.save(id, prefs);
        ResponseEntity.BodyBuilder ok = ResponseEntity.ok();
        if (isNew) {
            ResponseCookie c = ResponseCookie.from(COOKIE, id.toString())
                    .httpOnly(true).secure(secure).sameSite(sameSite).path("/")
                    .maxAge(Duration.ofDays(365)).build();
            ok.header(HttpHeaders.SET_COOKIE, c.toString());
        }
        return ok.body(saved);
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
