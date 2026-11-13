package com.fireatlas.preference.service;

import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.preference.entity.UserPreference;
import com.fireatlas.preference.repository.PreferenceRepository;
import com.fireatlas.region.entity.BBox;
import com.fireatlas.region.entity.Region;
import com.fireatlas.sensor.policy.Sensors;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

/** Perfil anonimo: regiao favorita guardada por um identificador sem login (cookie no navegador). */
@Service
public class PreferenceService {

    private final PreferenceRepository repository;

    public PreferenceService(PreferenceRepository repository) {
        this.repository = repository;
    }

    public Optional<UserPreference> find(UUID anonId) {
        return repository.find(anonId);
    }

    /** Valida (bbox, celulas H3, sensor, zoom) e grava; lanca IllegalArgumentException se invalido. */
    public UserPreference save(UUID anonId, UserPreference prefs) {
        validate(prefs);
        try {
            repository.upsert(anonId, prefs);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("preferencia nao serializavel", e);
        }
        return prefs;
    }

    static void validate(UserPreference p) {
        if (p == null) throw new InvalidParameterException("body", "corpo obrigatorio");
        if (p.bbox() != null) {
            if (p.bbox().size() != 4) throw new InvalidParameterException("bbox", "bbox deve ter 4 valores");
            BBox.parse(p.bbox().get(0) + "," + p.bbox().get(1) + "," + p.bbox().get(2) + "," + p.bbox().get(3));
        }
        if (p.cells() != null && !p.cells().isEmpty()) {
            Region.of(null, String.join(",", p.cells())); // valida indices H3 e limite
        }
        if (p.sensor() != null) Sensors.parse(p.sensor());
        if (p.name() != null && p.name().length() > 100) throw new InvalidParameterException("name", "name muito longo");
        if (p.zoom() != null && (p.zoom() < 0 || p.zoom() > 22)) throw new InvalidParameterException("zoom", "zoom invalido");
    }
}
