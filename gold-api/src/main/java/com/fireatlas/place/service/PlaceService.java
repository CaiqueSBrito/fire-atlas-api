package com.fireatlas.place.service;

import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.common.policy.ParameterError;
import com.fireatlas.place.dto.CityItem;
import com.fireatlas.place.dto.ContinentItem;
import com.fireatlas.place.dto.CountryItem;
import com.fireatlas.place.dto.PlaceList;
import com.fireatlas.place.mapper.PlaceMapper;
import com.fireatlas.place.repository.PlaceRepository;
import com.fireatlas.region.entity.Region;
import com.fireatlas.region.policy.Continents;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/** Casos de uso do catalogo de lugares: continentes, paises e cidades para usar nos filtros das consultas. */
@Service
public class PlaceService {

    static final String SOURCE = "gold/h3_places (time de dados): continente, pais predominante e cidade mais "
            + "proxima de cada celula H3";
    static final String COUNTRY_NOTE = "Cada celula H3 e atribuida ao pais predominante nela: nas fronteiras o "
            + "filtro por pais e aproximado (na ordem do tamanho da celula).";
    static final String CITY_NOTE = "A cidade e a MAIS PROXIMA do centro da celula (GeoNames cities1000), nao "
            + "\"dentro\" da cidade: o filtro usa um raio maximo (cityRadiusKm).";

    private final PlaceRepository repository;
    private final PlaceMapper mapper;

    public PlaceService(PlaceRepository repository, PlaceMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    public PlaceList<ContinentItem> continents() {
        var items = repository.findContinents().stream().map(mapper::toItem).toList();
        return new PlaceList<>(items.size(), items, SOURCE, COUNTRY_NOTE);
    }

    public PlaceList<CountryItem> countries(String continent) {
        String resolved = null;
        if (continent != null && !continent.isBlank()) {
            resolved = Continents.resolve(continent).orElseThrow(() -> new InvalidParameterException("continent",
                    "continent desconhecido; use um entre: " + Continents.validNames()));
        }
        var items = repository.findCountries(resolved).stream().map(mapper::toItem).toList();
        return new PlaceList<>(items.size(), items, SOURCE, COUNTRY_NOTE);
    }

    public PlaceList<CityItem> cities(String country, String q, Double radiusKm, int limit) {
        List<ParameterError> errors = new ArrayList<>();
        double radius = radiusKm == null ? Region.DEFAULT_CITY_RADIUS_KM : radiusKm;
        if (radius < 1 || radius > 500) {
            errors.add(new ParameterError("cityRadiusKm", "cityRadiusKm deve estar entre 1 e 500"));
        }
        if (limit < 1 || limit > 500) errors.add(new ParameterError("limit", "limit deve estar entre 1 e 500"));
        if (country != null && !country.isBlank() && !country.trim().matches("[A-Za-z]{2}")) {
            errors.add(new ParameterError("country", "country deve ser o codigo ISO de 2 letras (ex.: BR)"));
        }
        if (!errors.isEmpty()) throw new InvalidParameterException(errors);
        var items = repository.findCities(country == null || country.isBlank() ? null
                : country.trim().toUpperCase(), q == null || q.isBlank() ? null : q, radius, limit).stream()
                .map(mapper::toItem).toList();
        return new PlaceList<>(items.size(), items, SOURCE, CITY_NOTE);
    }
}
