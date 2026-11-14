package com.fireatlas.place.controller;

import com.fireatlas.common.controller.HttpCache;
import com.fireatlas.place.dto.CityItem;
import com.fireatlas.place.dto.ContinentItem;
import com.fireatlas.place.dto.CountryItem;
import com.fireatlas.place.dto.PlaceList;
import com.fireatlas.place.service.PlaceService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

/** Catalogo de lugares para os filtros country=, continent= e city= das consultas. */
@RestController
@RequestMapping("/api/v1/places")
public class PlaceController {

    private final PlaceService service;

    public PlaceController(PlaceService service) {
        this.service = service;
    }

    @Operation(summary = "Continentes com dados na grade (use o nome em continent=)")
    @GetMapping("/continents")
    public ResponseEntity<PlaceList<ContinentItem>> continents() {
        return HttpCache.ok(service.continents(), LocalDate.now().minusYears(1));
    }

    @Operation(summary = "Paises (use isoA2 em country=); filtre por continent")
    @GetMapping("/countries")
    public ResponseEntity<PlaceList<CountryItem>> countries(@RequestParam(required = false) String continent) {
        return HttpCache.ok(service.countries(continent), LocalDate.now().minusYears(1));
    }

    @Operation(summary = "Cidades (use geonameId em city=); a cidade e a mais proxima das celulas, dentro de um raio")
    @GetMapping("/cities")
    public ResponseEntity<PlaceList<CityItem>> cities(
            @RequestParam(required = false) String country,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) Double cityRadiusKm,
            @RequestParam(defaultValue = "50") int limit) {
        return HttpCache.ok(service.cities(country, q, cityRadiusKm, limit), LocalDate.now().minusYears(1));
    }
}
