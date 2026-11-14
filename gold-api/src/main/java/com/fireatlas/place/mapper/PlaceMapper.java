package com.fireatlas.place.mapper;

import com.fireatlas.place.dto.CityItem;
import com.fireatlas.place.dto.ContinentItem;
import com.fireatlas.place.dto.CountryItem;
import com.fireatlas.place.entity.City;
import com.fireatlas.place.entity.Continent;
import com.fireatlas.place.entity.Country;
import org.springframework.stereotype.Component;

/** Converte as entidades de lugar nos DTOs de resposta. */
@Component
public class PlaceMapper {

    public ContinentItem toItem(Continent c) {
        return new ContinentItem(c.name(), c.countries());
    }

    public CountryItem toItem(Country c) {
        return new CountryItem(c.isoA2(), c.isoA3(), c.name(), c.namePt(), c.continent(), c.cells());
    }

    public CityItem toItem(City c) {
        return new CityItem(c.geonameId(), c.name(), c.countryCode(), c.cells(),
                Math.round(c.nearestKm() * 10) / 10.0);
    }
}
