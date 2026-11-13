package com.fireatlas;

import com.fireatlas.calendar.mapper.CalendarMapper;
import com.fireatlas.detection.entity.CellAggregate;
import com.fireatlas.detection.entity.DayTotal;
import com.fireatlas.detection.entity.Detection;
import com.fireatlas.detection.entity.Location;
import com.fireatlas.detection.mapper.DetectionMapper;
import com.fireatlas.h3.policy.H3Index;
import com.fireatlas.sensor.entity.Satellite;
import com.fireatlas.sensor.entity.Sensor;
import com.fireatlas.sensor.mapper.SensorMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Testes da camada de mappers (dominio -> DTO), sem banco e sem Spring. */
class MapperTests {

    private final DetectionMapper detectionMapper = new DetectionMapper();

    @Test
    void detectionBecomesPointKeepingOriginsAndUnits() {
        var d = new Detection("MODIS", "Aqua", "MODIS", Instant.parse("2026-09-02T18:05:00Z"),
                Location.of(-10.5, -55.5), 330.1, 300.2, 1.0, 1.0, 25.5, "85", "h", "D", "6.1NRT", 0,
                new Detection.Source("MODIS_C6.1", "bronze/raw/MODIS_C6.1/2026/a.csv"));
        var p = detectionMapper.toPoint(d);
        assertEquals("MODIS", p.sensor());
        assertEquals("Aqua", p.satellite());
        assertEquals("MODIS", p.instrument());
        assertEquals(-10.5, p.lat());
        assertEquals(-55.5, p.lon());
        assertEquals(25.5f, p.frp());
        assertEquals(330.1f, p.brightness());
        assertEquals("h", p.confidence());
        assertEquals("D", p.dayNight());
        assertEquals(0, p.fireType());
        assertEquals("MODIS_C6.1", p.product());
        assertEquals("bronze/raw/MODIS_C6.1/2026/a.csv", p.sourceFile());

        var sparse = new Detection("VIIRS", "SNPP", null, Instant.parse("2026-09-02T18:05:00Z"),
                Location.of(0, 0), null, null, null, null, null, null, null, null, null, null,
                new Detection.Source("VIIRS_S-NPP", "f.csv"));
        var q = detectionMapper.toPoint(sparse);
        assertNull(q.frp());
        assertNull(q.brightness());
        assertNull(q.fireType());
    }

    @Test
    void cellAggregateBecomesH3CellWithoutCoordinates() {
        long cell = H3Index.parent(TestH3.cell(-10.5, -55.5), 6);
        var c = detectionMapper.toCell(new CellAggregate("VIIRS", cell, 7, 42.5, 9.5f), 6);
        assertEquals(H3Index.toHex(cell), c.h3());
        assertEquals(6, c.resolution());
        assertEquals(7, c.count());
        assertEquals(42.5, c.frpSum());
        assertEquals(9.5f, c.frpMax());
    }

    @Test
    void calendarFillsEveryDayAndKeepsSensorsApart() {
        var days = new CalendarMapper().toDays(LocalDate.of(2026, 9, 1), LocalDate.of(2026, 9, 3), List.of(
                new DayTotal(LocalDate.of(2026, 9, 1), "MODIS", 2, 11.0),
                new DayTotal(LocalDate.of(2026, 9, 1), "VIIRS", 35, 316.4),
                new DayTotal(LocalDate.of(2026, 9, 3), "VIIRS", 1, 9.2)));
        assertEquals(3, days.size());
        assertEquals(2, days.get(0).modis().count());
        assertEquals(35, days.get(0).viirs().count());
        assertEquals(0, days.get(1).modis().count());      // dia sem foco
        assertEquals(0.0, days.get(1).viirs().frpSum());
        assertEquals(0, days.get(2).modis().count());
        assertEquals(1, days.get(2).viirs().count());
    }

    @Test
    void catalogEntryCarriesSatellites() {
        var sensor = new Sensor("VIIRS", "Visible Infrared Imaging Radiometer Suite", 375, "d");
        var sat = new Satellite("SNPP", "VIIRS", "Suomi NPP", "VIIRS_S-NPP");
        var entry = new SensorMapper().toEntry(sensor, List.of(sat));
        assertEquals("VIIRS", entry.code());
        assertEquals(375, entry.nominalResolutionMeters());
        assertEquals(List.of(sat), entry.satellites());
    }
}
