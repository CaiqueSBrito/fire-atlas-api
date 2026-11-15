package com.fireatlas.gold;

import com.fireatlas.gold.activity.entity.DayActivity;
import com.fireatlas.gold.activity.mapper.ActivityMapper;
import com.fireatlas.gold.calendar.mapper.CalendarMapper;
import com.fireatlas.gold.detection.entity.CellAggregate;
import com.fireatlas.gold.detection.entity.DayTotal;
import com.fireatlas.gold.detection.mapper.DetectionMapper;
import com.fireatlas.gold.h3.policy.H3Index;
import com.fireatlas.gold.sensor.entity.Satellite;
import com.fireatlas.gold.sensor.entity.Sensor;
import com.fireatlas.gold.sensor.mapper.SensorMapper;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Testes da camada de mappers (dominio -> DTO), sem banco e sem Spring. */
class MapperTests {

    private final DetectionMapper detectionMapper = new DetectionMapper();

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

    @Test
    void activityDaySplitsHarmonizedIntoTheSevenTemperatureBinsInContractOrder() {
        var month = YearMonth.of(2026, 11);
        var days = new ActivityMapper().toDays(month, List.of(
                new DayActivity(month.atDay(1), 10.0, 2, 4, "viirs", Arrays.asList(1.0, 2.0, null, 3.0, 1.5, 2.0, 0.5)),
                new DayActivity(month.atDay(2), 4.0, 2, 1, "aqua", Arrays.asList(null, null, null, null, null, null,
                        null)),
                new DayActivity(month.atDay(3), 0.0, 0, 1, "viirs", Arrays.asList(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))));

        var bins = days.get(0).temperature();
        assertEquals("viirs", days.get(0).basis());
        assertEquals(List.of("lt320", "320", "330", "340", "350", "ge360", "none"),
                bins.stream().map(b -> b.key()).toList());
        assertNull(bins.get(0).fromK());
        assertEquals(320.0, bins.get(0).toK());
        assertEquals(360.0, bins.get(5).fromK());
        assertNull(bins.get(5).toK());
        assertEquals(0.0, bins.get(2).harmonized());          // faixa nula num conjunto presente vale 0
        assertEquals(days.get(0).harmonized(), bins.stream().mapToDouble(b -> b.harmonized()).sum(), 1e-9);

        assertNull(days.get(1).temperature());                 // arquivo antigo, sem faixas
        assertEquals("aqua", days.get(1).basis());
        assertNull(days.get(2).temperature());                 // contributing = 0: sem dado
        assertNull(days.get(2).basis());
        assertNull(days.get(3).temperature());                 // dia sem linha
    }
}
