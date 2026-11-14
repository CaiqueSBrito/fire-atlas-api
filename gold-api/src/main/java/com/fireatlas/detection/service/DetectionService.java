package com.fireatlas.detection.service;

import com.fireatlas.common.dto.IndicatorMeta;
import com.fireatlas.common.policy.Messages;
import com.fireatlas.detection.dto.DetectionDto;
import com.fireatlas.detection.entity.CellAggregate;
import com.fireatlas.detection.entity.Coverage;
import com.fireatlas.detection.mapper.DetectionMapper;
import com.fireatlas.detection.repository.DetectionRepository;
import com.fireatlas.h3.policy.H3Settings;
import com.fireatlas.region.entity.Region;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

/** Casos de uso das deteccoes: grade H3 para o mapa, contagem por sensor e cobertura dos dados carregados. */
@Service
public class DetectionService {

    static final String COUNT_METHOD =
            "Soma das deteccoes por sensor nas celulas H3 da regiao (grade diaria da gold, na resolucao das celulas "
                    + "informadas); frpSum e a soma do FRP em MW; daysWithData = dias com ao menos uma deteccao. "
                    + "Contagens de MODIS e VIIRS nao sao diretamente comparaveis.";

    private final DetectionRepository repository;
    private final DetectionMapper mapper;
    private final H3Settings h3;

    public DetectionService(DetectionRepository repository, DetectionMapper mapper, H3Settings h3) {
        this.repository = repository;
        this.mapper = mapper;
        this.h3 = h3;
    }

    /** Celulas H3 agregadas na resolucao do zoom (a gold nao tem pontos individuais). */
    public DetectionDto.DetectionsResponse detections(Region region, LocalDate start, LocalDate end,
                                                      List<String> sensors, int zoom) {
        Region.Cover cover = region.coverFitting(h3.resolutionForZoom(zoom), Region.MAX_MAP_CELLS);
        List<CellAggregate> found = repository.findCells(cover, start, end, sensors, Region.MAX_MAP_CELLS);
        boolean truncated = found.size() > Region.MAX_MAP_CELLS;
        List<DetectionDto.Cell> cells = found.stream().limit(Region.MAX_MAP_CELLS)
                .map(c -> mapper.toCell(c, cover.res())).toList();
        int total = cells.stream().mapToInt(DetectionDto.Cell::count).sum();
        return new DetectionDto.DetectionsResponse("grid", total, truncated, cover.res(), cells,
                cells.isEmpty() ? Messages.NO_DETECTIONS : null);
    }

    /** Contagem de deteccoes por sensor numa regiao e periodo (sem numeros inventados quando nao ha dados). */
    public DetectionDto.CountResponse count(Region region, LocalDate start, LocalDate end, List<String> sensors) {
        Region.Cover cover = region.coverNative(h3.baseResolution(), Region.MAX_AGG_CELLS);
        List<DetectionDto.SensorCount> bySensor = repository.findPeriodTotals(cover, start, end, sensors).stream()
                .map(t -> new DetectionDto.SensorCount(t.sensor(), t.count(), t.frpSum(), t.daysWithData()))
                .toList();
        long total = bySensor.stream().mapToLong(DetectionDto.SensorCount::detections).sum();
        return new DetectionDto.CountResponse(total, bySensor,
                IndicatorMeta.of(region.describe(), start, end, COUNT_METHOD),
                bySensor.isEmpty() ? Messages.NO_DETECTIONS : null);
    }

    /** Cobertura dos dados carregados (periodo e total de deteccoes por sensor). */
    public List<Coverage> coverage() {
        return repository.findCoverage();
    }
}
