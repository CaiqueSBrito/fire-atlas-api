package com.fireatlas.detection.service;

import com.fireatlas.common.dto.IndicatorMeta;
import com.fireatlas.h3.policy.H3Settings;
import com.fireatlas.common.policy.Messages;
import com.fireatlas.detection.dto.DetectionDto;
import com.fireatlas.detection.entity.CellAggregate;
import com.fireatlas.detection.entity.Coverage;
import com.fireatlas.detection.entity.Detection;
import com.fireatlas.detection.mapper.DetectionMapper;
import com.fireatlas.detection.repository.DetectionRepository;
import com.fireatlas.h3.policy.H3Index;
import com.fireatlas.region.entity.Region;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

/** Casos de uso das deteccoes: escolhe pontos ou celulas H3 conforme o zoom e informa a cobertura dos dados. */
@Service
public class DetectionService {

    private final DetectionRepository repository;
    private final DetectionMapper mapper;
    private final int maxPoints;
    private final H3Settings h3;

    public DetectionService(DetectionRepository repository, DetectionMapper mapper, H3Settings h3,
                            @Value("${fireatlas.limits.max-points}") int maxPoints) {
        this.repository = repository;
        this.mapper = mapper;
        this.maxPoints = maxPoints;
        this.h3 = h3;
    }

    /** zoom >= 9: pontos individuais (ate o limite); abaixo disso, celulas H3 na resolucao do zoom. */
    public DetectionDto.DetectionsResponse detections(Region region, LocalDate start, LocalDate end,
                                                      List<String> sensors, int zoom) {
        if (zoom >= H3Index.POINTS_MIN_ZOOM) {
            List<Detection> found = repository.findDetections(region, start, end, sensors, maxPoints);
            boolean truncated = found.size() > maxPoints;
            List<DetectionDto.Point> points = found.stream().limit(maxPoints).map(mapper::toPoint).toList();
            return new DetectionDto.DetectionsResponse("points", points.size(), truncated, null, points, null,
                    points.isEmpty() ? Messages.NO_DETECTIONS : null);
        }
        Region.Cover cover = region.coverFitting(h3.resolutionForZoom(zoom), Region.MAX_MAP_CELLS);
        List<CellAggregate> found = repository.findCells(cover, start, end, sensors, Region.MAX_MAP_CELLS);
        boolean truncated = found.size() > Region.MAX_MAP_CELLS;
        List<DetectionDto.Cell> cells = found.stream().limit(Region.MAX_MAP_CELLS)
                .map(c -> mapper.toCell(c, cover.res())).toList();
        int total = cells.stream().mapToInt(DetectionDto.Cell::count).sum();
        return new DetectionDto.DetectionsResponse("grid", total, truncated, cover.res(), null, cells,
                cells.isEmpty() ? Messages.NO_DETECTIONS : null);
    }

    static final String COUNT_METHOD =
            "Contagem exata de deteccoes (linhas da tabela de deteccoes) por sensor no recorte, com filtro de regiao "
                    + "exato (bbox) ou por indice H3 (cells); frpSum e a soma do FRP em MW; daysWithData = dias com "
                    + "ao menos uma deteccao. Contagens de MODIS e VIIRS nao sao diretamente comparaveis.";

    /** Contagem de deteccoes por sensor numa regiao e periodo (sem numeros inventados quando nao ha dados). */
    public DetectionDto.CountResponse count(Region region, LocalDate start, LocalDate end, List<String> sensors) {
        List<DetectionDto.SensorCount> bySensor = repository.findDetectionTotals(region, start, end, sensors).stream()
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
