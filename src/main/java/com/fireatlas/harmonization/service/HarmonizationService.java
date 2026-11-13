package com.fireatlas.harmonization.service;

import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.harmonization.dto.HarmonizationDto;
import com.fireatlas.harmonization.entity.HarmonizationResult;
import com.fireatlas.harmonization.entity.MatchedPair;
import com.fireatlas.harmonization.mapper.HarmonizationMapper;
import com.fireatlas.harmonization.repository.HarmonizationRepository;
import com.fireatlas.region.entity.Region;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

/** Caso de uso da harmonizacao: pareamento espaco-temporal MODIS -> VIIRS (metodo v0, docs/HARMONIZACAO.md). */
@Service
public class HarmonizationService {

    private final HarmonizationRepository repository;
    private final HarmonizationMapper mapper;

    public HarmonizationService(HarmonizationRepository repository, HarmonizationMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    public HarmonizationDto.Response harmonize(Region region, LocalDate start, LocalDate end,
                                               int maxDistanceMeters, int maxMinutes, int modisSampleCap) {
        validate(maxDistanceMeters, maxMinutes, modisSampleCap);
        HarmonizationResult result = repository.computeHarmonization(region, start, end, maxDistanceMeters,
                maxMinutes, modisSampleCap);
        HarmonizationDto.Method method = method();
        String message = result.modisEvaluated() == 0
                ? "Nenhum foco MODIS encontrado para os filtros selecionados." : null;
        return new HarmonizationDto.Response(region.describe(), start, end,
                new HarmonizationDto.Params(maxDistanceMeters, maxMinutes, modisSampleCap),
                result, method, message);
    }

    /** Camada de correspondencias: cada MODIS com o VIIRS mais proximo dentro dos limiares. */
    public HarmonizationDto.MatchesResponse matches(Region region, LocalDate start, LocalDate end,
                                                    int maxDistanceMeters, int maxMinutes, int modisSampleCap,
                                                    int limit) {
        validate(maxDistanceMeters, maxMinutes, modisSampleCap);
        if (limit < 1 || limit > 5000) {
            throw new InvalidParameterException("limit", "limit deve estar entre 1 e 5000");
        }
        List<MatchedPair> pairs = repository.findMatches(region, start, end, maxDistanceMeters, maxMinutes,
                modisSampleCap, limit + 1);
        boolean truncated = pairs.size() > limit;
        List<HarmonizationDto.Match> out = pairs.stream().limit(limit).map(mapper::toMatch).toList();
        return new HarmonizationDto.MatchesResponse(region.describe(), start, end,
                new HarmonizationDto.Params(maxDistanceMeters, maxMinutes, modisSampleCap), out.size(), truncated,
                out, method(),
                out.isEmpty() ? "Nenhuma correspondencia MODIS x VIIRS para os filtros selecionados." : null);
    }

    private static void validate(int maxDistanceMeters, int maxMinutes, int modisSampleCap) {
        if (maxDistanceMeters < 100 || maxDistanceMeters > 5000) {
            throw new InvalidParameterException("maxDistanceMeters", "maxDistanceMeters deve estar entre 100 e 5000");
        }
        if (maxMinutes < 1 || maxMinutes > 720) {
            throw new InvalidParameterException("maxMinutes", "maxMinutes deve estar entre 1 e 720");
        }
        if (modisSampleCap < 100 || modisSampleCap > 100_000) {
            throw new InvalidParameterException("modisSampleCap", "modisSampleCap deve estar entre 100 e 100000");
        }
    }

    private static HarmonizationDto.Method method() {
        return new HarmonizationDto.Method(
                "Pareamento espaco-temporal MODIS->VIIRS (v0)",
                "Para cada deteccao MODIS, conta deteccoes VIIRS a ate maxDistanceMeters e maxMinutes. "
                        + "Compara FRP do MODIS com a soma do FRP VIIRS dentro do mesmo pixel e reporta taxa de "
                        + "correspondencia, razao de FRP e correlacao. Campos originais e origem sao preservados.",
                List.of("Metodo v0: precisa de validacao com a equipe (limiares e razao de FRP).",
                        "Passagens de satelites diferentes ocorrem em horarios distintos (em 09/2026 sobre o Brasil: "
                                + "Aqua-VIIRS ~2 h, Terra-VIIRS ~4-5 h; nenhum par a menos de 30 min): "
                                + "o pareamento e de 'mesmo foco no mesmo periodo', nao de observacao simultanea.",
                        "Nuvens, angulo de varredura e area do pixel afetam a deteccao nos dois sensores.",
                        "Amostra limitada a modisSampleCap deteccoes MODIS (campo sampled)."));
    }
}
