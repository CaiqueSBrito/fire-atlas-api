package com.fireatlas.query;

import com.fireatlas.api.Dto;
import com.fireatlas.domain.BBox;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Orquestra a correspondencia exploratoria MODIS x VIIRS sob snapshot transacional consistente,
 * gerenciando orcamento de tempo SQL compartilhado entre statements na transacao (SET LOCAL statement_timeout,
 * sem garantir tempo total HTTP/rede/serializacao) e separacao rigorosa entre associacao exploratoria,
 * harmonizacao cientifica nao aprovada e classificacao nao avaliada.
 */
@Service
public class HarmonizationService {

    public static final String EXPLORATORY_METHOD_VERSION = "exploratory-spatial-temporal-v0.2";
    public static final String CORRESPONDENCE_EXPLORATORY = "EXPLORATORY_ASSOCIATION";
    public static final String CORRESPONDENCE_NOT_COMPUTABLE = "NOT_COMPUTABLE";
    public static final String HARMONIZATION_NOT_APPROVED = "METHOD_NOT_APPROVED";
    public static final String CLASSIFICATION_NOT_EVALUATED = "NOT_EVALUATED";
    public static final String CONFIDENCE_SEMANTICS = "PRODUCT_NATIVE_NOT_EQUIVALENT";

    private static final List<String> LIMITATIONS = List.of(
            "Parametros de proximidade (ex.: 750 m e 180 min) sao pontos de partida exploratorios e NAO possuem aprovacao cientifica da equipe.",
            "MODIS (~1 km no nadir, expandindo ate ~2 x 4,8 km fora do nadir) e VIIRS (~375 m no nadir) possuem resolucoes, geometrias de varredura e sensibilidades distintas; centros proximos NAO comprovam mesmo pixel nem mesmo incendio.",
            "Passagens orbitais ocorrem em horarios solares locais distintos que variam em UTC conforme a longitude e sofrem deriva orbital; associacoes refletem proximidade em janela temporal, nao observacao simultanea.",
            "NASA FIRMS encerrou a serie VIIRS S-NPP em 2026-11-02; series recentes dependem de NOAA-20/NOAA-21 sem calibracao automatica entre plataformas.",
            "Relacoes candidatas N:M sao preservadas sem atribuicao exclusiva 1:1; FRP original (MW) nao e somada, convertida nem preenchida com zero quando ausente.",
            "Classes de confianca MODIS (numerica 0-100) e VIIRS (categorica l/n/h) nao sao cientificamente equivalentes; valores nativos sao preservados em confidenceRaw.",
            "Nenhuma classificacao derivada (incendio confirmado ou falso positivo) e avaliada sem metodologia cientifica aprovada.",
            "Ausencia de deteccoes ou de pares candidatos nao comprova ausencia de fogo nem cobertura orbital continua (nuvens, lacunas de passagem ou recorte de ingestao).",
            "Consultas grandes estao sujeitas a amostragem MODIS (modisSampleCap) e truncamento de lista (associationsLimit), sinalizados explicitamente."
    );

    private final DetectionRepository repo;
    private final TransactionTemplate readTx;
    private final long harmonizeTimeoutMs;

    public HarmonizationService(
            DetectionRepository repo,
            PlatformTransactionManager txManager,
            @Value("${fireatlas.limits.harmonize-timeout-ms:10000}") long harmonizeTimeoutMs) {
        if (harmonizeTimeoutMs <= 0) {
            throw new IllegalArgumentException("harmonize-timeout-ms deve ser positivo");
        }
        this.repo = repo;
        this.harmonizeTimeoutMs = harmonizeTimeoutMs;
        this.readTx = new TransactionTemplate(txManager);
        this.readTx.setReadOnly(true);
        this.readTx.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
    }

    public Dto.HarmonizationResponse harmonize(
            BBox bbox, LocalDate start, LocalDate end,
            int maxDistanceMeters, int maxMinutes, int modisSampleCap,
            boolean includeAssociations, int associationsLimit) {

        DetectionRepository.HarmonizationQueryResult data = readTx.execute(status -> {
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(harmonizeTimeoutMs);
            repo.setTransactionStatementTimeoutMs(harmonizeTimeoutMs);
            DetectionRepository.HarmonizationWindowTotals totals = repo.harmonizationWindowTotals(bbox, start, end);
            long remainingMs = remainingBudgetMs(deadlineNanos);
            if (remainingMs <= 0) {
                throw new QueryTimeoutException(
                        "Orcamento de tempo SQL (" + harmonizeTimeoutMs + " ms) esgotado na contagem da janela.");
            }
            repo.setTransactionStatementTimeoutMs(remainingMs);
            return repo.findExploratoryAssociations(
                    totals,
                    bbox, start, end,
                    maxDistanceMeters, maxMinutes, modisSampleCap,
                    includeAssociations, associationsLimit);
        });
        if (data == null) {
            throw new IllegalStateException("Falha ao obter resultado transacional de correspondencia");
        }

        boolean modisSampled = data.modisTotalInWindow() > modisSampleCap;
        long assocReturned = data.associations() == null ? 0 : data.associations().size();

        String correspondenceMode;
        String availabilityStatus;
        Double matchRate;
        String message;

        if (data.modisTotalInWindow() == 0 && data.viirsTotalInWindow() == 0) {
            correspondenceMode = CORRESPONDENCE_NOT_COMPUTABLE;
            availabilityStatus = "NO_DETECTIONS";
            matchRate = null;
            message = "Nenhuma deteccao MODIS ou VIIRS encontrada para os filtros selecionados "
                    + "(ausencia de registros nao comprova ausencia de fogo ou cobertura orbital).";
        } else if (data.modisTotalInWindow() == 0) {
            correspondenceMode = CORRESPONDENCE_NOT_COMPUTABLE;
            availabilityStatus = "VIIRS_ONLY";
            matchRate = null;
            message = "Nenhuma deteccao MODIS encontrada para os filtros selecionados "
                    + "(existem deteccoes apenas de VIIRS; pareamento cruzado nao computavel).";
        } else if (data.viirsTotalInWindow() == 0 && data.candidateAssociationsTotal() == 0) {
            correspondenceMode = CORRESPONDENCE_NOT_COMPUTABLE;
            availabilityStatus = "MODIS_ONLY";
            matchRate = null;
            message = "Nenhuma deteccao VIIRS encontrada para os filtros selecionados "
                    + "(existem deteccoes apenas de MODIS; pareamento cruzado nao computavel).";
        } else if (data.viirsTotalInWindow() == 0) {
            correspondenceMode = CORRESPONDENCE_EXPLORATORY;
            availabilityStatus = "MODIS_IN_WINDOW_WITH_BOUNDARY_VIIRS_CANDIDATES";
            matchRate = data.modisWithCandidateMatch() / (double) data.modisEvaluated();
            message = null;
        } else if (data.candidateAssociationsTotal() == 0) {
            correspondenceMode = CORRESPONDENCE_EXPLORATORY;
            availabilityStatus = "BOTH_SENSORS_NO_CANDIDATE_PAIRS";
            matchRate = 0.0;
            message = "Nenhum par candidato MODIS-VIIRS encontrado dentro dos criterios espaco-temporais "
                    + "informados (ausencia de pares nao indica ausencia de incendio).";
        } else {
            correspondenceMode = CORRESPONDENCE_EXPLORATORY;
            availabilityStatus = "BOTH_SENSORS_WITH_CANDIDATE_PAIRS";
            matchRate = data.modisWithCandidateMatch() / (double) data.modisEvaluated();
            message = null;
        }

        List<String> reasons = buildReasons(
                data, modisSampled, modisSampleCap, includeAssociations, associationsLimit, assocReturned, availabilityStatus);
        Map<String, Dto.HarmonizationResponse.IndicatorStatus> unavailable = buildUnavailableIndicators(matchRate);

        var params = new Dto.HarmonizationResponse.Params(
                maxDistanceMeters, maxMinutes, modisSampleCap, includeAssociations, associationsLimit);

        var result = new Dto.HarmonizationResponse.Result(
                data.modisEvaluated(),
                data.modisWithCandidateMatch(),
                matchRate,
                null,
                null,
                null,
                null,
                null,
                modisSampled
        );

        var method = new Dto.HarmonizationResponse.Method(
                "Associacao espaco-temporal exploratoria MODIS x VIIRS (v0.2 - nao aprovada cientificamente)",
                "Identifica relacoes candidatas N:M entre deteccoes MODIS e VIIRS dentro de distancia geodesica "
                        + "WGS84 inclusiva (maxDistanceMeters) e janela temporal UTC inclusiva (maxMinutes), "
                        + "preservando identificadores individuais, proveniencia, FRP original (MW ou null) e "
                        + "confianca nativa. Indicadores legados de equivalencia ou soma de FRP permanecem "
                        + "indisponiveis ate aprovacao de metodologia cientifica.",
                LIMITATIONS
        );

        var scientificState = new Dto.HarmonizationResponse.ScientificState(
                correspondenceMode,
                HARMONIZATION_NOT_APPROVED,
                false,
                CLASSIFICATION_NOT_EVALUATED,
                CONFIDENCE_SEMANTICS,
                availabilityStatus,
                reasons
        );

        var summary = new Dto.HarmonizationResponse.DescriptiveSummary(
                data.modisTotalInWindow(),
                data.viirsTotalInWindow(),
                data.modisEvaluated(),
                modisSampled,
                data.modisWithCandidateMatch(),
                data.distinctViirsMatched(),
                data.candidateAssociationsTotal(),
                assocReturned,
                data.associationsTruncated(),
                data.modisEvaluatedWithFrp(),
                data.modisEvaluatedWithoutFrp(),
                data.distinctViirsMatchedWithFrp(),
                data.distinctViirsMatchedWithoutFrp(),
                false
        );

        return new Dto.HarmonizationResponse(
                bbox.toString(),
                start,
                end,
                params,
                result,
                method,
                message,
                EXPLORATORY_METHOD_VERSION,
                scientificState,
                summary,
                data.associations(),
                unavailable
        );
    }

    private static List<String> buildReasons(
            DetectionRepository.HarmonizationQueryResult data,
            boolean modisSampled,
            int modisSampleCap,
            boolean includeAssociations,
            int associationsLimit,
            long assocReturned,
            String availabilityStatus) {
        List<String> reasons = new ArrayList<>();
        reasons.add("Harmonizacao cientifica indisponivel (METHOD_NOT_APPROVED): nao ha calibracao ou fator de conversao MODIS-VIIRS validado neste repositorio.");
        reasons.add("Classificacao derivada indisponivel (NOT_EVALUATED): proximidade espaco-temporal nao classifica incendio confirmado nem falso positivo.");
        switch (availabilityStatus) {
            case "NO_DETECTIONS" ->
                    reasons.add("Dados insuficientes (NOT_COMPUTABLE): nenhuma deteccao MODIS ou VIIRS encontrada no recorte; ausencia de registros nao prova ausencia de fogo.");
            case "VIIRS_ONLY" ->
                    reasons.add("Dados insuficientes (NOT_COMPUTABLE): apenas VIIRS presente no recorte; ausencia de MODIS impede pareamento cruzado.");
            case "MODIS_ONLY" ->
                    reasons.add("Dados insuficientes (NOT_COMPUTABLE): apenas MODIS presente no recorte e sem vizinhos VIIRS na janela; ausencia de VIIRS impede pareamento cruzado.");
            case "BOTH_SENSORS_NO_CANDIDATE_PAIRS" ->
                    reasons.add("Ambos os sensores presentes no recorte, porem 0 pares candidatos atenderam aos criterios exploratorios; ausencia de pares nao indica ausencia de incendio comum.");
            case "MODIS_IN_WINDOW_WITH_BOUNDARY_VIIRS_CANDIDATES" ->
                    reasons.add("Nenhuma deteccao VIIRS dentro do bbox/periodo exatos, mas foram encontrados candidatos VIIRS na vizinhanca de fronteira espacial/UTC.");
            default -> {
                // Ambos os sensores com pares candidatos
            }
        }
        if (modisSampled) {
            reasons.add("Amostragem MODIS aplicada de forma deterministica (acq_ts, acq_date, id): avaliados "
                    + data.modisEvaluated() + " de " + data.modisTotalInWindow()
                    + " registros MODIS (limite modisSampleCap=" + modisSampleCap + ").");
        }
        if (!includeAssociations) {
            reasons.add("Lista individual de associacoes omitida por parametro (includeAssociations=false).");
        } else if (data.associationsTruncated()) {
            reasons.add("Lista de associacoes truncada de forma deterministica: retornados "
                    + assocReturned + " de " + data.candidateAssociationsTotal()
                    + " pares candidatos (limite associationsLimit=" + associationsLimit + ").");
        }
        if (data.modisEvaluatedWithoutFrp() > 0 || data.distinctViirsMatchedWithoutFrp() > 0) {
            reasons.add("Existem deteccoes com FRP ausente (null) preservadas integralmente sem conversao para zero.");
        }
        if (data.modisEvaluated() > 0 && data.modisEvaluatedWithFrp() == 0) {
            reasons.add("Dados insuficientes de FRP: nenhuma deteccao MODIS avaliada possui FRP disponivel neste recorte.");
        }
        if (data.legacyUnverifiedSemanticHashDetected()) {
            reasons.add("Existem registros com assinatura semantica historica pre-V6 nao verificada (LEGACY_UNVERIFIED): sourceSemanticHash permanece null ate comprovacao ou reingestao, sem conversao automatica.");
        }
        if (data.upgradedLegacySemanticHashDetected()) {
            reasons.add("Parte dos registros teve o hash semantico atualizado de assinatura historica (UPDATED_FROM_LEGACY) apos retransmissao integralmente comprovada, preservando id e valores originais.");
        }
        if (data.incompleteProvenanceDetected()) {
            reasons.add("Alguns registros avaliados ou associados possuem proveniencia ou metadados nativos incompletos (incluindo campos V3/V4/V6), preservados como ausentes sem preenchimento artificial.");
        }
        return reasons;
    }

    private static Map<String, Dto.HarmonizationResponse.IndicatorStatus> buildUnavailableIndicators(Double matchRate) {
        Map<String, Dto.HarmonizationResponse.IndicatorStatus> map = new LinkedHashMap<>();
        if (matchRate == null) {
            map.put("matchRate", new Dto.HarmonizationResponse.IndicatorStatus(
                    CORRESPONDENCE_NOT_COMPUTABLE,
                    "Dados insuficientes de MODIS e/ou VIIRS no recorte para calcular taxa exploratoria de correspondencia."));
        }
        map.put("meanModisFrp", new Dto.HarmonizationResponse.IndicatorStatus(
                HARMONIZATION_NOT_APPROVED,
                "Indicador legado acoplado a comparacao pareada de FRP sem metodologia cientifica validada; valores originais de FRP (MW) permanecem nas associacoes individuais."));
        map.put("meanViirsFrpSum", new Dto.HarmonizationResponse.IndicatorStatus(
                HARMONIZATION_NOT_APPROVED,
                "Soma de FRP VIIRS por pixel MODIS em relacoes N:M gera dupla contagem de deteccoes VIIRS compartilhadas, exige regra nao aprovada para FRP ausente e nao possui equivalencia fisica validada."));
        map.put("viirsToModisFrpRatio", new Dto.HarmonizationResponse.IndicatorStatus(
                HARMONIZATION_NOT_APPROVED,
                "Razao sum(FRP VIIRS)/sum(FRP MODIS) nao e fator de conversao cientificamente aprovado e sofre dupla contagem N:M, defasagem orbital e diferencas de varredura."));
        map.put("frpCorrelation", new Dto.HarmonizationResponse.IndicatorStatus(
                HARMONIZATION_NOT_APPROVED,
                "Correlacao entre FRP MODIS e soma de FRP VIIRS exige metodo validado de agregacao, sincronizacao orbital e tratamento de dupla contagem."));
        map.put("medianViirsPixelsPerModis", new Dto.HarmonizationResponse.IndicatorStatus(
                HARMONIZATION_NOT_APPROVED,
                "Raio fixo entre centroides nao representa a pegada real do pixel MODIS/VIIRS nem atribuicao exclusiva; consulte summary para contagens descritivas sem dupla contagem."));
        map.put("harmonizedFrp", new Dto.HarmonizationResponse.IndicatorStatus(
                HARMONIZATION_NOT_APPROVED,
                "Nao existe FRP harmonizada ou corrigida aprovada cientificamente; somente a FRP original em MW e exposta."));
        map.put("derivedClassification", new Dto.HarmonizationResponse.IndicatorStatus(
                CLASSIFICATION_NOT_EVALUATED,
                "Classificacao de incendio confirmado ou falso positivo nao e avaliada sem criterios cientificos definidos."));
        return map;
    }

    static long remainingBudgetMs(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return 0L;
        }
        return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
    }
}
