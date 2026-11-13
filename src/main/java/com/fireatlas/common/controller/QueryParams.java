package com.fireatlas.common.controller;

import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.common.policy.ParameterError;
import com.fireatlas.region.entity.Region;
import com.fireatlas.sensor.policy.Sensors;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Parametros comuns da requisicao, validados: regiao (bbox ou celulas H3), periodo e sensores. */
public record QueryParams(Region region, LocalDate start, LocalDate end, List<String> sensors) {

    /** Valida tudo e, se houver erro, devolve TODOS os parametros rejeitados de uma vez (CA04). */
    public static QueryParams of(String bbox, String cells, LocalDate start, LocalDate end, String sensor,
                                 int maxDays) {
        return of(bbox, cells, start, end, sensor, maxDays, "start", "end");
    }

    /** Igual a {@link #of}, com os nomes dos parametros de periodo (ex.: startA/endA) usados nos erros. */
    public static QueryParams of(String bbox, String cells, LocalDate start, LocalDate end, String sensor,
                                 int maxDays, String startName, String endName) {
        List<ParameterError> errors = new ArrayList<>();
        if (start == null) errors.add(new ParameterError(startName, "parametro " + startName + " (YYYY-MM-DD) obrigatorio"));
        if (end == null) errors.add(new ParameterError(endName, "parametro " + endName + " (YYYY-MM-DD) obrigatorio"));
        if (start != null && end != null) {
            if (end.isBefore(start)) {
                errors.add(new ParameterError(endName, endName + " deve ser igual ou posterior a " + startName));
            } else if (ChronoUnit.DAYS.between(start, end) + 1 > maxDays) {
                errors.add(new ParameterError(endName, "periodo maximo para esta consulta: " + maxDays + " dias"));
            }
        }
        Region region = null;
        try {
            region = Region.of(bbox, cells);
        } catch (InvalidParameterException e) {
            errors.addAll(e.errors());
        }
        List<String> sensors = null;
        try {
            sensors = Sensors.parse(sensor);
        } catch (InvalidParameterException e) {
            errors.addAll(e.errors());
        }
        if (!errors.isEmpty()) throw new InvalidParameterException(errors);
        return new QueryParams(region, start, end, sensors);
    }

    /** Junta listas de erros sem repetir o mesmo parametro/motivo (ex.: regiao validada para dois periodos). */
    public static List<ParameterError> merge(List<ParameterError> first, List<ParameterError> second) {
        Set<ParameterError> all = new LinkedHashSet<>(first);
        all.addAll(second);
        return new ArrayList<>(all);
    }

    /** Executa a validacao e devolve os erros (lista vazia se tudo certo). */
    public static List<ParameterError> errorsOf(Runnable validation) {
        try {
            validation.run();
            return List.of();
        } catch (InvalidParameterException e) {
            return e.errors();
        }
    }
}
