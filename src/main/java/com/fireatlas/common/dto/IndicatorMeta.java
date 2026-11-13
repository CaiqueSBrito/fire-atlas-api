package com.fireatlas.common.dto;

import java.time.LocalDate;
import java.util.Map;

/**
 * Identificacao de um indicador (RN04): recorte (regiao e periodo), fonte, unidades e metodo de calculo.
 * Vai em toda resposta com estatisticas, para o cliente nunca mostrar um numero sem dizer de onde veio.
 */
public record IndicatorMeta(String region, LocalDate start, LocalDate end, String source,
                            Map<String, String> units, String method) {

    public static final String SOURCE = "NASA FIRMS: MODIS C6.1 e VIIRS (S-NPP, NOAA-20, NOAA-21), via data lake";

    /** Unidades comuns: contagens de deteccoes (focos de calor) e FRP em megawatts. */
    public static final Map<String, String> UNITS = Map.of(
            "detections", "contagem de deteccoes (focos de calor), nao de focos unicos",
            "frp", "MW (megawatts)",
            "nominalResolution", "m (metros)");

    public static IndicatorMeta of(String region, LocalDate start, LocalDate end, String method) {
        return new IndicatorMeta(region, start, end, SOURCE, UNITS, method);
    }
}
