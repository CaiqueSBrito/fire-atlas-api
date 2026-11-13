package com.fireatlas.domain;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Deteccao de foco de calor (MODIS ou VIIRS) com proveniencia. Representa a medicao original: nenhum campo
 * derivado substitui um valor medido (RN05); o indice H3 em {@link Location} e apenas derivado das coordenadas.
 */
public record Detection(String sensor, String satellite, Instant acquiredAt, Location location,
                        Double brightness, Double brightT31, Double scan, Double track, Double frp,
                        String confidenceRaw, String confidenceClass, String dayNight, String version,
                        Source source) {

    /** Proveniencia (RN04): produto e arquivo de origem no data lake. */
    public record Source(String product, String file) {
        public Source {
            if (product == null || product.isBlank() || file == null || file.isBlank()) {
                throw new IllegalArgumentException("produto e arquivo de origem sao obrigatorios");
            }
        }
    }

    public Detection {
        if (sensor == null || sensor.isBlank()) throw new IllegalArgumentException("sensor obrigatorio");
        if (satellite == null || satellite.isBlank()) throw new IllegalArgumentException("satelite obrigatorio");
        if (acquiredAt == null) throw new IllegalArgumentException("instante de aquisicao obrigatorio");
        if (location == null) throw new IllegalArgumentException("localizacao obrigatoria");
        if (source == null) throw new IllegalArgumentException("origem obrigatoria");
        if (frp != null && !(frp >= 0)) throw new IllegalArgumentException("FRP deve ser >= 0: " + frp);
    }

    public LocalDate acqDate() {
        return acquiredAt.atOffset(ZoneOffset.UTC).toLocalDate();
    }

    /** Linha CSV para o COPY em detection (mesma ordem das colunas de DetectionCsvLoader). */
    public String toCopyLine() {
        StringBuilder sb = new StringBuilder(176);
        sb.append(csv(sensor)).append(',')
                .append(csv(satellite)).append(',')
                .append(acqDate()).append(',')
                .append(acquiredAt).append(',')
                .append(location.lat()).append(',')
                .append(location.lon()).append(',')
                .append(num(brightness)).append(',')
                .append(num(brightT31)).append(',')
                .append(num(scan)).append(',')
                .append(num(track)).append(',')
                .append(num(frp)).append(',')
                .append(csv(confidenceRaw)).append(',')
                .append(csv(confidenceClass)).append(',')
                .append(csv(dayNight)).append(',')
                .append(csv(version)).append(',')
                .append(csv(source.product())).append(',')
                .append(csv(source.file())).append(',')
                .append(location.h3());
        return sb.toString();
    }

    private static String num(Double v) {
        return v == null ? "" : v.toString();
    }

    /** Valor de campo CSV: vazio vira NULL no COPY; aspas apenas quando necessario. */
    private static String csv(String v) {
        if (v == null || v.isEmpty()) return "";
        if (v.indexOf(',') >= 0 || v.indexOf('"') >= 0 || v.indexOf('\n') >= 0 || v.indexOf('\r') >= 0) {
            return '"' + v.replace("\"", "\"\"") + '"';
        }
        return v;
    }
}
