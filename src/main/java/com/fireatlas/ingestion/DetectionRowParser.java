package com.fireatlas.ingestion;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/**
 * Converte uma linha de CSV da FIRMS (MODIS ou VIIRS) para uma linha CSV do esquema unico "detection".
 * O mapeamento e guiado pelo cabecalho, entao funciona com variacoes de colunas entre produtos.
 */
public final class DetectionRowParser {

    public record Row(LocalDate date, double lat, double lon, String csvLine) {}

    private final Map<String, Integer> idx = new HashMap<>();
    private final String sensor;
    private final String product;
    private final String sourceFile;

    public DetectionRowParser(String header, String sensor, String product, String sourceFile) {
        String[] cols = header.trim().split(",");
        for (int i = 0; i < cols.length; i++) idx.put(cols[i].trim().toLowerCase(), i);
        for (String required : new String[]{"latitude", "longitude", "acq_date", "acq_time", "satellite"}) {
            if (!idx.containsKey(required)) {
                throw new IllegalArgumentException("coluna obrigatoria ausente no CSV: " + required);
            }
        }
        this.sensor = sensor;
        this.product = product;
        this.sourceFile = sourceFile;
    }

    /** Retorna null para linhas malformadas (contadas como descartadas pelo chamador). */
    public Row parse(String line) {
        String[] f = line.split(",", -1);
        try {
            double lat = Double.parseDouble(get(f, "latitude"));
            double lon = Double.parseDouble(get(f, "longitude"));
            LocalDate date = LocalDate.parse(get(f, "acq_date"));
            String t = pad4(get(f, "acq_time"));
            String ts = date + "T" + t.substring(0, 2) + ":" + t.substring(2, 4) + ":00Z";
            String confRaw = get(f, "confidence");
            StringBuilder sb = new StringBuilder(160);
            sb.append(sensor).append(',')
                    .append(get(f, "satellite")).append(',')
                    .append(date).append(',')
                    .append(ts).append(',')
                    .append(lat).append(',')
                    .append(lon).append(',')
                    .append(first(f, "brightness", "bright_ti4")).append(',')
                    .append(first(f, "bright_t31", "bright_ti5")).append(',')
                    .append(get(f, "scan")).append(',')
                    .append(get(f, "track")).append(',')
                    .append(get(f, "frp")).append(',')
                    .append(confRaw).append(',')
                    .append(confidenceClass(confRaw)).append(',')
                    .append(get(f, "daynight")).append(',')
                    .append(get(f, "version")).append(',')
                    .append(product).append(',')
                    .append(sourceFile);
            return new Row(date, lat, lon, sb.toString());
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Classe de confianca harmonizada (l/n/h).
     * MODIS: numerico 0-100 (FIRMS: baixa 0-29, nominal 30-79, alta 80-100). VIIRS: l/n/h (ou low/nominal/high).
     */
    public static String confidenceClass(String raw) {
        if (raw == null || raw.isBlank()) return "";
        String r = raw.trim().toLowerCase();
        char c = r.charAt(0);
        if (Character.isDigit(c)) {
            int v = (int) Math.round(Double.parseDouble(r));
            return v >= 80 ? "h" : v >= 30 ? "n" : "l";
        }
        return (c == 'l' || c == 'n' || c == 'h') ? String.valueOf(c) : "";
    }

    static String pad4(String t) {
        String s = t.trim();
        return s.length() >= 4 ? s : "0".repeat(4 - s.length()) + s;
    }

    private String get(String[] f, String col) {
        Integer i = idx.get(col);
        if (i == null || i >= f.length) return "";
        return f[i].trim();
    }

    private String first(String[] f, String a, String b) {
        String v = get(f, a);
        return v.isEmpty() ? get(f, b) : v;
    }
}
