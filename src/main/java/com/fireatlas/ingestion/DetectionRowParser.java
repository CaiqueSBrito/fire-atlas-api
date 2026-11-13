package com.fireatlas.ingestion;

import com.fireatlas.domain.H3Index;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/**
 * Converte uma linha de CSV da FIRMS (MODIS ou VIIRS) para uma linha CSV do esquema unico "detection",
 * validando os campos obrigatorios (RN03). Linhas invalidas sao rejeitadas com um motivo.
 * O mapeamento e guiado pelo cabecalho, entao funciona com variacoes de colunas entre produtos.
 */
public final class DetectionRowParser {

    public record Row(LocalDate date, double lat, double lon, String csvLine) {}

    /** Exatamente um dos dois e nao nulo. */
    public record Result(Row row, String rejectReason) {
        static Result ok(Row row) {
            return new Result(row, null);
        }

        static Result reject(String reason) {
            return new Result(null, reason);
        }

        public boolean accepted() {
            return row != null;
        }
    }

    // Motivos de rejeicao (estaveis: aparecem em logs, ingestion_log e testes).
    public static final String LATITUDE_INVALIDA = "latitude_invalida";
    public static final String LONGITUDE_INVALIDA = "longitude_invalida";
    public static final String DATA_INVALIDA = "data_invalida";
    public static final String HORA_INVALIDA = "hora_invalida";
    public static final String SATELITE_AUSENTE = "satelite_ausente";
    public static final String FRP_INVALIDO = "frp_invalido";
    public static final String CAMPO_NUMERICO_INVALIDO = "campo_numerico_invalido";

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

    /**
     * Data e coordenadas da linha, se forem validas (senao null). Permite ao carregador ignorar linhas
     * invalidas que estao fora do recorte pedido, sem contá-las como rejeicoes.
     */
    public Row peekLocation(String line) {
        String[] f = line.split(",", -1);
        try {
            double lat = Double.parseDouble(get(f, "latitude"));
            double lon = Double.parseDouble(get(f, "longitude"));
            if (!(lat >= -90 && lat <= 90) || !(lon >= -180 && lon <= 180)) return null;
            return new Row(LocalDate.parse(get(f, "acq_date")), lat, lon, null);
        } catch (RuntimeException e) {
            return null;
        }
    }

    public Result parse(String line) {
        String[] f = line.split(",", -1);

        double lat;
        try {
            lat = Double.parseDouble(get(f, "latitude"));
        } catch (NumberFormatException e) {
            return Result.reject(LATITUDE_INVALIDA);
        }
        if (!(lat >= -90 && lat <= 90)) return Result.reject(LATITUDE_INVALIDA);

        double lon;
        try {
            lon = Double.parseDouble(get(f, "longitude"));
        } catch (NumberFormatException e) {
            return Result.reject(LONGITUDE_INVALIDA);
        }
        if (!(lon >= -180 && lon <= 180)) return Result.reject(LONGITUDE_INVALIDA);

        LocalDate date;
        try {
            date = LocalDate.parse(get(f, "acq_date"));
        } catch (RuntimeException e) {
            return Result.reject(DATA_INVALIDA);
        }

        String t = pad4(get(f, "acq_time"));
        if (!t.matches("\\d{4}") || Integer.parseInt(t.substring(0, 2)) > 23
                || Integer.parseInt(t.substring(2, 4)) > 59) {
            return Result.reject(HORA_INVALIDA);
        }

        String satellite = get(f, "satellite");
        if (satellite.isEmpty()) return Result.reject(SATELITE_AUSENTE);

        String frp = get(f, "frp");
        if (!frp.isEmpty()) {
            try {
                if (!(Double.parseDouble(frp) >= 0)) return Result.reject(FRP_INVALIDO);
            } catch (NumberFormatException e) {
                return Result.reject(FRP_INVALIDO);
            }
        }

        String brightness = first(f, "brightness", "bright_ti4");
        String t31 = first(f, "bright_t31", "bright_ti5");
        String scan = get(f, "scan");
        String track = get(f, "track");
        for (String v : new String[]{brightness, t31, scan, track}) {
            if (!isOptionalNumber(v)) return Result.reject(CAMPO_NUMERICO_INVALIDO);
        }

        String ts = date + "T" + t.substring(0, 2) + ":" + t.substring(2, 4) + ":00Z";
        String confRaw = get(f, "confidence");
        StringBuilder sb = new StringBuilder(160);
        sb.append(sensor).append(',')
                .append(satellite).append(',')
                .append(date).append(',')
                .append(ts).append(',')
                .append(lat).append(',')
                .append(lon).append(',')
                .append(brightness).append(',')
                .append(t31).append(',')
                .append(scan).append(',')
                .append(track).append(',')
                .append(frp).append(',')
                .append(confRaw).append(',')
                .append(confidenceClass(confRaw)).append(',')
                .append(get(f, "daynight")).append(',')
                .append(get(f, "version")).append(',')
                .append(product).append(',')
                .append(sourceFile).append(',')
                .append(H3Index.cell(lat, lon));
        return Result.ok(new Row(date, lat, lon, sb.toString()));
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
            try {
                int v = (int) Math.round(Double.parseDouble(r));
                return v >= 80 ? "h" : v >= 30 ? "n" : "l";
            } catch (NumberFormatException e) {
                return "";
            }
        }
        return (c == 'l' || c == 'n' || c == 'h') ? String.valueOf(c) : "";
    }

    static String pad4(String t) {
        String s = t.trim();
        return s.length() >= 4 ? s : "0".repeat(4 - s.length()) + s;
    }

    private static boolean isOptionalNumber(String v) {
        if (v.isEmpty()) return true;
        try {
            return !Double.isNaN(Double.parseDouble(v));
        } catch (NumberFormatException e) {
            return false;
        }
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
