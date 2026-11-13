package com.fireatlas.ingestion;

import com.fireatlas.domain.Detection;
import com.fireatlas.domain.Location;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/**
 * Converte uma linha de CSV da FIRMS (MODIS ou VIIRS) em uma entidade {@link Detection}, validando os campos
 * obrigatorios (RN03). Linhas invalidas sao rejeitadas com um motivo.
 * O mapeamento e guiado pelo cabecalho, entao funciona com variacoes de colunas entre produtos.
 */
public final class DetectionRowParser {

    /** Exatamente um dos dois e nao nulo. */
    public record Result(Detection detection, String rejectReason) {
        static Result ok(Detection detection) {
            return new Result(detection, null);
        }

        static Result reject(String reason) {
            return new Result(null, reason);
        }

        public boolean accepted() {
            return detection != null;
        }
    }

    /** Data e coordenadas de uma linha (validas), mesmo que o resto da linha seja invalido. */
    public record Peek(LocalDate date, double lat, double lon) {}

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
    private final Detection.Source source;

    public DetectionRowParser(String header, String sensor, String product, String sourceFile) {
        String[] cols = header.trim().split(",");
        for (int i = 0; i < cols.length; i++) idx.put(cols[i].trim().toLowerCase(), i);
        for (String required : new String[]{"latitude", "longitude", "acq_date", "acq_time", "satellite"}) {
            if (!idx.containsKey(required)) {
                throw new IllegalArgumentException("coluna obrigatoria ausente no CSV: " + required);
            }
        }
        this.sensor = sensor;
        this.source = new Detection.Source(product, sourceFile);
    }

    /**
     * Data e coordenadas da linha, se forem validas (senao null). Permite ao carregador ignorar linhas
     * invalidas que estao fora do recorte pedido, sem conta-las como rejeicoes.
     */
    public Peek peekLocation(String line) {
        String[] f = line.split(",", -1);
        try {
            double lat = Double.parseDouble(get(f, "latitude"));
            double lon = Double.parseDouble(get(f, "longitude"));
            if (!(lat >= -90 && lat <= 90) || !(lon >= -180 && lon <= 180)) return null;
            return new Peek(LocalDate.parse(get(f, "acq_date")), lat, lon);
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

        Double frp;
        try {
            frp = optionalNumber(get(f, "frp"));
        } catch (NumberFormatException e) {
            return Result.reject(FRP_INVALIDO);
        }
        if (frp != null && !(frp >= 0)) return Result.reject(FRP_INVALIDO);

        Double brightness, t31, scan, track;
        try {
            brightness = optionalNumber(first(f, "brightness", "bright_ti4"));
            t31 = optionalNumber(first(f, "bright_t31", "bright_ti5"));
            scan = optionalNumber(get(f, "scan"));
            track = optionalNumber(get(f, "track"));
        } catch (NumberFormatException e) {
            return Result.reject(CAMPO_NUMERICO_INVALIDO);
        }

        String confRaw = get(f, "confidence");
        var instant = LocalDateTime.of(date,
                LocalTime.of(Integer.parseInt(t.substring(0, 2)), Integer.parseInt(t.substring(2, 4))))
                .toInstant(ZoneOffset.UTC);
        return Result.ok(new Detection(sensor, satellite, instant, Location.of(lat, lon),
                brightness, t31, scan, track, frp, confRaw, confidenceClass(confRaw),
                get(f, "daynight"), get(f, "version"), source));
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

    /** Vazio vira null; texto nao numerico, NaN ou infinito lancam NumberFormatException. */
    private static Double optionalNumber(String v) {
        if (v.isEmpty()) return null;
        double d = Double.parseDouble(v);
        if (Double.isNaN(d) || Double.isInfinite(d)) throw new NumberFormatException(v);
        return d;
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
