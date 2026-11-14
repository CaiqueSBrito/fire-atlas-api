package com.fireatlas.ingestion;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;

/**
 * Converte uma linha de CSV da FIRMS (MODIS ou VIIRS) para o esquema unico "detection".
 * A validacao respeita os dominios documentados por produto (tempo UTC 0000..2359, FRP >= 0 e
 * escalas de confianca distintas para MODIS e VIIRS).
 */
public final class DetectionRowParser {

    /** Versao canonica atual de source_semantic_hash (conversao decimal -> float32/float64 direta sem duplo arredondamento). */
    public static final String SEMANTIC_HASH_VERSION = "v2-ieee754-direct";

    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponivel", e);
        }
    });

    /**
     * Registro interpretado de uma linha CSV FIRMS.
     *
     * @param sourceRowHash      SHA-256 da linha textual bruta (rastreabilidade de bytes da linha no CSV)
     * @param sourceSemanticHash SHA-256 canonico dos campos efetivamente interpretados apos mapeamento de cabecalho
     */
    public record Row(LocalDate date, double lat, double lon, String csvLine,
                      String sourceRowHash, String sourceSemanticHash) {
        public String toStagedCsvLine(long sourceLineNumber) {
            return csvLine + ',' + sourceLineNumber + ',' + sourceRowHash + ',' + sourceSemanticHash;
        }

        public String toDirectCsvLine(long sourceLineNumber) {
            return toStagedCsvLine(sourceLineNumber) + ',' + SEMANTIC_HASH_VERSION + ",CURRENT";
        }
    }

    private final Map<String, Integer> idx = new HashMap<>();
    private final String sensor;
    private final String product;
    private final String sourceFile;

    public DetectionRowParser(String header, String sensor, String product, String sourceFile) {
        String[] cols = header.trim().split(",");
        for (int i = 0; i < cols.length; i++) idx.put(cols[i].trim().toLowerCase(Locale.ROOT), i);
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
     * Retorna null para linhas malformadas (contadas como descartadas pelo chamador).
     * Politica para confianca: valor vazio ("") e preservado como NULL sem fabricacao;
     * valor preenchido fora da escala documentada do sensor rejeita o registro como malformado,
     * impedindo persistencia de tokens corrompidos ou coercao indevida para alta/baixa confianca.
     */
    public Row parse(String line) {
        String[] f = line.split(",", -1);
        try {
            double lat = Double.parseDouble(get(f, "latitude"));
            double lon = Double.parseDouble(get(f, "longitude"));
            if (!Double.isFinite(lat) || lat < -90.0 || lat > 90.0
                    || !Double.isFinite(lon) || lon < -180.0 || lon > 180.0) {
                return null;
            }
            LocalDate date = LocalDate.parse(get(f, "acq_date"));
            String t = pad4Strict(get(f, "acq_time"));
            if (t == null) return null;
            int hh = Integer.parseInt(t.substring(0, 2));
            int mm = Integer.parseInt(t.substring(2, 4));
            if (hh > 23 || mm > 59) return null;
            LocalTime time = LocalTime.of(hh, mm);
            ZonedDateTime.of(date, time, ZoneOffset.UTC).toInstant();
            String ts = date + "T" + t.substring(0, 2) + ":" + t.substring(2, 4) + ":00Z";

            String brightnessRaw = first(f, "brightness", "bright_ti4");
            String brightT31Raw = first(f, "bright_t31", "bright_ti5");
            String scanRaw = get(f, "scan");
            String trackRaw = get(f, "track");
            String frpRaw = get(f, "frp");

            String brightness = optionalFiniteReal(brightnessRaw);
            String brightT31 = optionalFiniteReal(brightT31Raw);
            String scan = optionalFiniteReal(scanRaw);
            String track = optionalFiniteReal(trackRaw);
            String frp = optionalNonNegativeFiniteReal(frpRaw);

            String confRaw = get(f, "confidence");
            String confClass = "";
            if (!confRaw.isEmpty()) {
                confClass = confidenceClass(sensor, confRaw);
                if (confClass.isEmpty()) {
                    return null;
                }
            }

            String satellite = get(f, "satellite");
            String version = get(f, "version");
            String daynight = get(f, "daynight");
            if (daynight.length() > 1) daynight = "";
            StringBuilder sb = new StringBuilder(160);
            sb.append(csvCell(sensor)).append(',')
                    .append(csvCell(satellite)).append(',')
                    .append(date).append(',')
                    .append(ts).append(',')
                    .append(lat).append(',')
                    .append(lon).append(',')
                    .append(brightness).append(',')
                    .append(brightT31).append(',')
                    .append(scan).append(',')
                    .append(track).append(',')
                    .append(frp).append(',')
                    .append(csvCell(confRaw)).append(',')
                    .append(csvCell(confClass)).append(',')
                    .append(csvCell(daynight)).append(',')
                    .append(csvCell(version)).append(',')
                    .append(csvCell(product)).append(',')
                    .append(csvCell(sourceFile));

            String rawHash = sha256Hex(line);
            String semanticHash = computeSemanticHash(
                    sensor, satellite, date.toString(), ts,
                    canonicalDouble(lat), canonicalDouble(lon),
                    canonicalReal(brightness), canonicalReal(brightT31),
                    canonicalReal(scan), canonicalReal(track), canonicalReal(frp),
                    confRaw, confClass, daynight, version, product, sourceFile);
            return new Row(date, lat, lon, sb.toString(), rawHash, semanticHash);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String optionalFiniteReal(String raw) {
        if (raw == null || raw.isEmpty()) return "";
        float fv = parseDirectFiniteReal(raw);
        return Float.isFinite(fv) ? raw : "";
    }

    private static String optionalNonNegativeFiniteReal(String raw) {
        if (raw == null || raw.isEmpty()) return "";
        float fv = parseDirectFiniteReal(raw);
        if (fv < 0.0f || raw.startsWith("-")) {
            throw new IllegalArgumentException("FRP negativa ou nao finita");
        }
        return raw;
    }

    private static float parseDirectFiniteReal(String raw) {
        float fv = Float.parseFloat(raw);
        if (!Float.isFinite(fv)) {
            throw new IllegalArgumentException("valor real nao finito: " + raw);
        }
        if (fv == 0.0f && new BigDecimal(raw).signum() != 0) {
            throw new IllegalArgumentException("valor real abaixo da faixa representavel (underflow): " + raw);
        }
        return fv;
    }

    private static String csvCell(String s) {
        if (s == null || s.isEmpty()) return "";
        if (s.indexOf(',') < 0 && s.indexOf('"') < 0 && s.indexOf('\n') < 0 && s.indexOf('\r') < 0) {
            return s;
        }
        return '"' + s.replace("\"", "\"\"") + '"';
    }

    /**
     * Classe de confianca auxiliar (l/n/h) especifica por sensor, sem equivalencia cientifica:
     * MODIS aceita somente inteiro documentado 0..100 (0-29 = l, 30-79 = n, 80-100 = h);
     * VIIRS aceita somente categorias documentadas l/n/h ou low/nominal/high.
     * Valores desconhecidos ou fora de escala retornam string vazia (indisponivel), nunca 'h'/'l'.
     */
    public static String confidenceClass(String sensor, String raw) {
        if (sensor == null || raw == null || raw.isBlank()) return "";
        String s = sensor.trim().toUpperCase(Locale.ROOT);
        String r = raw.trim();
        if ("MODIS".equals(s)) {
            if (r.isEmpty() || r.length() > 3 || !allAsciiDigits(r)) {
                return "";
            }
            int v = Integer.parseInt(r);
            if (v > 100) {
                return "";
            }
            return v >= 80 ? "h" : v >= 30 ? "n" : "l";
        }
        if ("VIIRS".equals(s)) {
            return switch (r.toLowerCase(Locale.ROOT)) {
                case "l", "low" -> "l";
                case "n", "nominal" -> "n";
                case "h", "high" -> "h";
                default -> "";
            };
        }
        return "";
    }

    static String pad4(String t) {
        String padded = pad4Strict(t);
        if (padded == null) {
            throw new IllegalArgumentException("acq_time invalido: " + t);
        }
        return padded;
    }

    private static String pad4Strict(String t) {
        if (t == null) return null;
        String s = t.trim();
        if (s.isEmpty() || s.length() > 4 || !allAsciiDigits(s)) {
            return null;
        }
        return s.length() == 4 ? s : "0".repeat(4 - s.length()) + s;
    }

    private static boolean allAsciiDigits(String s) {
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch < '0' || ch > '9') return false;
        }
        return true;
    }

    private static String canonicalDouble(double v) {
        return Double.toString(v == 0.0 ? 0.0 : v);
    }

    private static String canonicalReal(String raw) {
        if (raw == null || raw.isEmpty()) return "";
        float fv = parseDirectFiniteReal(raw);
        return Float.toString(fv == 0.0f ? 0.0f : fv);
    }

    private static String computeSemanticHash(String... fields) {
        StringBuilder canonical = new StringBuilder(192);
        for (String field : fields) {
            String val = field == null ? "" : field;
            canonical.append(val.length()).append(':').append(val).append('|');
        }
        return sha256Hex(canonical.toString());
    }

    static String sha256Hex(String value) {
        MessageDigest md = SHA256.get();
        md.reset();
        return HexFormat.of().formatHex(md.digest(value.getBytes(StandardCharsets.UTF_8)));
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
