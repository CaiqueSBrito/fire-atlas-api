package com.fireatlas.ingestion.integration;


import com.fireatlas.detection.entity.Detection;
import com.fireatlas.detection.entity.Location;
import com.fireatlas.h3.policy.H3Index;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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
    public static final String H3_INVALIDO = "h3_invalido";
    public static final String H3_INCONSISTENTE = "h3_inconsistente";
    /** Colunas H3 por resolucao: h3_0 ... h3_6 (tambem h3_r6, h3_res6). */
    private static final java.util.regex.Pattern H3_RESOLUTION_COLUMN =
            java.util.regex.Pattern.compile("^h3[_-]?(?:r|res|resolution)?[_-]?([0-9]{1,2})$");

    private final Map<String, Integer> idx = new HashMap<>();
    private final H3Options h3;
    /** Coluna com o H3 na resolucao-base (null se a fonte nao traz H3). */
    private final Integer h3BaseColumn;
    /** Colunas com o H3 de resolucoes mais grossas que a base, por resolucao. */
    private final Map<Integer, Integer> h3ParentColumns = new TreeMap<>();
    private final String sensor;
    private final Detection.Source source;

    /** Padrao: le o H3 pronto da coluna "h3" da fonte. */
    public DetectionRowParser(String header, String sensor, String product, String sourceFile) {
        this(header, sensor, product, sourceFile, H3Options.DEFAULT);
    }

    public DetectionRowParser(String header, String sensor, String product, String sourceFile, H3Options h3) {
        this.h3 = h3;
        String[] cols = header.trim().split(",");
        for (int i = 0; i < cols.length; i++) idx.put(cols[i].trim().toLowerCase(), i);
        for (String required : new String[]{"latitude", "longitude", "acq_date", "acq_time", "satellite"}) {
            if (!idx.containsKey(required)) {
                throw new IllegalArgumentException("coluna obrigatoria ausente no CSV: " + required);
            }
        }
        // H3: uma coluna unica (h3) OU uma coluna por resolucao (h3_0 ... h3_6)
        Map<Integer, Integer> byResolution = new TreeMap<>();
        for (int i = 0; i < cols.length; i++) {
            var m = H3_RESOLUTION_COLUMN.matcher(cols[i].trim().toLowerCase());
            if (m.matches()) byResolution.put(Integer.parseInt(m.group(1)), i);
        }
        if (idx.containsKey(h3.column())) {
            this.h3BaseColumn = idx.get(h3.column());
        } else if (!byResolution.isEmpty()) {
            Integer baseColumn = byResolution.get(h3.baseResolution());
            if (baseColumn == null) {
                throw new IllegalArgumentException("o arquivo traz H3 nas resolucoes " + byResolution.keySet()
                        + ", mas nao na resolucao-base " + h3.baseResolution()
                        + "; ajuste H3_BASE_RESOLUTION para a resolucao mais fina do arquivo");
            }
            this.h3BaseColumn = baseColumn;
            byResolution.entrySet().stream().filter(e -> e.getKey() < h3.baseResolution())
                    .forEach(e -> h3ParentColumns.put(e.getKey(), e.getValue()));
        } else {
            this.h3BaseColumn = null;
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

        Integer fireType;
        try {
            String type = get(f, "type");
            fireType = type.isEmpty() ? null : Integer.valueOf(type);
        } catch (NumberFormatException e) {
            return Result.reject(CAMPO_NUMERICO_INVALIDO);
        }

        // O H3 vem pronto da fonte: o backend so o le (aceita hexadecimal ou inteiro) e valida; nao converte lat/lon.
        Long h3Index = null;
        String h3Raw = h3BaseColumn == null ? "" : at(f, h3BaseColumn);
        if (!h3Raw.isEmpty()) {
            try {
                h3Index = H3Index.parseIndex(h3Raw);
            } catch (IllegalArgumentException e) {
                return Result.reject(H3_INVALIDO);
            }
            int base = h3.baseResolution();
            int res = H3Index.resolution(h3Index);
            if (res < base) return Result.reject(H3_INVALIDO);
            if (res > base) h3Index = H3Index.parent(h3Index, base); // relacao hierarquica: normaliza para a base
            // colunas de resolucoes mais grossas (h3_0 ... h3_5) precisam ser o "pai" do indice-base
            for (var parentColumn : h3ParentColumns.entrySet()) {
                String raw = at(f, parentColumn.getValue());
                if (raw.isEmpty()) continue;
                long given;
                try {
                    given = H3Index.parseIndex(raw);
                } catch (IllegalArgumentException e) {
                    return Result.reject(H3_INVALIDO);
                }
                if (given != H3Index.parent(h3Index, parentColumn.getKey())) return Result.reject(H3_INCONSISTENTE);
            }
        } // sem coluna ou valor vazio: o indice fica ausente (nunca e calculado aqui)

        String confRaw = get(f, "confidence");
        var instant = LocalDateTime.of(date,
                LocalTime.of(Integer.parseInt(t.substring(0, 2)), Integer.parseInt(t.substring(2, 4))))
                .toInstant(ZoneOffset.UTC);
        return Result.ok(new Detection(sensor, satellite, get(f, "instrument"), instant,
                new Location(lat, lon, h3Index),
                brightness, t31, scan, track, frp, confRaw, confidenceClass(confRaw),
                get(f, "daynight"), get(f, "version"), fireType, source));
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

    private static String at(String[] f, int index) {
        return index < f.length ? f[index].trim() : "";
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
