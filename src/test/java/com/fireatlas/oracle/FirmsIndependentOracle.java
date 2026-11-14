package com.fireatlas.oracle;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Oraculo cientifico-geometrico independente (sem dependencias de Spring, PostGIS ou codigo de producao).
 *
 * <p>Execucao reproduzivel direta via JDK 21+:
 * <pre>
 * java src/test/java/com/fireatlas/oracle/FirmsIndependentOracle.java
 * </pre>
 *
 * <p>Verifica hashes SHA-256 das fixtures imutaveis NASA FIRMS, diferencia explicitamente fluxo NRT vs Standard,
 * e recalcula por formula geodesica inversa de Vincenty (WGS84) e aritmetica UTC os 58 pares candidatos
 * MODIS x VIIRS (metodo exploratorio NAO aprovado cientificamente).
 */
public final class FirmsIndependentOracle {

    public static final Path FIXTURE_DIR = Path.of("src", "test", "resources", "fixtures", "firms_brazil_2026_11_12_13");
    public static final double WGS84_A = 6378137.0;
    public static final double WGS84_F = 1.0 / 298.257223563;
    public static final double WGS84_B = WGS84_A * (1.0 - WGS84_F);
    public static final double VINCENTY_CONVERGENCE_RAD = 1e-12;
    public static final double POSTGIS_COMPARISON_TOLERANCE_METERS = 0.50;

    public static final double DEFAULT_MAX_DISTANCE_METERS = 750.0;
    public static final long DEFAULT_MAX_SECONDS = 180L * 60L;

    public enum ProcessingStream {
        URT("Ultra Real-Time (leitura direta/antenas regionais, menor latencia, provisorio)"),
        RT("Real-Time (leitura direta, latencia tipica < 1h, provisorio)"),
        NRT("Near Real-Time (provisorio, janela movel, sujeito a revisao de geolocalizacao/calibracao)"),
        STANDARD("Standard / Science-Quality Archive (processamento definitivo por produto/ano)");

        private static final Pattern URT_PATTERN = Pattern.compile("^(?:6(?:\\.[01])?|2(?:\\.0)?|1(?:\\.0)?)URT$");
        private static final Pattern NRT_PATTERN = Pattern.compile("^(?:6(?:\\.[01])?|2(?:\\.0)?|1(?:\\.0)?)NRT$");
        private static final Pattern RT_PATTERN = Pattern.compile("^(?:6(?:\\.[01])?|2(?:\\.0)?|1(?:\\.0)?)RT$");
        private static final Pattern STANDARD_PATTERN = Pattern.compile("^(?:6(?:\\.[01])?|2(?:\\.0)?|1(?:\\.0)?)$");

        private final String description;

        ProcessingStream(String description) {
            this.description = description;
        }

        public String description() {
            return description;
        }

        public static ProcessingStream fromFirmsVersion(String version) {
            if (version == null || version.isBlank()) {
                throw new IllegalArgumentException("Versao FIRMS ausente");
            }
            String normalized = version.trim().toUpperCase(Locale.ROOT);
            if (URT_PATTERN.matcher(normalized).matches()) {
                return URT;
            }
            if (NRT_PATTERN.matcher(normalized).matches()) {
                return NRT;
            }
            if (RT_PATTERN.matcher(normalized).matches()) {
                return RT;
            }
            if (STANDARD_PATTERN.matcher(normalized).matches()) {
                return STANDARD;
            }
            throw new IllegalArgumentException(
                    "Versao/fluxo FIRMS desconhecido ou nao documentado (nao presumindo STANDARD): '" + version + "'");
        }
    }

    public record FixtureSourceSpec(
            String fileName,
            String sensor,
            String product,
            String expectedVersion,
            ProcessingStream expectedStream,
            String officialUrl,
            String expectedSha256,
            int expectedRows
    ) {}

    public static final List<FixtureSourceSpec> CURATED_SOURCES = List.of(
            new FixtureSourceSpec(
                    "MODIS_C6_1_South_America_24h.csv",
                    "MODIS",
                    "MODIS_C6.1",
                    "6.1NRT",
                    ProcessingStream.NRT,
                    "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_South_America_24h.csv",
                    "ce18f9c4d99a460aa9d0100b270d55250537171e7f1cc70eddf4e2b176ee17ed",
                    31
            ),
            new FixtureSourceSpec(
                    "J1_VIIRS_C2_South_America_24h.csv",
                    "VIIRS",
                    "VIIRS_NOAA-20",
                    "2.0NRT",
                    ProcessingStream.NRT,
                    "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_South_America_24h.csv",
                    "200e1cc6f8e59c0d11384b11ffa1085c807c72801efaaf047942ff7bf8aaa68f",
                    82
            ),
            new FixtureSourceSpec(
                    "J2_VIIRS_C2_South_America_24h.csv",
                    "VIIRS",
                    "VIIRS_NOAA-21",
                    "2.0NRT",
                    ProcessingStream.NRT,
                    "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/csv/J2_VIIRS_C2_South_America_24h.csv",
                    "0d152e191e8a5a98369f51c9475c424ba39d1c19fb8ed5a600ecd9b2ed2c7d5b",
                    83
            )
    );

    public record DetectionRecord(
            String fileName,
            long lineNumber,
            String sensor,
            String product,
            String satellite,
            String version,
            ProcessingStream stream,
            Instant acquiredAt,
            double lat,
            double lon,
            Double frp,
            String confidenceRaw,
            String dayNight
    ) {
        public String uniqueRecordKey() {
            return fileName + "#L" + lineNumber;
        }
    }

    public record ExpectedPair(
            DetectionRecord modis,
            DetectionRecord viirs,
            double vincentyDistanceMeters,
            long timeOffsetSeconds
    ) {}

    public record VerifiedSource(
            FixtureSourceSpec spec,
            String actualSha256,
            int parsedRows
    ) {}

    public record OracleReport(
            List<VerifiedSource> sources,
            int modisTotal,
            int viirsTotal,
            int modisMatched,
            int distinctViirsMatched,
            List<ExpectedPair> pairs
    ) {}

    private FirmsIndependentOracle() {}

    public static void main(String[] args) throws IOException {
        Path dir = args.length > 0 ? Path.of(args[0]) : FIXTURE_DIR;
        boolean verbose = args.length > 1 && "--verbose".equals(args[1]);
        OracleReport report = verifyAndCompute(dir, DEFAULT_MAX_DISTANCE_METERS, DEFAULT_MAX_SECONDS);

        System.out.println("=== NASA-018 Independent FIRMS Exploratory Oracle ===");
        System.out.println("Fixture directory: " + dir.toAbsolutePath());
        System.out.println("Geodesic method: WGS84 Vincenty inverse (convergence=" + VINCENTY_CONVERGENCE_RAD
                + " rad, comparison tolerance vs PostGIS=" + POSTGIS_COMPARISON_TOLERANCE_METERS + " m)");
        System.out.println("Scientific status: METHOD_NOT_APPROVED (exploratory geometric/temporal check only)");
        System.out.println();
        for (VerifiedSource s : report.sources()) {
            System.out.printf(Locale.ROOT,
                    "- %s | product=%s version=%s stream=%s | rows=%d | sha256=%s [VERIFIED]%n  url=%s%n",
                    s.spec().fileName(), s.spec().product(), s.spec().expectedVersion(),
                    s.spec().expectedStream().name(), s.parsedRows(), s.actualSha256(), s.spec().officialUrl());
        }
        System.out.println();
        System.out.printf(Locale.ROOT,
                "Expected counts (<= %.1f m, |dt| <= %d s): MODIS=%d, VIIRS=%d, MODIS_matched=%d, VIIRS_distinct=%d, candidate_pairs=%d%n",
                DEFAULT_MAX_DISTANCE_METERS, DEFAULT_MAX_SECONDS,
                report.modisTotal(), report.viirsTotal(), report.modisMatched(),
                report.distinctViirsMatched(), report.pairs().size());

        ExpectedPair closest = report.pairs().stream()
                .min(Comparator.comparingDouble(ExpectedPair::vincentyDistanceMeters))
                .orElseThrow();
        ExpectedPair farthest = report.pairs().stream()
                .max(Comparator.comparingDouble(ExpectedPair::vincentyDistanceMeters))
                .orElseThrow();
        System.out.printf(Locale.ROOT,
                "Closest pair : d=%.3f m, dt=%d s, MODIS=%s (%.5f, %.5f) <-> VIIRS=%s %s (%.5f, %.5f)%n",
                closest.vincentyDistanceMeters(), closest.timeOffsetSeconds(),
                closest.modis().acquiredAt(), closest.modis().lat(), closest.modis().lon(),
                closest.viirs().satellite(), closest.viirs().acquiredAt(), closest.viirs().lat(), closest.viirs().lon());
        System.out.printf(Locale.ROOT,
                "Farthest pair: d=%.3f m, dt=%d s, MODIS=%s (%.5f, %.5f) <-> VIIRS=%s %s (%.5f, %.5f)%n",
                farthest.vincentyDistanceMeters(), farthest.timeOffsetSeconds(),
                farthest.modis().acquiredAt(), farthest.modis().lat(), farthest.modis().lon(),
                farthest.viirs().satellite(), farthest.viirs().acquiredAt(), farthest.viirs().lat(), farthest.viirs().lon());

        if (verbose) {
            System.out.println("\n--- All " + report.pairs().size() + " candidate pairs ---");
            for (int i = 0; i < report.pairs().size(); i++) {
                ExpectedPair p = report.pairs().get(i);
                System.out.printf(Locale.ROOT,
                        "%02d | MODIS %s (%s, %.5f, %.5f) <-> VIIRS %s %s (%s, %.5f, %.5f) | dist=%.3f m | dt=%d s%n",
                        i + 1,
                        p.modis().uniqueRecordKey(), p.modis().acquiredAt(), p.modis().lat(), p.modis().lon(),
                        p.viirs().satellite(), p.viirs().uniqueRecordKey(), p.viirs().acquiredAt(), p.viirs().lat(), p.viirs().lon(),
                        p.vincentyDistanceMeters(), p.timeOffsetSeconds());
            }
        }
    }

    public static OracleReport verifyAndCompute(Path fixtureDir, double maxDistanceMeters, long maxSeconds)
            throws IOException {
        Path manifestPath = fixtureDir.resolve("MANIFEST.json");
        if (!Files.isRegularFile(manifestPath)) {
            throw new IllegalStateException("MANIFEST.json ausente em " + fixtureDir);
        }
        String manifestText = Files.readString(manifestPath, StandardCharsets.UTF_8);

        List<VerifiedSource> verifiedSources = new ArrayList<>();
        List<DetectionRecord> modis = new ArrayList<>();
        List<DetectionRecord> viirs = new ArrayList<>();

        for (FixtureSourceSpec spec : CURATED_SOURCES) {
            Path csvPath = fixtureDir.resolve(spec.fileName());
            if (!Files.isRegularFile(csvPath)) {
                throw new IllegalStateException("Fixture ausente: " + csvPath);
            }
            String actualHash = sha256Hex(Files.readAllBytes(csvPath));
            if (!spec.expectedSha256().equals(actualHash)) {
                throw new IllegalStateException("SHA-256 divergente para " + spec.fileName()
                        + ": esperado=" + spec.expectedSha256() + ", obtido=" + actualHash);
            }
            if (!manifestText.contains(actualHash)) {
                throw new IllegalStateException("MANIFEST.json nao registra hash verificado de " + spec.fileName());
            }
            List<DetectionRecord> records = parseFixtureCsv(csvPath, spec);
            if (records.size() != spec.expectedRows()) {
                throw new IllegalStateException("Quantidade de linhas divergente em " + spec.fileName()
                        + ": esperado=" + spec.expectedRows() + ", obtido=" + records.size());
            }
            verifiedSources.add(new VerifiedSource(spec, actualHash, records.size()));
            if ("MODIS".equals(spec.sensor())) {
                modis.addAll(records);
            } else {
                viirs.addAll(records);
            }
        }

        List<ExpectedPair> pairs = new ArrayList<>();
        Set<String> modisMatchedKeys = new HashSet<>();
        Set<String> viirsMatchedKeys = new HashSet<>();

        for (DetectionRecord m : modis) {
            for (DetectionRecord v : viirs) {
                long dtSec = Duration.between(m.acquiredAt(), v.acquiredAt()).getSeconds();
                if (Math.abs(dtSec) > maxSeconds) {
                    continue;
                }
                double distM = vincentyDistanceMeters(m.lat(), m.lon(), v.lat(), v.lon());
                if (distM <= maxDistanceMeters) {
                    modisMatchedKeys.add(m.uniqueRecordKey());
                    viirsMatchedKeys.add(v.uniqueRecordKey());
                    pairs.add(new ExpectedPair(m, v, distM, dtSec));
                }
            }
        }

        pairs.sort(Comparator
                .comparing((ExpectedPair p) -> p.modis().acquiredAt())
                .thenComparing(p -> p.modis().uniqueRecordKey())
                .thenComparingDouble(ExpectedPair::vincentyDistanceMeters)
                .thenComparingLong(p -> Math.abs(p.timeOffsetSeconds()))
                .thenComparing(p -> p.viirs().acquiredAt())
                .thenComparing(p -> p.viirs().uniqueRecordKey()));

        return new OracleReport(
                List.copyOf(verifiedSources),
                modis.size(),
                viirs.size(),
                modisMatchedKeys.size(),
                viirsMatchedKeys.size(),
                List.copyOf(pairs)
        );
    }

    private static List<DetectionRecord> parseFixtureCsv(Path csvPath, FixtureSourceSpec spec) throws IOException {
        List<String> lines = Files.readAllLines(csvPath, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IllegalStateException("CSV vazio: " + csvPath);
        }
        String[] headers = lines.get(0).split(",", -1);
        Map<String, Integer> colIdx = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            colIdx.put(headers[i].trim().toLowerCase(Locale.ROOT), i);
        }

        List<DetectionRecord> out = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank()) {
                continue;
            }
            String[] cols = line.split(",", -1);
            double lat = Double.parseDouble(col(cols, colIdx, "latitude"));
            double lon = Double.parseDouble(col(cols, colIdx, "longitude"));
            if (lat < -4.0 || lat > -3.0 || lon < -54.0 || lon > -53.0) {
                throw new IllegalStateException("Ponto fora do recorte esperado em " + spec.fileName() + " linha " + (i + 1));
            }
            LocalDate date = LocalDate.parse(col(cols, colIdx, "acq_date"));
            String rawTime = col(cols, colIdx, "acq_time");
            String padded = "0".repeat(Math.max(0, 4 - rawTime.length())) + rawTime;
            int hh = Integer.parseInt(padded.substring(0, 2));
            int mm = Integer.parseInt(padded.substring(2, 4));
            Instant ts = ZonedDateTime.of(date, LocalTime.of(hh, mm), ZoneOffset.UTC).toInstant();

            String sat = col(cols, colIdx, "satellite");
            String version = col(cols, colIdx, "version");
            ProcessingStream stream = ProcessingStream.fromFirmsVersion(version);
            if (!spec.expectedVersion().equals(version) || stream != spec.expectedStream()) {
                throw new IllegalStateException("Versao/stream inesperado em " + spec.fileName()
                        + ": " + version + " (" + stream + ")");
            }
            String frpRaw = col(cols, colIdx, "frp");
            Double frp = frpRaw.isEmpty() ? null : Double.parseDouble(frpRaw);
            String confRaw = col(cols, colIdx, "confidence");
            String dayNight = col(cols, colIdx, "daynight");

            out.add(new DetectionRecord(
                    spec.fileName(),
                    i + 1L,
                    spec.sensor(),
                    spec.product(),
                    sat,
                    version,
                    stream,
                    ts,
                    lat,
                    lon,
                    frp,
                    confRaw,
                    dayNight
            ));
        }
        return out;
    }

    private static String col(String[] row, Map<String, Integer> colIdx, String name) {
        Integer idx = colIdx.get(name);
        if (idx == null || idx >= row.length) {
            return "";
        }
        return row[idx].trim();
    }

    /** Formula inversa de Vincenty no elipsoide WGS84 (metros). */
    public static double vincentyDistanceMeters(double lat1Deg, double lon1Deg, double lat2Deg, double lon2Deg) {
        if (lat1Deg == lat2Deg && lon1Deg == lon2Deg) {
            return 0.0;
        }
        double phi1 = Math.toRadians(lat1Deg);
        double phi2 = Math.toRadians(lat2Deg);
        double u1 = Math.atan((1.0 - WGS84_F) * Math.tan(phi1));
        double u2 = Math.atan((1.0 - WGS84_F) * Math.tan(phi2));
        double dLon = Math.toRadians(lon2Deg - lon1Deg);
        if (dLon > Math.PI) dLon -= 2.0 * Math.PI;
        if (dLon < -Math.PI) dLon += 2.0 * Math.PI;

        double sinU1 = Math.sin(u1), cosU1 = Math.cos(u1);
        double sinU2 = Math.sin(u2), cosU2 = Math.cos(u2);

        double lambda = dLon;
        double lambdaPrev;
        double cosSqAlpha = 0, sinSigma = 0, cosSigma = 0, sigma = 0, cos2SigmaM = 0;
        int iter = 0;
        do {
            double sinLambda = Math.sin(lambda);
            double cosLambda = Math.cos(lambda);
            sinSigma = Math.sqrt(Math.pow(cosU2 * sinLambda, 2)
                    + Math.pow(cosU1 * sinU2 - sinU1 * cosU2 * cosLambda, 2));
            if (sinSigma == 0.0) {
                return 0.0;
            }
            cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosLambda;
            sigma = Math.atan2(sinSigma, cosSigma);
            double sinAlpha = cosU1 * cosU2 * sinLambda / sinSigma;
            cosSqAlpha = 1.0 - sinAlpha * sinAlpha;
            cos2SigmaM = cosSqAlpha == 0.0 ? 0.0 : cosSigma - 2.0 * sinU1 * sinU2 / cosSqAlpha;
            double c = (WGS84_F / 16.0) * cosSqAlpha * (4.0 + WGS84_F * (4.0 - 3.0 * cosSqAlpha));
            lambdaPrev = lambda;
            lambda = dLon + (1.0 - c) * WGS84_F * sinAlpha
                    * (sigma + c * sinSigma * (cos2SigmaM + c * cosSigma * (-1.0 + 2.0 * cos2SigmaM * cos2SigmaM)));
        } while (Math.abs(lambda - lambdaPrev) > VINCENTY_CONVERGENCE_RAD && ++iter < 200);

        double uSq = cosSqAlpha * (WGS84_A * WGS84_A - WGS84_B * WGS84_B) / (WGS84_B * WGS84_B);
        double bigA = 1.0 + (uSq / 16384.0) * (4096.0 + uSq * (-768.0 + uSq * (320.0 - 175.0 * uSq)));
        double bigB = (uSq / 1024.0) * (256.0 + uSq * (-128.0 + uSq * (74.0 - 47.0 * uSq)));
        double deltaSigma = bigB * sinSigma * (cos2SigmaM + (bigB / 4.0) * (cosSigma * (-1.0 + 2.0 * cos2SigmaM * cos2SigmaM)
                - (bigB / 6.0) * cos2SigmaM * (-3.0 + 4.0 * sinSigma * sinSigma) * (-3.0 + 4.0 * cos2SigmaM * cos2SigmaM)));
        return WGS84_B * bigA * (sigma - deltaSigma);
    }

    /** Formula direta de Vincenty no elipsoide WGS84 (retorna [latDeg, lonDeg] normalizada em -180..180). */
    public static double[] vincentyDirectPoint(double lat1Deg, double lon1Deg, double azimuthDeg, double distanceMeters) {
        if (distanceMeters == 0.0) {
            return new double[]{lat1Deg, lon1Deg};
        }
        double phi1 = Math.toRadians(lat1Deg);
        double alpha1 = Math.toRadians(azimuthDeg);
        double sinAlpha1 = Math.sin(alpha1);
        double cosAlpha1 = Math.cos(alpha1);

        double tanU1 = (1.0 - WGS84_F) * Math.tan(phi1);
        double cosU1 = 1.0 / Math.sqrt(1.0 + tanU1 * tanU1);
        double sinU1 = tanU1 * cosU1;
        double sigma1 = Math.atan2(tanU1, cosAlpha1);
        double sinAlpha = cosU1 * sinAlpha1;
        double cosSqAlpha = 1.0 - sinAlpha * sinAlpha;
        double uSq = cosSqAlpha * (WGS84_A * WGS84_A - WGS84_B * WGS84_B) / (WGS84_B * WGS84_B);
        double bigA = 1.0 + (uSq / 16384.0) * (4096.0 + uSq * (-768.0 + uSq * (320.0 - 175.0 * uSq)));
        double bigB = (uSq / 1024.0) * (256.0 + uSq * (-128.0 + uSq * (74.0 - 47.0 * uSq)));

        double sigma = distanceMeters / (WGS84_B * bigA);
        double sigmaPrev;
        double cos2SigmaM, sinSigma, cosSigma;
        int iter = 0;
        do {
            cos2SigmaM = Math.cos(2.0 * sigma1 + sigma);
            sinSigma = Math.sin(sigma);
            cosSigma = Math.cos(sigma);
            double deltaSigma = bigB * sinSigma * (cos2SigmaM + (bigB / 4.0) * (cosSigma * (-1.0 + 2.0 * cos2SigmaM * cos2SigmaM)
                    - (bigB / 6.0) * cos2SigmaM * (-3.0 + 4.0 * sinSigma * sinSigma) * (-3.0 + 4.0 * cos2SigmaM * cos2SigmaM)));
            sigmaPrev = sigma;
            sigma = distanceMeters / (WGS84_B * bigA) + deltaSigma;
        } while (Math.abs(sigma - sigmaPrev) > VINCENTY_CONVERGENCE_RAD && ++iter < 200);

        double tmp = sinU1 * sinSigma - cosU1 * cosSigma * cosAlpha1;
        double phi2 = Math.atan2(sinU1 * cosSigma + cosU1 * sinSigma * cosAlpha1,
                (1.0 - WGS84_F) * Math.sqrt(sinAlpha * sinAlpha + tmp * tmp));
        double lambda = Math.atan2(sinSigma * sinAlpha1, cosU1 * cosSigma - sinU1 * sinSigma * cosAlpha1);
        double c = (WGS84_F / 16.0) * cosSqAlpha * (4.0 + WGS84_F * (4.0 - 3.0 * cosSqAlpha));
        double bigL = lambda - (1.0 - c) * WGS84_F * sinAlpha
                * (sigma + c * sinSigma * (cos2SigmaM + c * cosSigma * (-1.0 + 2.0 * cos2SigmaM * cos2SigmaM)));
        double lon2 = ((lon1Deg + Math.toDegrees(bigL) + 540.0) % 360.0) - 180.0;
        return new double[]{Math.toDegrees(phi2), lon2};
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponivel", e);
        }
    }
}
