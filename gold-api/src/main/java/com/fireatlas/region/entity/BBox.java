package com.fireatlas.region.entity;

import com.fireatlas.common.policy.InvalidParameterException;

/** Retangulo geografico em graus (WGS84). */
public record BBox(double minLon, double minLat, double maxLon, double maxLat) {

    public BBox {
        if (minLon < -180 || maxLon > 180 || minLat < -90 || maxLat > 90) {
            throw new InvalidParameterException("bbox", "bbox fora dos limites (lon -180..180, lat -90..90)");
        }
        if (minLon >= maxLon || minLat >= maxLat) {
            throw new InvalidParameterException("bbox", "bbox invalido: esperado minLon<maxLon e minLat<maxLat");
        }
    }

    /** Formato: minLon,minLat,maxLon,maxLat */
    public static BBox parse(String s) {
        if (s == null || s.isBlank()) {
            throw new InvalidParameterException("bbox", "parametro bbox obrigatorio: minLon,minLat,maxLon,maxLat");
        }
        String[] p = s.split(",");
        if (p.length != 4) {
            throw new InvalidParameterException("bbox", "bbox deve ter 4 valores: minLon,minLat,maxLon,maxLat");
        }
        try {
            return new BBox(Double.parseDouble(p[0].trim()), Double.parseDouble(p[1].trim()),
                    Double.parseDouble(p[2].trim()), Double.parseDouble(p[3].trim()));
        } catch (NumberFormatException e) {
            throw new InvalidParameterException("bbox", "bbox com valor nao numerico");
        }
    }

    public double maxAbsLat() {
        return Math.max(Math.abs(minLat), Math.abs(maxLat));
    }

    public boolean contains(double lon, double lat) {
        return lon >= minLon && lon <= maxLon && lat >= minLat && lat <= maxLat;
    }

    @Override
    public String toString() {
        return minLon + "," + minLat + "," + maxLon + "," + maxLat;
    }
}
