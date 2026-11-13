package com.fireatlas.domain;

/**
 * Grade global de 0,1 grau usada na tabela daily_grid.
 * cx = floor((lon+180)*10) em 0..3599; cy = floor((lat+90)*10) em 0..1799.
 */
public final class GridMath {

    public static final int SCALE = 10;
    public static final int MAX_CX = 3599;
    public static final int MAX_CY = 1799;
    /** A partir deste zoom a API devolve pontos individuais em vez de celulas. */
    public static final int POINTS_MIN_ZOOM = 9;

    private GridMath() {}

    public static int cx(double lon) {
        return clamp((int) Math.floor((lon + 180.0) * SCALE), 0, MAX_CX);
    }

    public static int cy(double lat) {
        return clamp((int) Math.floor((lat + 90.0) * SCALE), 0, MAX_CY);
    }

    /** Quantas celulas de 0,1 grau formam um bloco de agregacao para o zoom dado. */
    public static int blockFactor(int zoom) {
        if (zoom <= 3) return 20;   // 2,0 grau
        if (zoom == 4) return 10;   // 1,0
        if (zoom == 5) return 5;    // 0,5
        if (zoom <= 7) return 2;    // 0,2
        return 1;                   // 0,1
    }

    public static double centerLon(int gx, int k) {
        return (gx * (double) k + k / 2.0) / SCALE - 180.0;
    }

    public static double centerLat(int gy, int k) {
        return (gy * (double) k + k / 2.0) / SCALE - 90.0;
    }

    public static double cellSizeDeg(int k) {
        return k / (double) SCALE;
    }

    private static int clamp(int v, int lo, int hi) {
        return Math.max(lo, Math.min(hi, v));
    }
}
