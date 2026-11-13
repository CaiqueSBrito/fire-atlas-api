package com.fireatlas.harmonization.entity;

import java.time.Instant;

/** Par MODIS x VIIRS: a deteccao MODIS e a deteccao VIIRS mais proxima dentro dos limiares. */
public record MatchedPair(double modisLat, double modisLon, Instant modisAcquiredAt, Float modisFrp,
                          String modisSourceFile, double viirsLat, double viirsLon, Instant viirsAcquiredAt,
                          Float viirsFrp, String viirsSatellite, String viirsSourceFile,
                          double distanceMeters, double minutesApart) {}
