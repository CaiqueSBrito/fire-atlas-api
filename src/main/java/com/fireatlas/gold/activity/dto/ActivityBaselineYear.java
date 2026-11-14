package com.fireatlas.gold.activity.dto;

/** Soma do mesmo mes em um ano anterior (so entram anos com dado). */
public record ActivityBaselineYear(int year, double harmonized, int daysWithData) {}
