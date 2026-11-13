package com.fireatlas.detection.repository;

import com.fireatlas.detection.entity.Detection;
import com.fireatlas.detection.entity.Location;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/** Converte uma linha da tabela detection na entidade de dominio {@link Detection}. */
public final class DetectionRowMapper implements RowMapper<Detection> {

    /** Colunas lidas (mesma ordem de uso do mapeamento). */
    static final String COLUMNS = "sensor, satellite, instrument, acq_ts, lat, lon, h3, brightness, bright_t31, "
            + "scan, track, frp, confidence_raw, confidence_class, daynight, version, fire_type, "
            + "source_product, source_file";

    @Override
    public Detection mapRow(ResultSet rs, int rowNum) throws SQLException {
        double lat = rs.getDouble("lat");
        double lon = rs.getDouble("lon");
        long h3 = rs.getLong("h3");
        Location location = rs.wasNull() ? Location.of(lat, lon) : new Location(lat, lon, h3); // sem calcular H3
        int type = rs.getInt("fire_type");
        Integer fireType = rs.wasNull() ? null : type;
        return new Detection(rs.getString("sensor"), rs.getString("satellite"), rs.getString("instrument"),
                rs.getTimestamp("acq_ts").toInstant(), location,
                real(rs, "brightness"), real(rs, "bright_t31"), real(rs, "scan"), real(rs, "track"),
                real(rs, "frp"), rs.getString("confidence_raw"), rs.getString("confidence_class"),
                rs.getString("daynight"), rs.getString("version"), fireType,
                new Detection.Source(rs.getString("source_product"), rs.getString("source_file")));
    }

    /** Colunas real (float4) do banco; ausente vira null. */
    private static Double real(ResultSet rs, String col) throws SQLException {
        float v = rs.getFloat(col);
        return rs.wasNull() ? null : (double) v;
    }
}
