package com.fireatlas.query;

import com.fireatlas.domain.Satellite;
import com.fireatlas.domain.Sensor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

/** Catalogo de sensores e satelites (tabelas sensor e satellite). */
@Repository
public class SensorRepository {

    private final JdbcTemplate jdbc;

    public SensorRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<Sensor> sensors() {
        return jdbc.query("SELECT code, name, nominal_resolution_m, description FROM sensor ORDER BY code",
                (rs, i) -> new Sensor(rs.getString("code"), rs.getString("name"),
                        rs.getInt("nominal_resolution_m"), rs.getString("description")));
    }

    public List<Satellite> satellites() {
        return jdbc.query("SELECT code, sensor, platform_name, source_product FROM satellite ORDER BY sensor, code",
                (rs, i) -> new Satellite(rs.getString("code"), rs.getString("sensor"),
                        rs.getString("platform_name"), rs.getString("source_product")));
    }
}
