package com.fireatlas.ingestion.repository;


import com.fireatlas.h3.policy.H3Index;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

/** Recalcula a agregacao diaria por celula H3 (daily_h3) nas resolucoes informadas (0 ate a resolucao-base). */
public final class H3Aggregates {

    private H3Aggregates() {}

    /** Reagrega, a partir das deteccoes, todas as celulas e sensores no intervalo de datas. */
    public static void refresh(Connection con, LocalDate from, LocalDate to, int[] resolutions) throws SQLException {
        try (PreparedStatement del = con.prepareStatement("DELETE FROM daily_h3 WHERE acq_date BETWEEN ? AND ?")) {
            del.setObject(1, from);
            del.setObject(2, to);
            del.executeUpdate();
        }
        for (int res : resolutions) {
            String sql = """
                    INSERT INTO daily_h3 (acq_date, sensor, res, cell, n, frp_sum, frp_max)
                    SELECT acq_date, sensor, %d, %s AS cell, count(*), coalesce(sum(frp::double precision), 0), max(frp)
                    FROM detection
                    WHERE acq_date BETWEEN ? AND ? AND h3 IS NOT NULL
                    GROUP BY acq_date, sensor, cell""".formatted(res, H3Index.parentSql("h3", res));
            try (PreparedStatement ins = con.prepareStatement(sql)) {
                ins.setObject(1, from);
                ins.setObject(2, to);
                ins.executeUpdate();
            }
        }
    }
}
