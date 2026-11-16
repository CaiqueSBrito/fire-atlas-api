package com.fireatlas.gold.bootstrap.repository;

import org.springframework.jdbc.core.JdbcTemplate;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Formato binario do COPY do PostgreSQL: cada valor vai no tipo da coluna, sem o banco interpretar texto.
 * Os tipos saem do catalogo, nao de uma copia do schema no codigo.
 */
final class PgBinaryCopy {
    enum Type { INT2, INT4, INT8, FLOAT4, FLOAT8, TEXT, DATE, BOOL }

    static final byte[] HEADER = {'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', 0, 0, 0, 0, 0, 0, 0, 0, 0};
    static final byte[] TRAILER = {(byte) 0xFF, (byte) 0xFF};
    private static final long POSTGRES_EPOCH_DAY = LocalDate.of(2000, 1, 1).toEpochDay();

    private final List<String> columns;
    private final Type[] types;

    PgBinaryCopy(List<String> columns, List<Type> types) {
        if (columns.size() != types.size()) throw new IllegalArgumentException("colunas e tipos divergem");
        this.columns = List.copyOf(columns);
        this.types = types.toArray(Type[]::new);
    }

    /** Le os tipos das colunas no catalogo; coluna ausente ou tipo sem codificacao binaria e erro. */
    static PgBinaryCopy forTable(JdbcTemplate jdbc, String table, List<String> columns) {
        Map<String, String> catalog = new HashMap<>();
        jdbc.query("SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute"
                        + " WHERE attrelid = CAST(? AS regclass) AND attnum > 0 AND NOT attisdropped",
                rs -> { catalog.put(rs.getString(1), rs.getString(2)); }, table);
        return new PgBinaryCopy(columns, columns.stream().map(column -> {
            String type = catalog.get(column);
            if (type == null) throw new IllegalStateException(table + " sem coluna " + column);
            return type(type);
        }).toList());
    }

    static Type type(String postgresType) {
        if (postgresType.startsWith("character varying")) return Type.TEXT;
        return switch (postgresType) {
            case "smallint" -> Type.INT2;
            case "integer" -> Type.INT4;
            case "bigint" -> Type.INT8;
            case "real" -> Type.FLOAT4;
            case "double precision" -> Type.FLOAT8;
            case "text" -> Type.TEXT;
            case "date" -> Type.DATE;
            case "boolean" -> Type.BOOL;
            default -> throw new IllegalArgumentException("tipo sem codificacao binaria no COPY: " + postgresType);
        };
    }

    String sql(String table) {
        return "COPY " + table + " (" + String.join(",", columns) + ") FROM STDIN WITH (FORMAT binary)";
    }

    int size() {
        return types.length;
    }

    void beginRow(DataOutputStream out) throws IOException {
        out.writeShort(types.length);
    }

    void field(DataOutputStream out, int index, Object value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        switch (types[index]) {
            case INT2 -> {
                long v = longOf(index, value);
                if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) throw outOfRange(index, value);
                out.writeInt(2);
                out.writeShort((int) v);
            }
            case INT4 -> {
                long v = longOf(index, value);
                if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) throw outOfRange(index, value);
                out.writeInt(4);
                out.writeInt((int) v);
            }
            case INT8 -> {
                out.writeInt(8);
                out.writeLong(longOf(index, value));
            }
            case FLOAT4 -> {
                out.writeInt(4);
                out.writeFloat(number(index, value).floatValue());
            }
            case FLOAT8 -> {
                out.writeInt(8);
                out.writeDouble(number(index, value).doubleValue());
            }
            case TEXT -> {
                byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            case DATE -> {
                LocalDate date = value instanceof java.sql.Date sql ? sql.toLocalDate()
                        : value instanceof LocalDate local ? local : null;
                if (date == null) throw wrongType(index, value);
                out.writeInt(4);
                out.writeInt(Math.toIntExact(date.toEpochDay() - POSTGRES_EPOCH_DAY));
            }
            case BOOL -> {
                if (!(value instanceof Boolean flag)) throw wrongType(index, value);
                out.writeInt(1);
                out.writeByte(flag ? 1 : 0);
            }
        }
    }

    private long longOf(int index, Object value) {
        try {
            if (value instanceof BigInteger big) return big.longValueExact();
            if (value instanceof BigDecimal decimal) return decimal.longValueExact();
            if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
                return ((Number) value).longValue();
            }
        } catch (ArithmeticException e) {
            throw outOfRange(index, value);
        }
        throw wrongType(index, value);
    }

    private Number number(int index, Object value) {
        if (value instanceof Number number) return number;
        throw wrongType(index, value);
    }

    private IllegalArgumentException wrongType(int index, Object value) {
        return new IllegalArgumentException("coluna " + columns.get(index) + " (" + types[index] + ") recebeu "
                + value.getClass().getSimpleName());
    }

    private IllegalArgumentException outOfRange(int index, Object value) {
        return new IllegalArgumentException("coluna " + columns.get(index) + " (" + types[index] + ") fora do limite: " + value);
    }
}
