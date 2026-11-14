package com.fireatlas.gold;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.Connection;
import java.sql.DriverManager;

/** Pula os testes de integracao quando o PostGIS do docker compose nao esta acessivel. */
public class DbAvailable implements ExecutionCondition {

    public static final String URL = System.getenv().getOrDefault("TEST_DB_URL", "jdbc:postgresql://localhost:55432/fireatlas_test");
    public static final String USER = System.getenv().getOrDefault("TEST_DB_USER", "fireatlas");
    public static final String PASSWORD = System.getenv().getOrDefault("TEST_DB_PASSWORD", "fireatlas");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        try (Connection c = DriverManager.getConnection(URL, USER, PASSWORD)) {
            return ConditionEvaluationResult.enabled("PostGIS acessivel");
        } catch (Exception e) {
            return ConditionEvaluationResult.disabled(
                    "PostGIS indisponivel em " + URL + " (rode: docker compose up -d)");
        }
    }
}
