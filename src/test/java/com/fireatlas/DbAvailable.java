package com.fireatlas;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.Connection;
import java.sql.DriverManager;

/** Pula os testes de integracao quando o PostGIS do docker compose nao esta acessivel. */
public class DbAvailable implements ExecutionCondition {

    static final String URL = System.getenv().getOrDefault("TEST_DB_URL", "jdbc:postgresql://localhost:5433/fireatlas");
    static final String USER = System.getenv().getOrDefault("TEST_DB_USER", "fireatlas");
    static final String PASSWORD = System.getenv().getOrDefault("TEST_DB_PASSWORD", "fireatlas");

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
