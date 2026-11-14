package com.fireatlas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fireatlas.api.PredictionController;
import com.fireatlas.domain.BBox;
import com.fireatlas.query.DetectionRepository;
import com.fireatlas.query.PredictionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = PredictionController.class)
@Import(PredictionService.class)
class PredictionWebContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * A, B, C, D, E, I:
     * Endpoint valido retorna HTTP 200, available=false, MODEL_NOT_VALIDATED, request normalizado
     * e todos os campos cientificos/preditivos explicitamente serializados como JSON null.
     */
    @Test
    void predictionsWithValidParamsReturnsExplicitModelNotValidatedAndExplicitJsonNulls() throws Exception {
        MvcResult mvcResult = mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", " -54 , -4 , -53 , -3 ")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(header().string("Cache-Control", containsString("no-store")))
                .andExpect(jsonPath("$.available").value(false))
                .andExpect(jsonPath("$.status").value("MODEL_NOT_VALIDATED"))
                .andExpect(jsonPath("$.reason", containsString("Nao existe modelo preditivo validado")))
                .andExpect(jsonPath("$.request.bbox").value("-54.0,-4.0,-53.0,-3.0"))
                .andExpect(jsonPath("$.request.start").value("2026-11-14"))
                .andExpect(jsonPath("$.request.end").value("2026-11-15"))
                .andExpect(jsonPath("$.predictionTarget").value(nullValue()))
                .andExpect(jsonPath("$.validatedHorizon").value(nullValue()))
                .andExpect(jsonPath("$.modelVersion").value(nullValue()))
                .andExpect(jsonPath("$.evaluationMetrics").value(nullValue()))
                .andExpect(jsonPath("$.predictions").value(nullValue()))
                .andReturn();

        String rawJson = mvcResult.getResponse().getContentAsString(StandardCharsets.UTF_8);
        JsonNode root = objectMapper.readTree(rawJson);

        assertEquals(9, root.size(), "Contrato deve conter exatamente os 9 campos esperados");
        assertTrue(root.has("available") && root.get("available").isBoolean() && !root.get("available").asBoolean());
        assertEquals("MODEL_NOT_VALIDATED", root.get("status").asText());
        assertTrue(root.has("reason") && root.get("reason").isTextual() && !root.get("reason").asText().isBlank());

        // Campos obrigatoriamente presentes com JSON null explicito mesmo com spring.jackson.default-property-inclusion=non_null
        for (String nullableKey : Set.of("predictionTarget", "validatedHorizon", "modelVersion", "evaluationMetrics", "predictions")) {
            assertTrue(root.has(nullableKey), "Campo indisponivel deve permanecer presente no JSON: " + nullableKey);
            assertTrue(root.get(nullableKey).isNull(), "Campo deve ser explicitamente null sem valor sintetico: " + nullableKey);
        }

        // Nenhum valor numerico ficticio de FRP, probabilidade ou confianca pode existir fora de request
        assertFalse(rawJson.contains("predictedFrp"), "Nao deve haver FRP futura ficticia");
        assertFalse(rawJson.contains("occurrenceProbability"), "Nao deve haver probabilidade ficticia");
        assertFalse(rawJson.contains("confidenceLevel"), "Nao deve haver confianca ficticia");
    }

    /**
     * A: Aceita limite operacional exato (1 dia e 31 dias inclusive) sem confundir janela com horizonte aprovado.
     */
    @Test
    void predictionsAcceptsSingleDayAndMaxOperationalWindowWhileKeepingPredictionsNull() throws Exception {
        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-14"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available").value(false))
                .andExpect(jsonPath("$.status").value("MODEL_NOT_VALIDATED"))
                .andExpect(jsonPath("$.validatedHorizon").value(nullValue()))
                .andExpect(jsonPath("$.predictions").value(nullValue()));

        // 2026-11-01 ate 2026-12-01 = 31 dias inclusivos (teto operacional padrao = 31)
        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-01")
                        .param("end", "2026-12-01"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available").value(false))
                .andExpect(jsonPath("$.status").value("MODEL_NOT_VALIDATED"))
                .andExpect(jsonPath("$.validatedHorizon").value(nullValue()))
                .andExpect(jsonPath("$.predictions").value(nullValue()));
    }

    /**
     * F: Parametros obrigatorios ausentes ou bbox invalida retornam HTTP 400 (RFC 7807 ProblemDetail).
     */
    @Test
    void predictionsRejectsMissingAndInvalidBboxWithRfc7807BadRequest() throws Exception {
        // Parametros ausentes
        mockMvc.perform(get("/api/v1/predictions")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.detail").exists());

        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("end", "2026-11-15"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));

        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));

        // BBox nao finita, malformada, fora dos limites WGS84 ou invertida/degenerada
        for (String invalidBbox : new String[]{
                "",
                "   ",
                "1,2,3",
                "abc,-4,-53,-3",
                "NaN,-4,-53,-3",
                "-54,Infinity,-53,-3",
                "-54,-4,-Infinity,-3",
                "-181,-4,-53,-3",
                "-54,-91,-53,-3",
                "-54,-4,181,-3",
                "-54,-4,-53,91",
                "-53,-4,-54,-3",
                "-54,-3,-53,-4",
                "-54,-4,-54,-3"
        }) {
            mockMvc.perform(get("/api/v1/predictions")
                            .param("bbox", invalidBbox)
                            .param("start", "2026-11-14")
                            .param("end", "2026-11-15"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(400))
                    .andExpect(jsonPath("$.detail").exists());
        }
    }

    /**
     * G: Datas invalidas, intervalo invertido ou janela acima do teto operacional retornam HTTP 400 (RFC 7807).
     */
    @Test
    void predictionsRejectsInvalidDatesInvertedRangeAndExcessiveWindowWithBadRequest() throws Exception {
        // Datas malformadas
        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-13-40")
                        .param("end", "2026-11-15"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));

        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "amanha"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));

        // Intervalo invertido
        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-15")
                        .param("end", "2026-11-14"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.detail").exists());

        // Janela de 32 dias excede max-days-predictions=31
        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-01")
                        .param("end", "2026-12-02"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.detail").exists());
    }

    /**
     * J: Comprova estruturalmente e no contexto Spring que a funcionalidade nao possui
     * dependencia de banco, nao realiza consultas e nao expoe rotas de escrita/gravacao.
     */
    @Test
    void predictionsHasZeroDatabaseDependenciesQueriesOrWriteEndpoints() {
        // Contexto web funciona integralmente sem DataSource, JdbcTemplate ou DetectionRepository
        assertTrue(applicationContext.getBeansOfType(DataSource.class).isEmpty());
        assertTrue(applicationContext.getBeansOfType(JdbcTemplate.class).isEmpty());
        assertTrue(applicationContext.getBeansOfType(NamedParameterJdbcTemplate.class).isEmpty());
        assertTrue(applicationContext.getBeansOfType(DetectionRepository.class).isEmpty());

        // Servico e controller nao declaram campos ou construtores acoplados a JDBC/SQL/repositorios
        for (Class<?> component : new Class<?>[]{PredictionService.class, PredictionController.class}) {
            for (Field f : component.getDeclaredFields()) {
                assertFalse(isDatabaseType(f.getType()),
                        component.getSimpleName() + " nao deve depender de " + f.getType().getName());
            }
            for (var ctor : component.getDeclaredConstructors()) {
                for (Parameter p : ctor.getParameters()) {
                    assertFalse(isDatabaseType(p.getType()),
                            component.getSimpleName() + " nao deve injetar " + p.getType().getName());
                }
            }
        }

        // Controller somente leitura: nenhum endpoint POST/PUT/PATCH/DELETE
        for (Method m : PredictionController.class.getDeclaredMethods()) {
            assertNull(m.getAnnotation(PostMapping.class));
            assertNull(m.getAnnotation(PutMapping.class));
            assertNull(m.getAnnotation(PatchMapping.class));
            assertNull(m.getAnnotation(DeleteMapping.class));
        }

        // Invocacao direta do servico sem Spring ou banco preserva invariantes
        var service = new PredictionService();
        var response = service.unavailableForWindow(
                BBox.parse("-54,-4,-53,-3"), LocalDate.of(2026, 11, 14), LocalDate.of(2026, 11, 15));
        assertFalse(response.available());
        assertEquals("MODEL_NOT_VALIDATED", response.status());
        assertNull(response.predictionTarget());
        assertNull(response.validatedHorizon());
        assertNull(response.modelVersion());
        assertNull(response.evaluationMetrics());
        assertNull(response.predictions());

        assertThrows(IllegalArgumentException.class, () -> service.unavailableForWindow(
                BBox.parse("-54,-4,-53,-3"), LocalDate.of(2026, 11, 15), LocalDate.of(2026, 11, 14)));
    }

    private static boolean isDatabaseType(Class<?> type) {
        String name = type.getName();
        return DataSource.class.isAssignableFrom(type)
                || name.startsWith("org.springframework.jdbc.")
                || name.startsWith("javax.sql.")
                || name.startsWith("java.sql.")
                || DetectionRepository.class.isAssignableFrom(type);
    }
}
