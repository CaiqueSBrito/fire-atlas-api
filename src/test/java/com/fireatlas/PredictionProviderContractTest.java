package com.fireatlas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fireatlas.api.ApiExceptionHandler;
import com.fireatlas.api.PredictionController;
import com.fireatlas.domain.BBox;
import com.fireatlas.prediction.DisabledPredictionProvider;
import com.fireatlas.prediction.HttpPredictionProvider;
import com.fireatlas.prediction.PredictionModelConfig;
import com.fireatlas.prediction.PredictionModelProperties;
import com.fireatlas.prediction.PredictionProvider;
import com.fireatlas.prediction.PredictionProviderException;
import com.fireatlas.query.PredictionService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Testes automatizados da infraestrutura de integracao preditiva (NASA-020 Gate B.1, cenarios A-Q).
 *
 * <p>Todos os payloads remotos desta classe sao FIXTURES SIMULADAS DE CONTRATO HTTP v1 para
 * verificacao do adaptador e jamais representam modelo ou resultado cientifico real.
 */
class PredictionProviderContractTest {

    private static final Instant FIXED_AS_OF = Instant.parse("2026-11-13T12:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_AS_OF, ZoneOffset.UTC);
    private static final String TEST_SHA256 =
            "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    private ObjectMapper objectMapper;
    private HttpServer fakeServer;
    private ExecutorService serverExecutor;
    private int serverPort;
    private final AtomicInteger serverCalls = new AtomicInteger();
    private final AtomicReference<String> lastRequestMethod = new AtomicReference<>();
    private final AtomicReference<String> lastRequestBody = new AtomicReference<>();
    private final AtomicReference<String> lastRequestIdHeader = new AtomicReference<>();
    private volatile Consumer<HttpExchange> serverHandler;

    @BeforeEach
    void setUp() throws IOException {
        objectMapper = Jackson2ObjectMapperBuilder.json()
                .serializationInclusion(NON_NULL)
                .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build();
        serverCalls.set(0);
        lastRequestMethod.set(null);
        lastRequestBody.set(null);
        lastRequestIdHeader.set(null);

        serverExecutor = Executors.newCachedThreadPool();
        fakeServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeServer.setExecutor(serverExecutor);
        serverPort = fakeServer.getAddress().getPort();
        fakeServer.createContext("/v1/predict", exchange -> {
            serverCalls.incrementAndGet();
            lastRequestMethod.set(exchange.getRequestMethod());
            lastRequestIdHeader.set(exchange.getRequestHeaders().getFirst("X-Request-Id"));
            byte[] reqBytes = exchange.getRequestBody().readAllBytes();
            lastRequestBody.set(new String(reqBytes, StandardCharsets.UTF_8));
            Consumer<HttpExchange> handler = serverHandler;
            if (handler != null) {
                handler.accept(exchange);
            } else {
                writeJson(exchange, 200, validSimulatedResponseJson());
            }
        });
        fakeServer.start();
    }

    @AfterEach
    void tearDown() {
        if (fakeServer != null) {
            fakeServer.stop(0);
        }
        if (serverExecutor != null) {
            serverExecutor.shutdownNow();
        }
    }

    /**
     * A & B: Provedor desativado (padrao) nao executa nenhuma chamada HTTP, mesmo se endpointUrl
     * estiver preenchida, e preserva MODEL_NOT_VALIDATED com todos os campos indisponiveis nulos.
     */
    @Test
    void disabledProviderNeverExecutesHttpCallAndPreservesModelNotValidated() throws Exception {
        PredictionModelProperties props = completeApprovedProperties();
        props.setEnabled(false); // Desativado por padrao

        PredictionModelConfig config = new PredictionModelConfig();
        PredictionProvider provider = config.predictionProvider(props, objectMapper);
        assertInstanceOf(DisabledPredictionProvider.class, provider);
        assertFalse(provider.isEnabled());

        PredictionService service = new PredictionService(provider, FIXED_CLOCK);
        MockMvc mockMvc = buildMockMvc(service);

        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available").value(false))
                .andExpect(jsonPath("$.status").value("MODEL_NOT_VALIDATED"))
                .andExpect(jsonPath("$.predictionTarget").value(nullValue()))
                .andExpect(jsonPath("$.validatedHorizon").value(nullValue()))
                .andExpect(jsonPath("$.modelVersion").value(nullValue()))
                .andExpect(jsonPath("$.evaluationMetrics").value(nullValue()))
                .andExpect(jsonPath("$.predictions").value(nullValue()));

        assertEquals(0, serverCalls.get(), "Provedor desativado jamais deve realizar chamadas HTTP");
    }

    /**
     * C & D: Configuracao incompleta ou aprovacao invalida falha de forma controlada e explicita,
     * impedindo ativacao acidental apenas com enabled=true ou endpointUrl.
     */
    @Test
    void incompleteOrInvalidApprovalConfigurationFailsFastOnActivation() {
        PredictionModelConfig config = new PredictionModelConfig();

        // Apenas enabled=true sem manifesto falha imediatamente
        PredictionModelProperties onlyEnabled = new PredictionModelProperties();
        onlyEnabled.setEnabled(true);
        onlyEnabled.setEndpointUrl("http://127.0.0.1:" + serverPort + "/v1/predict");
        assertThrows(IllegalStateException.class, () -> config.predictionProvider(onlyEnabled, objectMapper));

        // Ausencia individual de cada pre-requisito obrigatorio deve falhar
        assertActivationFails(p -> p.setEndpointUrl(" "));
        assertActivationFails(p -> p.setEndpointUrl("ftp://127.0.0.1/predict"));
        assertActivationFails(p -> p.setProtocolVersion("v0"));
        assertActivationFails(p -> p.setModelId(null));
        assertActivationFails(p -> p.setModelVersion(""));
        assertActivationFails(p -> p.setPredictionTarget(null));
        assertActivationFails(p -> p.setFeatureContractId(null));
        assertActivationFails(p -> p.setHorizonUnit("WEEKS"));
        assertActivationFails(p -> p.setMinLead(-1));
        assertActivationFails(p -> p.setMaxLead(0));
        assertActivationFails(p -> {
            p.setMinLead(5);
            p.setMaxLead(2);
        });
        assertActivationFails(p -> p.setTemporalResolution(null));
        assertActivationFails(p -> p.setSpatialResolution(null));
        assertActivationFails(p -> p.setCoverageBbox("NaN,-4,-53,-3"));
        assertActivationFails(p -> p.setArtifactUri(null));
        assertActivationFails(p -> p.setArtifactSha256("hash-curto-invalido"));
        assertActivationFails(p -> p.setTrainingDatasetId(null));
        assertActivationFails(p -> p.setEvaluationReportUri(null));
        assertActivationFails(p -> p.setTemporalSplitPolicy("RANDOM"));
        assertActivationFails(p -> p.setTemporalSplitPolicy("K_FOLD_RANDOM"));
        assertActivationFails(p -> p.setBaselineComparisonSummary(null));
        assertActivationFails(p -> p.setOutOfSampleMetrics(Map.of()));
        assertActivationFails(p -> p.setOutOfSampleMetrics(Map.of("prAuc", Double.NaN)));
        assertActivationFails(p -> p.setApprovedBy(null));
        assertActivationFails(p -> p.setApprovalReference(null));
        assertActivationFails(p -> p.setApprovedAt(null));

        assertEquals(0, serverCalls.get());
    }

    /**
     * E & F: Adaptador HTTP envia payload conforme protocolo v1 (com asOf UTC, bbox, janela,
     * alvo e featureContractId) e processa resposta simulada de contrato valida em teste.
     */
    @Test
    void httpAdapterSendsV1RequestAndMapsValidSimulatedContractFixture() throws Exception {
        serverHandler = exchange -> writeJson(exchange, 200, validSimulatedResponseJson());

        MockMvc mockMvc = buildMockMvc(enabledService());
        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available").value(true))
                .andExpect(jsonPath("$.status").value("AVAILABLE"))
                .andExpect(jsonPath("$.reason", containsString("SIM-EXEC-001")))
                .andExpect(jsonPath("$.request.bbox").value("-54.0,-4.0,-53.0,-3.0"))
                .andExpect(jsonPath("$.predictionTarget").value("ACTIVE_FIRE_OCCURRENCE_PROBABILITY_DAILY_01DEG"))
                .andExpect(jsonPath("$.modelVersion").value("0.1.0-contract-fixture"))
                .andExpect(jsonPath("$.validatedHorizon.unit").value("DAYS"))
                .andExpect(jsonPath("$.validatedHorizon.minLead").value(1))
                .andExpect(jsonPath("$.validatedHorizon.maxLead").value(7))
                .andExpect(jsonPath("$.evaluationMetrics.prAuc").value(0.61))
                .andExpect(jsonPath("$.evaluationMetrics.brierScore").value(0.08))
                .andExpect(jsonPath("$.predictions.length()").value(2))
                .andExpect(jsonPath("$.predictions[0].lat").value(-3.45))
                .andExpect(jsonPath("$.predictions[0].lon").value(-53.55))
                .andExpect(jsonPath("$.predictions[0].occurrenceProbability").value(0.72))
                .andExpect(jsonPath("$.predictions[0].predictedFrpMw").value(14.5))
                .andExpect(jsonPath("$.predictions[0].frpUnit").value("MW"))
                .andExpect(jsonPath("$.predictions[0].confidenceLevel").value("EXPERIMENTAL_CONTRACT_FIXTURE"));

        assertEquals(1, serverCalls.get());
        assertEquals("POST", lastRequestMethod.get());
        assertNotNull(lastRequestIdHeader.get());

        JsonNode sentRequest = objectMapper.readTree(lastRequestBody.get());
        assertEquals("v1", sentRequest.get("protocolVersion").asText());
        assertEquals(lastRequestIdHeader.get(), sentRequest.get("requestId").asText());
        assertEquals(-54.0, sentRequest.get("bbox").get("minLon").asDouble());
        assertEquals(-4.0, sentRequest.get("bbox").get("minLat").asDouble());
        assertEquals(-53.0, sentRequest.get("bbox").get("maxLon").asDouble());
        assertEquals(-3.0, sentRequest.get("bbox").get("maxLat").asDouble());
        assertEquals("2026-11-13T12:00:00Z", sentRequest.get("asOf").asText());
        assertEquals("2026-11-14", sentRequest.get("requestedWindow").get("start").asText());
        assertEquals("2026-11-15", sentRequest.get("requestedWindow").get("end").asText());
        assertEquals("ACTIVE_FIRE_OCCURRENCE_PROBABILITY_DAILY_01DEG", sentRequest.get("predictionTarget").asText());
        assertEquals("firms-daily-grid-v1-fixture", sentRequest.get("featureContractId").asText());
        assertEquals("contract-test-model", sentRequest.get("modelId").asText());
        assertEquals("0.1.0-contract-fixture", sentRequest.get("modelVersion").asText());
    }

    /**
     * G, H, I & P: Falha de conexao, timeout e HTTP 5xx retornam HTTP 503 (PROVIDER_UNAVAILABLE),
     * sem previsoes parciais/ficticias e sem expor URL interna, IP ou mensagem interna do servico remoto.
     */
    @Test
    void connectionFailureTimeoutAndHttp5xxReturn503WithoutLeakingInternalsOrPredictions() throws Exception {
        // G: Falha de conexao (porta local fechada)
        int closedPort;
        try (ServerSocket ss = new ServerSocket(0)) {
            closedPort = ss.getLocalPort();
        }
        PredictionModelProperties closedProps = completeApprovedProperties();
        closedProps.setEndpointUrl("http://127.0.0.1:" + closedPort + "/v1/predict");
        PredictionService closedService = new PredictionService(
                new HttpPredictionProvider(closedProps.validateForActivation(), objectMapper), FIXED_CLOCK);

        buildMockMvc(closedService).perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("PROVIDER_UNAVAILABLE"))
                .andExpect(jsonPath("$.available").value(false))
                .andExpect(jsonPath("$.detail", not(containsString("127.0.0.1"))))
                .andExpect(jsonPath("$.detail", not(containsString(String.valueOf(closedPort)))))
                .andExpect(jsonPath("$.predictions").doesNotExist());

        // H: Timeout de leitura
        PredictionModelProperties timeoutProps = completeApprovedProperties();
        timeoutProps.setReadTimeoutMs(100);
        PredictionService timeoutService = new PredictionService(
                new HttpPredictionProvider(timeoutProps.validateForActivation(), objectMapper), FIXED_CLOCK);
        serverHandler = exchange -> {
            try {
                Thread.sleep(450);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            writeJson(exchange, 200, validSimulatedResponseJson());
        };

        buildMockMvc(timeoutService).perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("PROVIDER_UNAVAILABLE"))
                .andExpect(jsonPath("$.available").value(false))
                .andExpect(jsonPath("$.predictions").doesNotExist());

        // I: HTTP 500 / 503 com segredo interno no corpo remoto
        MockMvc mockMvc = buildMockMvc(enabledService());
        for (int statusCode : new int[]{500, 502, 503}) {
            serverHandler = exchange -> writeJson(exchange, statusCode,
                    "{\"error\":\"SEGREDO_INTERNO_SQL_CREDENTIAL\",\"internalHost\":\"10.99.88.77:8080\"}");

            mockMvc.perform(get("/api/v1/predictions")
                            .param("bbox", "-54,-4,-53,-3")
                            .param("start", "2026-11-14")
                            .param("end", "2026-11-15"))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(jsonPath("$.status").value("PROVIDER_UNAVAILABLE"))
                    .andExpect(jsonPath("$.available").value(false))
                    .andExpect(jsonPath("$.detail", not(containsString("SEGREDO_INTERNO"))))
                    .andExpect(jsonPath("$.detail", not(containsString("10.99.88.77"))))
                    .andExpect(jsonPath("$.predictions").doesNotExist());
        }
    }

    /**
     * J, K, L, M, N & P: JSON malformado, versao de protocolo/modelo incompativel, probabilidades
     * fora de [0,1], FRP negativa ou sem unidade, NaN/Infinity, coordenadas fora do bbox, timestamps
     * fora do horizonte e respostas parciais/incompletas sao rejeitados com INVALID_PROVIDER_RESPONSE (503).
     */
    @Test
    void malformedOrContractViolatingResponsesAreRejectedAtomicallyWithoutPartialResults() throws Exception {
        MockMvc mockMvc = buildMockMvc(enabledService());

        String[] invalidPayloads = new String[]{
                // J: JSON malformado e corpo vazio
                "{malformed-json",
                "",
                // K: protocolVersion, modelId, modelVersion, predictionTarget ou validatedHorizon incompativeis
                validSimulatedResponseJson().replace("\"protocolVersion\":\"v1\"", "\"protocolVersion\":\"v2\""),
                validSimulatedResponseJson().replace("\"modelId\":\"contract-test-model\"", "\"modelId\":\"other-model\""),
                validSimulatedResponseJson().replace("\"modelVersion\":\"0.1.0-contract-fixture\"", "\"modelVersion\":\"9.9.9\""),
                validSimulatedResponseJson().replace(
                        "\"predictionTarget\":\"ACTIVE_FIRE_OCCURRENCE_PROBABILITY_DAILY_01DEG\"",
                        "\"predictionTarget\":\"OTHER_TARGET\""),
                validSimulatedResponseJson().replace("\"maxLead\":7", "\"maxLead\":14"),
                // L: Probabilidade > 1, < 0, FRP negativa, FRP sem unidade MW, NaN, coordenada fora do bbox
                validSimulatedResponseJson().replace("\"occurrenceProbability\":0.72", "\"occurrenceProbability\":1.05"),
                validSimulatedResponseJson().replace("\"occurrenceProbability\":0.72", "\"occurrenceProbability\":-0.1"),
                validSimulatedResponseJson().replace("\"predictedFrpMw\":14.5", "\"predictedFrpMw\":-2.0"),
                validSimulatedResponseJson().replace("\"frpUnit\":\"MW\"", "\"frpUnit\":\"kW\""),
                validSimulatedResponseJson().replace("\"occurrenceProbability\":0.72", "\"occurrenceProbability\":\"NaN\""),
                validSimulatedResponseJson().replace("\"occurrenceProbability\":0.72", "\"occurrenceProbability\":\"Infinity\""),
                validSimulatedResponseJson().replace("\"lat\":-3.45", "\"lat\":-12.0"),
                validSimulatedResponseJson().replace("\"lon\":-53.55", "\"lon\":-60.0"),
                validSimulatedResponseJson().replace("\"lat\":-3.45,", ""),
                // M: Timestamp anterior ao asOf ou fora do intervalo previsto / asOf futuro
                validSimulatedResponseJson().replace(
                        "\"forecastTimestamp\":\"2026-11-14T15:00:00Z\"",
                        "\"forecastTimestamp\":\"2026-11-13T10:00:00Z\""),
                validSimulatedResponseJson().replace(
                        "\"forecastTimestamp\":\"2026-11-14T15:00:00Z\"",
                        "\"forecastTimestamp\":\"2026-11-19T15:00:00Z\""),
                validSimulatedResponseJson().replace(
                        "\"asOf\":\"2026-11-13T12:00:00Z\"",
                        "\"asOf\":\"2026-11-14T00:00:00Z\""),
                // N: Metadados incompletos, lista vazia com SUFFICIENT ou segundo item invalido (sem retorno parcial!)
                validSimulatedResponseJson().replace("\"executionId\":\"SIM-EXEC-001\",", ""),
                validSimulatedResponseJson().replace("\"coverage\":{\"status\":\"SUFFICIENT\",\"observedCoverageRatio\":0.94},", ""),
                validSimulatedResponseJson().replace("\"end\":\"2026-11-15\"", "\"end\":\"2026-11-16\""),
                """
                {
                  "protocolVersion":"v1",
                  "executionId":"SIM-EXEC-EMPTY",
                  "modelId":"contract-test-model",
                  "modelVersion":"0.1.0-contract-fixture",
                  "predictionTarget":"ACTIVE_FIRE_OCCURRENCE_PROBABILITY_DAILY_01DEG",
                  "validatedHorizon":{"unit":"DAYS","minLead":1,"maxLead":7,"temporalResolution":"P1D","spatialResolution":"0.1_DEG"},
                  "asOf":"2026-11-13T12:00:00Z",
                  "predictedInterval":{"start":"2026-11-14","end":"2026-11-15"},
                  "coverage":{"status":"SUFFICIENT","observedCoverageRatio":0.95},
                  "predictions":[]
                }
                """,
                // Item 1 valido, Item 2 com coordenada fora do bbox -> deve rejeitar tudo sem expor Item 1
                validSimulatedResponseJson().replace("\"lat\":-3.60", "\"lat\":15.0"),
                // Cobertura INSUFFICIENT_COVERAGE contraditoria com lista de predictions nao vazia -> rejeitada
                validSimulatedResponseJson().replace("\"status\":\"SUFFICIENT\"", "\"status\":\"INSUFFICIENT_COVERAGE\"")
        };

        for (String payload : invalidPayloads) {
            serverHandler = exchange -> writeJson(exchange, 200, payload);

            mockMvc.perform(get("/api/v1/predictions")
                            .param("bbox", "-54,-4,-53,-3")
                            .param("start", "2026-11-14")
                            .param("end", "2026-11-15"))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(jsonPath("$.status").value(PredictionProviderException.CODE_INVALID_PROVIDER_RESPONSE))
                    .andExpect(jsonPath("$.available").value(false))
                    .andExpect(jsonPath("$.predictions").doesNotExist());
        }

        // J.2: Payload excedendo maxResponseBytes é rejeitado
        PredictionModelProperties smallLimitProps = completeApprovedProperties();
        smallLimitProps.setMaxResponseBytes(1024);
        MockMvc smallLimitMvc = buildMockMvc(new PredictionService(
                new HttpPredictionProvider(smallLimitProps.validateForActivation(), objectMapper), FIXED_CLOCK));
        String oversizedPayload = validSimulatedResponseJson() + " ".repeat(2048);
        serverHandler = exchange -> writeJson(exchange, 200, oversizedPayload);
        smallLimitMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(PredictionProviderException.CODE_INVALID_PROVIDER_RESPONSE))
                .andExpect(jsonPath("$.predictions").doesNotExist());

        // L.2: Modelo com supportsFrp=false no manifesto rejeita resposta que envie predictedFrpMw
        PredictionModelProperties noFrpProps = completeApprovedProperties();
        noFrpProps.setSupportsFrp(false);
        MockMvc noFrpMvc = buildMockMvc(new PredictionService(
                new HttpPredictionProvider(noFrpProps.validateForActivation(), objectMapper), FIXED_CLOCK));
        serverHandler = exchange -> writeJson(exchange, 200, validSimulatedResponseJson());
        noFrpMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(PredictionProviderException.CODE_INVALID_PROVIDER_RESPONSE))
                .andExpect(jsonPath("$.predictions").doesNotExist());
    }

    /**
     * M & O: Janela/regiao fora do escopo aprovado (OUT_OF_APPROVED_SCOPE) e falta de cobertura
     * observacional (INSUFFICIENT_DATA_COVERAGE) retornam HTTP 200 com available=false e predictions=null,
     * sem jamais converter ausencia de cobertura em previsao de zero ocorrencias.
     */
    @Test
    void outOfScopeAndInsufficientCoverageReturnExplicitUnavailableHttp200WithNullPredictions() throws Exception {
        MockMvc mockMvc = buildMockMvc(enabledService());

        // M.1: BBox fora do coverageBbox aprovado (-60,-10,-50,0) -> nao chama o provedor remoto
        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-45,-4,-44,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available").value(false))
                .andExpect(jsonPath("$.status").value("OUT_OF_APPROVED_SCOPE"))
                .andExpect(jsonPath("$.predictions").value(nullValue()));
        assertEquals(0, serverCalls.get(), "Requisicao fora do dominio espacial aprovado nao deve acionar o servico remoto");

        // M.2: Janela temporal fora do horizonte aprovado [asOf + 1d .. asOf + 7d] = [2026-11-14 .. 2026-11-20]
        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-21")
                        .param("end", "2026-11-22"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available").value(false))
                .andExpect(jsonPath("$.status").value("OUT_OF_APPROVED_SCOPE"))
                .andExpect(jsonPath("$.predictions").value(nullValue()));
        assertEquals(0, serverCalls.get(), "Requisicao fora do horizonte aprovado nao deve acionar o servico remoto");

        // O: Falta de cobertura reportada pelo provedor -> HTTP 200, available=false, status=INSUFFICIENT_DATA_COVERAGE, predictions=null
        serverHandler = exchange -> writeJson(exchange, 200, """
                {
                  "protocolVersion":"v1",
                  "executionId":"SIM-EXEC-NOCOV",
                  "modelId":"contract-test-model",
                  "modelVersion":"0.1.0-contract-fixture",
                  "predictionTarget":"ACTIVE_FIRE_OCCURRENCE_PROBABILITY_DAILY_01DEG",
                  "validatedHorizon":{"unit":"DAYS","minLead":1,"maxLead":7,"temporalResolution":"P1D","spatialResolution":"0.1_DEG"},
                  "asOf":"2026-11-13T12:00:00Z",
                  "predictedInterval":{"start":"2026-11-14","end":"2026-11-15"},
                  "coverage":{
                    "status":"INSUFFICIENT_COVERAGE",
                    "observedCoverageRatio":0.12,
                    "reason":"Cobertura observacional insuficiente no recorte (nuvens > 85%); lacuna nao equivale a zero ocorrencias."
                  },
                  "predictions":null
                }
                """);

        mockMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available").value(false))
                .andExpect(jsonPath("$.status").value("INSUFFICIENT_DATA_COVERAGE"))
                .andExpect(jsonPath("$.reason", containsString("lacuna nao equivale a zero ocorrencias")))
                .andExpect(jsonPath("$.predictions").value(nullValue()));
        assertEquals(1, serverCalls.get());
    }

    /**
     * Regressao adversarial 1 & 2: Timeout de 200 ms com cabecalhos HTTP 200 enviados imediatamente
     * e corpo atrasado em 1500 ms deve abortar no deadline efetivo (~200 ms) com HTTP 503
     * (PROVIDER_UNAVAILABLE) e limpar recursos sem aguardar os 1500 ms.
     */
    @Test
    void delayedBodyAfterImmediateHeadersTimesOutWithin200msDeadline() throws Exception {
        PredictionModelProperties props = completeApprovedProperties();
        props.setReadTimeoutMs(200);
        PredictionService service = new PredictionService(
                new HttpPredictionProvider(props.validateForActivation(), objectMapper), FIXED_CLOCK);
        MockMvc mockMvc = buildMockMvc(service);

        serverHandler = exchange -> {
            try {
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
                // Envia cabecalhos 200 imediatamente (chunked) e atrasa o corpo em 1500 ms
                exchange.sendResponseHeaders(200, 0);
                OutputStream os = exchange.getResponseBody();
                os.write("{\"protocolVersion\":".getBytes(StandardCharsets.UTF_8));
                os.flush();
                Thread.sleep(1500);
                os.write("\"v1\"}".getBytes(StandardCharsets.UTF_8));
                os.close();
            } catch (Exception ignored) {
            } finally {
                exchange.close();
            }
        };

        for (int attempt = 1; attempt <= 2; attempt++) {
            long t0 = System.nanoTime();
            mockMvc.perform(get("/api/v1/predictions")
                            .param("bbox", "-54,-4,-53,-3")
                            .param("start", "2026-11-14")
                            .param("end", "2026-11-15"))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(jsonPath("$.status").value("PROVIDER_UNAVAILABLE"))
                    .andExpect(jsonPath("$.available").value(false))
                    .andExpect(jsonPath("$.predictions").doesNotExist());
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            assertTrue(elapsedMs < 1100,
                    "Leitura de corpo atrasado (tentativa " + attempt + ") deve abortar no deadline de 200ms (elapsed=" + elapsedMs + "ms)");
        }
    }

    /**
     * Regressao adversarial 3 & 4: Horizonte aprovado de 6–12 HOURS rejeita ponto em asOf+1h (e asOf+5h59m59s
     * ou asOf+12h00m01s) e aceita exatamente os limites de 6h e 12h. Tambem valida precisao em DAYS (D+1
     * rejeita ponto no dia de asOf).
     */
    @Test
    void hoursHorizon6To12RejectsPointAtAsOfPlus1hAndEnforcesExactBoundaries() throws Exception {
        PredictionModelProperties hoursProps = completeApprovedProperties();
        hoursProps.setHorizonUnit("HOURS");
        hoursProps.setMinLead(6);
        hoursProps.setMaxLead(12);
        hoursProps.setTemporalResolution("PT1H");
        PredictionService hoursService = new PredictionService(
                new HttpPredictionProvider(hoursProps.validateForActivation(), objectMapper), FIXED_CLOCK);
        MockMvc hoursMvc = buildMockMvc(hoursService);

        // FIXED_AS_OF = 2026-11-13T12:00:00Z -> janela aprovada [18:00Z .. 2026-11-14T00:00:00Z]
        // Ponto em asOf + 1h (13:00Z), asOf + 5h59m59s (17:59:59Z) e asOf + 12h00m01s (2026-11-14T00:00:01Z) devem ser rejeitados
        for (String invalidTs : new String[]{
                "2026-11-13T13:00:00Z",
                "2026-11-13T17:59:59Z",
                "2026-11-14T00:00:01Z"
        }) {
            serverHandler = exchange -> writeJson(exchange, 200, hoursResponseJson(invalidTs, "2026-11-13", "2026-11-14"));
            hoursMvc.perform(get("/api/v1/predictions")
                            .param("bbox", "-54,-4,-53,-3")
                            .param("start", "2026-11-13")
                            .param("end", "2026-11-14"))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(jsonPath("$.status").value("INVALID_PROVIDER_RESPONSE"))
                    .andExpect(jsonPath("$.available").value(false))
                    .andExpect(jsonPath("$.predictions").doesNotExist());
        }

        // Limites exatos asOf + 6h (18:00:00Z) e asOf + 12h (2026-11-14T00:00:00Z) devem ser aceitos
        for (String validBoundaryTs : new String[]{
                "2026-11-13T18:00:00Z",
                "2026-11-14T00:00:00Z"
        }) {
            serverHandler = exchange -> writeJson(exchange, 200, hoursResponseJson(validBoundaryTs, "2026-11-13", "2026-11-14"));
            hoursMvc.perform(get("/api/v1/predictions")
                            .param("bbox", "-54,-4,-53,-3")
                            .param("start", "2026-11-13")
                            .param("end", "2026-11-14"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.available").value(true))
                    .andExpect(jsonPath("$.status").value("AVAILABLE"))
                    .andExpect(jsonPath("$.predictions[0].forecastTimestamp").value(validBoundaryTs));
        }

        // Timestamp dentro de [6h, 12h] (2026-11-13T20:00:00Z) porem fora da janela solicitada (2026-11-14..2026-11-14) deve ser rejeitado
        serverHandler = exchange -> writeJson(exchange, 200, hoursResponseJson("2026-11-13T20:00:00Z", "2026-11-14", "2026-11-14"));
        hoursMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-14"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("INVALID_PROVIDER_RESPONSE"))
                .andExpect(jsonPath("$.predictions").doesNotExist());

        // Precisao em DAYS (D+1 a D+7): aceita limites exatos 2026-11-14T00:00:00Z e 2026-11-15T23:59:59Z,
        // e rejeita timestamp fora da janela solicitada (2026-11-16T00:00:00Z quando janela = 14..15)
        MockMvc daysMvc = buildMockMvc(enabledService());
        serverHandler = exchange -> writeJson(exchange, 200,
                validSimulatedResponseJson()
                        .replace("2026-11-14T15:00:00Z", "2026-11-14T00:00:00Z")
                        .replace("2026-11-15T15:00:00Z", "2026-11-15T23:59:59Z"));
        daysMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.available").value(true));

        serverHandler = exchange -> writeJson(exchange, 200,
                validSimulatedResponseJson().replace("2026-11-15T15:00:00Z", "2026-11-16T00:00:00Z"));
        daysMvc.perform(get("/api/v1/predictions")
                        .param("bbox", "-54,-4,-53,-3")
                        .param("start", "2026-11-14")
                        .param("end", "2026-11-15"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("INVALID_PROVIDER_RESPONSE"))
                .andExpect(jsonPath("$.predictions").doesNotExist());
    }

    /**
     * Regressao adversarial 5: Valida que PREDICTION_PROVIDER_URL em application.yml faz binding real
     * no contexto Spring tanto em modo desativado (padrao) quanto quando habilitado com manifesto completo.
     */
    @Test
    void predictionProviderUrlEnvVarBindsInRealSpringContextFromApplicationYml() throws Exception {
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        PropertySource<?> yamlSource = loader.load("application", new ClassPathResource("application.yml")).getFirst();

        String customUrl = "http://127.0.0.1:" + serverPort + "/v1/predict";

        // Modo padrao (enabled=false) com PREDICTION_PROVIDER_URL definida mantem DisabledPredictionProvider
        new ApplicationContextRunner()
                .withInitializer(ctx -> ctx.getEnvironment().getPropertySources().addLast(yamlSource))
                .withBean(ObjectMapper.class, () -> objectMapper)
                .withUserConfiguration(PredictionModelConfig.class)
                .withPropertyValues("PREDICTION_PROVIDER_URL=" + customUrl)
                .run(context -> {
                    PredictionModelProperties props = context.getBean(PredictionModelProperties.class);
                    assertFalse(props.isEnabled());
                    assertEquals(customUrl, props.getEndpointUrl());
                    PredictionProvider provider = context.getBean(PredictionProvider.class);
                    assertInstanceOf(DisabledPredictionProvider.class, provider);
                    assertFalse(provider.isEnabled());
                });

        // Modo habilitado (PREDICTION_ENABLED=true) + PREDICTION_PROVIDER_URL + manifesto completo instancia HttpPredictionProvider
        new ApplicationContextRunner()
                .withInitializer(ctx -> ctx.getEnvironment().getPropertySources().addLast(yamlSource))
                .withBean(ObjectMapper.class, () -> objectMapper)
                .withUserConfiguration(PredictionModelConfig.class)
                .withPropertyValues(
                        "PREDICTION_ENABLED=true",
                        "PREDICTION_PROVIDER_URL=" + customUrl,
                        "fireatlas.prediction.model-id=contract-test-model",
                        "fireatlas.prediction.model-version=0.1.0-contract-fixture",
                        "fireatlas.prediction.prediction-target=ACTIVE_FIRE_OCCURRENCE_PROBABILITY_DAILY_01DEG",
                        "fireatlas.prediction.feature-contract-id=firms-daily-grid-v1-fixture",
                        "fireatlas.prediction.supports-frp=true",
                        "fireatlas.prediction.horizon-unit=DAYS",
                        "fireatlas.prediction.min-lead=1",
                        "fireatlas.prediction.max-lead=7",
                        "fireatlas.prediction.temporal-resolution=P1D",
                        "fireatlas.prediction.spatial-resolution=0.1_DEG",
                        "fireatlas.prediction.coverage-bbox=-60,-10,-50,0",
                        "fireatlas.prediction.artifact-uri=s3://fireatlas-models-test/contract-fixture.onnx",
                        "fireatlas.prediction.artifact-sha256=" + TEST_SHA256,
                        "fireatlas.prediction.training-dataset-id=firms-pa-2020-2024-chronological-fixture",
                        "fireatlas.prediction.evaluation-report-uri=docs/eval/contract-fixture-report.json",
                        "fireatlas.prediction.temporal-split-policy=WALK_FORWARD_CHRONOLOGICAL_2020_2024_TEST_2025",
                        "fireatlas.prediction.baseline-comparison-summary=Supera persistencia e climatologia",
                        "fireatlas.prediction.out-of-sample-metrics.prAuc=0.61",
                        "fireatlas.prediction.approved-by=Comite Cientifico Simulado",
                        "fireatlas.prediction.approval-reference=CONTRACT-TEST-ONLY",
                        "fireatlas.prediction.approved-at=2026-11-13T00:00:00Z"
                )
                .run(context -> {
                    PredictionProvider provider = context.getBean(PredictionProvider.class);
                    assertInstanceOf(HttpPredictionProvider.class, provider);
                    HttpPredictionProvider httpProvider = (HttpPredictionProvider) provider;
                    assertEquals(customUrl, httpProvider.manifest().endpointUri().toString());
                });
    }

    private void assertActivationFails(Consumer<PredictionModelProperties> mutator) {
        PredictionModelProperties props = completeApprovedProperties();
        mutator.accept(props);
        assertThrows(IllegalStateException.class, props::validateForActivation);
    }

    private PredictionService enabledService() {
        PredictionModelProperties props = completeApprovedProperties();
        HttpPredictionProvider provider = new HttpPredictionProvider(props.validateForActivation(), objectMapper);
        return new PredictionService(provider, FIXED_CLOCK);
    }

    private MockMvc buildMockMvc(PredictionService service) {
        PredictionController controller = new PredictionController(service, 31);
        return MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ApiExceptionHandler())
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    private PredictionModelProperties completeApprovedProperties() {
        PredictionModelProperties p = new PredictionModelProperties();
        p.setEnabled(true);
        p.setEndpointUrl("http://127.0.0.1:" + serverPort + "/v1/predict");
        p.setProtocolVersion("v1");
        p.setConnectTimeoutMs(1000);
        p.setReadTimeoutMs(2000);
        p.setMaxResponseBytes(65536);
        p.setMaxPredictions(500);
        p.setModelId("contract-test-model");
        p.setModelVersion("0.1.0-contract-fixture");
        p.setPredictionTarget("ACTIVE_FIRE_OCCURRENCE_PROBABILITY_DAILY_01DEG");
        p.setFeatureContractId("firms-daily-grid-v1-fixture");
        p.setSupportsFrp(true);
        p.setHorizonUnit("DAYS");
        p.setMinLead(1);
        p.setMaxLead(7);
        p.setTemporalResolution("P1D");
        p.setSpatialResolution("0.1_DEG");
        p.setCoverageBbox("-60,-10,-50,0");
        p.setArtifactUri("s3://fireatlas-models-test/contract-fixture-v0.1.0.onnx");
        p.setArtifactSha256(TEST_SHA256);
        p.setTrainingDatasetId("firms-pa-2020-2024-chronological-fixture");
        p.setEvaluationReportUri("docs/eval/contract-fixture-report.json");
        p.setTemporalSplitPolicy("WALK_FORWARD_CHRONOLOGICAL_2020_2024_TEST_2025");
        p.setBaselineComparisonSummary("Supera persistencia e climatologia sazonal no teste de contrato simulado");
        Map<String, Double> metrics = new LinkedHashMap<>();
        metrics.put("prAuc", 0.61);
        metrics.put("brierScore", 0.08);
        p.setOutOfSampleMetrics(metrics);
        p.setApprovedBy("Comite Cientifico Simulado (Apenas Teste de Contrato)");
        p.setApprovalReference("CONTRACT-TEST-ONLY-DO-NOT-USE-IN-PROD");
        p.setApprovedAt(LocalDate.of(2026, 11, 13).atStartOfDay().toInstant(ZoneOffset.UTC));
        return p;
    }

    private static String validSimulatedResponseJson() {
        return """
                {
                  "protocolVersion":"v1",
                  "executionId":"SIM-EXEC-001",
                  "modelId":"contract-test-model",
                  "modelVersion":"0.1.0-contract-fixture",
                  "predictionTarget":"ACTIVE_FIRE_OCCURRENCE_PROBABILITY_DAILY_01DEG",
                  "validatedHorizon":{"unit":"DAYS","minLead":1,"maxLead":7,"temporalResolution":"P1D","spatialResolution":"0.1_DEG"},
                  "asOf":"2026-11-13T12:00:00Z",
                  "predictedInterval":{"start":"2026-11-14","end":"2026-11-15"},
                  "coverage":{"status":"SUFFICIENT","observedCoverageRatio":0.94},
                  "evaluationSummary":{
                    "datasetId":"firms-pa-2020-2024-chronological-fixture",
                    "temporalSplitPolicy":"WALK_FORWARD_CHRONOLOGICAL_2020_2024_TEST_2025",
                    "metrics":{"prAuc":0.61,"brierScore":0.08}
                  },
                  "predictions":[
                    {
                      "lat":-3.45,
                      "lon":-53.55,
                      "forecastTimestamp":"2026-11-14T15:00:00Z",
                      "occurrenceProbability":0.72,
                      "predictedFrpMw":14.5,
                      "frpUnit":"MW",
                      "confidenceLevel":"EXPERIMENTAL_CONTRACT_FIXTURE"
                    },
                    {
                      "lat":-3.60,
                      "lon":-53.40,
                      "forecastTimestamp":"2026-11-15T15:00:00Z",
                      "occurrenceProbability":0.38,
                      "predictedFrpMw":6.2,
                      "frpUnit":"MW",
                      "confidenceLevel":"EXPERIMENTAL_CONTRACT_FIXTURE"
                    }
                  ]
                }
                """;
    }

    private static String hoursResponseJson(String forecastTs, String start, String end) {
        return """
                {
                  "protocolVersion":"v1",
                  "executionId":"SIM-EXEC-HOURS",
                  "modelId":"contract-test-model",
                  "modelVersion":"0.1.0-contract-fixture",
                  "predictionTarget":"ACTIVE_FIRE_OCCURRENCE_PROBABILITY_DAILY_01DEG",
                  "validatedHorizon":{"unit":"HOURS","minLead":6,"maxLead":12,"temporalResolution":"PT1H","spatialResolution":"0.1_DEG"},
                  "asOf":"2026-11-13T12:00:00Z",
                  "predictedInterval":{"start":"%s","end":"%s"},
                  "coverage":{"status":"SUFFICIENT","observedCoverageRatio":0.95},
                  "predictions":[
                    {
                      "lat":-3.45,
                      "lon":-53.55,
                      "forecastTimestamp":"%s",
                      "occurrenceProbability":0.65,
                      "predictedFrpMw":11.0,
                      "frpUnit":"MW",
                      "confidenceLevel":"EXPERIMENTAL_CONTRACT_FIXTURE"
                    }
                  ]
                }
                """.formatted(start, end, forecastTs);
    }

    private static void writeJson(HttpExchange exchange, int status, String body) {
        try {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            exchange.close();
        }
    }
}
