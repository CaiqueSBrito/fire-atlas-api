package com.fireatlas.lake.integration;

import com.fireatlas.gold.common.policy.SourceUnavailableException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.QueryExecutionStatistics;
import software.amazon.awssdk.services.athena.model.QueryExecutionStatus;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AthenaCommandTest {
    @Test
    void commandUsesDurableTokenAndDisabledReuseAndReturnsOnlyManifestLocation() {
        var client = mock(AthenaClient.class);
        var query = new AthenaQuery(client, "fireatlas_gold", "fireatlas-api", 1440, 30000);
        when(client.startQueryExecution(any(StartQueryExecutionRequest.class)))
                .thenReturn(StartQueryExecutionResponse.builder().queryExecutionId("query-id").build());
        when(client.getQueryExecution(any(GetQueryExecutionRequest.class))).thenReturn(execution(
                QueryExecutionState.SUCCEEDED, null, "s3://results/query-id-manifest.csv"));
        assertEquals("query-id", query.startCommand("UNLOAD (SELECT day FROM map_grid) TO 's3://results/export/' "
                + "WITH (format='PARQUET',compression='ZSTD')", "12345678-1234-1234-1234-123456789012"));
        var command = query.awaitCommand("query-id", 300000);
        assertEquals("s3://results/query-id-manifest.csv", command.manifestLocation());
        var captured = ArgumentCaptor.forClass(StartQueryExecutionRequest.class);
        verify(client).startQueryExecution(captured.capture());
        assertEquals("12345678-1234-1234-1234-123456789012", captured.getValue().clientRequestToken());
        assertFalse(captured.getValue().resultReuseConfiguration().resultReuseByAgeConfiguration().enabled());
        assertEquals("fireatlas-api", captured.getValue().workGroup());
        assertEquals("fireatlas_gold", captured.getValue().queryExecutionContext().database());
        verify(client, never()).getQueryResults(any(GetQueryResultsRequest.class));
        verify(client, never()).getQueryResultsPaginator(any(GetQueryResultsRequest.class));
    }

    @Test
    void commandPreservesExactTerminalPermissionReasonRatherThanInventingSuccess() {
        var client = mock(AthenaClient.class);
        var query = new AthenaQuery(client, "gold", "api", 1440, 30000);
        when(client.getQueryExecution(any(GetQueryExecutionRequest.class))).thenReturn(execution(
                QueryExecutionState.FAILED, "AccessDenied: s3:PutObject on s3://results/export", null));
        var error = assertThrows(AthenaQuery.QueryFailedException.class, () -> query.awaitCommand("denied", 300000));
        assertTrue(error.getMessage().contains("AccessDenied: s3:PutObject"));
        verify(client, never()).getQueryResults(any(GetQueryResultsRequest.class));
    }

    @Test
    void successWithoutManifestFailsClosed() {
        var client = mock(AthenaClient.class);
        var query = new AthenaQuery(client, "gold", "api", 1440, 30000);
        when(client.getQueryExecution(any(GetQueryExecutionRequest.class))).thenReturn(execution(
                QueryExecutionState.SUCCEEDED, null, null));
        assertThrows(SourceUnavailableException.class, () -> query.awaitCommand("missing", 300000));
    }

    @Test
    void interruptionCancelsRemoteCommandAndPreservesInterruptFlag() {
        var client = mock(AthenaClient.class);
        var query = new AthenaQuery(client, "gold", "api", 1440, 30000);
        when(client.getQueryExecution(any(GetQueryExecutionRequest.class))).thenReturn(execution(
                QueryExecutionState.RUNNING, null, null));
        try {
            Thread.currentThread().interrupt();
            assertThrows(SourceUnavailableException.class, () -> query.awaitCommand("running", 300000));
            assertTrue(Thread.currentThread().isInterrupted());
            verify(client).stopQueryExecution(any(java.util.function.Consumer.class));
        } finally {
            Thread.interrupted();
        }
    }

    private static GetQueryExecutionResponse execution(QueryExecutionState state, String reason, String manifest) {
        return GetQueryExecutionResponse.builder().queryExecution(QueryExecution.builder()
                .status(QueryExecutionStatus.builder().state(state).stateChangeReason(reason).build())
                .statistics(QueryExecutionStatistics.builder().dataManifestLocation(manifest).build()).build()).build();
    }
}
