package com.fireatlas.gold;

import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.bootstrap.repository.BootstrapRepository;
import com.fireatlas.gold.bootstrap.repository.ForecastCacheRepository;
import com.fireatlas.gold.bootstrap.service.ForecastCacheService;
import com.fireatlas.gold.common.policy.SourceUnavailableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@Timeout(10)
class ForecastCacheServiceTest {
    private static BootstrapStatus state(String state) {
        boolean ready = state.equals("ready");
        return new BootstrapStatus(state, ready ? 3 : 0, ready ? 3 : 0, ready ? 120 : 0,
                state, null, ready ? "ready" : "preparing", ready ? 2 : 0, ready ? 2 : 0);
    }

    @Test
    void getRemainsPassiveAndExplainsWhyAnEmptyForecastIsWaiting() {
        var annual = mock(BootstrapRepository.class);
        var repository = mock(ForecastCacheRepository.class);
        when(repository.status()).thenReturn(state("idle"));
        var service = new ForecastCacheService(repository, annual);
        try {
            for (int poll = 0; poll < 5; poll++) {
                var status = service.status();
                assertEquals("idle", status.state());
                assertEquals("preparing", status.phase());
                assertTrue(status.message().contains("historicos"));
                assertEquals(0, status.completedUnits());
            }
            verifyNoInteractions(annual);
            verify(repository, never()).claim();
            verify(repository, never()).load(any());
        } finally {
            service.close();
        }
    }

    @Test
    void manualStartCannotDiscoverOrClaimForecastBefore2026IsReady() throws Exception {
        var annual = mock(BootstrapRepository.class);
        var repository = mock(ForecastCacheRepository.class);
        when(repository.status()).thenReturn(state("idle"));
        var service = new ForecastCacheService(repository, annual);
        try {
            for (String pending : List.of("idle", "loading", "failed")) {
                when(annual.status(2026)).thenReturn(state(pending));
                var status = service.start();
                assertEquals("idle", status.state());
                assertTrue(status.message().contains("2026"));
                assertEquals("preparing", status.phase());
            }
            verify(annual, never()).historicalYears();
            verify(repository, never()).claim();
            verify(repository, never()).load(any());
        } finally {
            service.close();
        }
    }

    @Test
    void manualStartWaitsForEveryActualHistoricalYearThenPublishesInBackground() throws Exception {
        var annual = mock(BootstrapRepository.class);
        var repository = mock(ForecastCacheRepository.class);
        var lease = mock(ForecastCacheRepository.Lease.class);
        Map<Integer, BootstrapStatus> years = new ConcurrentHashMap<>();
        years.put(2026, state("ready"));
        years.put(2025, state("loading"));
        years.put(2024, state("ready"));
        years.put(2021, state("idle"));
        when(annual.status(anyInt())).thenAnswer(call -> years.get(call.getArgument(0)));
        when(annual.historicalYears()).thenReturn(List.of(2025, 2024, 2021));
        var forecast = new AtomicReference<>(state("idle"));
        when(repository.status()).thenAnswer(call -> forecast.get());
        when(repository.claim()).thenAnswer(call -> {
            forecast.set(state("loading"));
            return lease;
        });
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        doAnswer(call -> {
            entered.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            forecast.set(state("ready"));
            return null;
        }).when(repository).load(lease);
        var service = new ForecastCacheService(repository, annual);
        try {
            assertEquals("idle", service.start().state());
            verify(repository, never()).claim();
            years.put(2025, state("ready"));
            assertEquals("idle", service.start().state());
            verify(repository, never()).claim();
            years.put(2021, state("ready"));
            assertEquals("loading", service.start().state());
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            for (int visitor = 0; visitor < 5; visitor++) {
                assertEquals("loading", service.start().state());
                assertEquals("loading", service.status().state());
            }
            verify(repository, times(1)).claim();
            verify(annual, times(3)).historicalYears();
            verify(annual, never()).status(2023);
        } finally {
            release.countDown();
            service.close();
        }
        assertEquals("ready", service.status().state());
        assertEquals(120, service.status().rowsLoaded());
        verify(repository, times(1)).load(lease);
    }

    @Test
    void manualStartPreservesPublishedForecastWhileHistoricalYearIsPending() throws Exception {
        var annual = mock(BootstrapRepository.class);
        var repository = mock(ForecastCacheRepository.class);
        var published = state("ready");
        when(repository.status()).thenReturn(published);
        when(annual.status(2026)).thenReturn(state("ready"));
        when(annual.historicalYears()).thenReturn(List.of(2025));
        when(annual.status(2025)).thenReturn(state("failed"));
        var service = new ForecastCacheService(repository, annual);
        try {
            assertEquals(published, service.start());
            assertEquals(published, service.status());
            verify(repository, never()).claim();
            verify(repository, never()).load(any());
        } finally {
            service.close();
        }
    }

    @Test
    void sharedAnnualLockContentionReturnsWaitingWithoutResettingDurableProgress() throws Exception {
        var annual = mock(BootstrapRepository.class);
        var repository = mock(ForecastCacheRepository.class);
        when(annual.status(2026)).thenReturn(state("ready"));
        when(annual.historicalYears()).thenReturn(List.of());
        when(repository.status()).thenReturn(new BootstrapStatus("failed", 2, 4, 80,
                "interrompida", "interrupcao", "importing", 0, 2));
        when(repository.claim()).thenReturn(null);
        var service = new ForecastCacheService(repository, annual);
        try {
            var waiting = service.start();
            assertEquals("idle", waiting.state());
            assertEquals("preparing", waiting.phase());
            assertTrue(waiting.message().contains("fila compartilhada"));
            assertEquals(2, waiting.completedUnits());
            assertEquals(4, waiting.totalUnits());
            assertEquals(80, waiting.rowsLoaded());
            assertNull(waiting.error());
            verify(repository, never()).load(any());
        } finally {
            service.close();
        }
    }

    @Test
    void anotherForecastOwnerRemainsLoadingWhenClaimIsUnavailable() throws Exception {
        var annual = mock(BootstrapRepository.class);
        var repository = mock(ForecastCacheRepository.class);
        when(annual.status(2026)).thenReturn(state("ready"));
        when(annual.historicalYears()).thenReturn(List.of());
        var running = state("loading");
        when(repository.status()).thenReturn(running);
        var service = new ForecastCacheService(repository, annual);
        try {
            assertEquals(running, service.start());
            verify(repository, never()).load(any());
        } finally {
            service.close();
        }
    }

    @Test
    void unavailableHistoricalManifestCannotStartForecast() throws Exception {
        var annual = mock(BootstrapRepository.class);
        var repository = mock(ForecastCacheRepository.class);
        when(repository.status()).thenReturn(state("idle"));
        when(annual.status(2026)).thenReturn(state("ready"));
        when(annual.historicalYears()).thenThrow(new IOException("manifesto indisponivel"));
        var service = new ForecastCacheService(repository, annual);
        try {
            assertThrows(SourceUnavailableException.class, service::start);
            verify(repository, never()).claim();
            verify(repository, never()).load(any());
        } finally {
            service.close();
        }
    }
}
