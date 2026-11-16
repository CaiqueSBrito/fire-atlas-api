package com.fireatlas.gold;

import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.bootstrap.repository.BootstrapRepository;
import com.fireatlas.gold.bootstrap.repository.ForecastCacheRepository;
import com.fireatlas.gold.bootstrap.service.BootstrapService;
import com.fireatlas.gold.bootstrap.service.ForecastCacheService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@Timeout(10)
class BootstrapServiceTest {
    private static BootstrapStatus state(String state) {
        boolean ready = state.equals("ready");
        return new BootstrapStatus(state, ready ? 1 : 0, 1, ready ? 10 : 0, state, null,
                ready ? "ready" : "importing", ready ? 1 : 0, 1);
    }

    @Test
    void releasesForegroundWhileHistoryRunsSequentiallyAndRepeatedVisitorsDoNotDuplicateWork() throws Exception {
        var repository = mock(BootstrapRepository.class);
        var foreground = mock(BootstrapRepository.Lease.class);
        var year2025 = mock(BootstrapRepository.Lease.class);
        var year2024 = mock(BootstrapRepository.Lease.class);
        Map<Integer, BootstrapStatus> states = new ConcurrentHashMap<>();
        states.put(2026, state("loading"));
        when(repository.status(anyInt())).thenAnswer(call -> states.getOrDefault(call.getArgument(0), state("idle")));
        when(repository.claim(2026)).thenReturn(foreground);
        when(repository.claim(2025)).thenReturn(year2025);
        when(repository.claim(2024)).thenReturn(year2024);
        when(repository.historicalYears()).thenReturn(List.of(2025, 2024));
        List<Integer> order = new CopyOnWriteArrayList<>();
        CountDownLatch entered2025 = new CountDownLatch(1);
        CountDownLatch release2025 = new CountDownLatch(1);
        CountDownLatch entered2024 = new CountDownLatch(1);
        CountDownLatch release2024 = new CountDownLatch(1);
        doAnswer(call -> {
            order.add(2026);
            states.put(2026, state("ready"));
            return null;
        }).when(repository).load(foreground);
        doAnswer(call -> {
            order.add(2025);
            states.put(2025, state("loading"));
            entered2025.countDown();
            assertTrue(release2025.await(5, TimeUnit.SECONDS));
            states.put(2025, state("ready"));
            return null;
        }).when(repository).load(year2025);
        doAnswer(call -> {
            order.add(2024);
            entered2024.countDown();
            assertTrue(release2024.await(5, TimeUnit.SECONDS));
            states.put(2024, state("ready"));
            return null;
        }).when(repository).load(year2024);
        var forecastRepository = mock(ForecastCacheRepository.class);
        var forecastLease = mock(ForecastCacheRepository.Lease.class);
        var forecastState = new AtomicReference<>(state("idle"));
        when(forecastRepository.status()).thenAnswer(call -> forecastState.get());
        when(forecastRepository.claim()).thenAnswer(call -> {
            assertEquals("ready", states.get(2024).state());
            forecastState.set(state("loading"));
            return forecastLease;
        });
        CountDownLatch forecastStarted = new CountDownLatch(1);
        CountDownLatch releaseForecast = new CountDownLatch(1);
        doAnswer(call -> {
            forecastStarted.countDown();
            assertTrue(releaseForecast.await(5, TimeUnit.SECONDS));
            forecastState.set(state("ready"));
            return null;
        }).when(forecastRepository).load(forecastLease);
        var forecast = new ForecastCacheService(forecastRepository, repository);
        var service = new BootstrapService(repository, forecast);
        try {
            service.start();
            assertTrue(entered2025.await(5, TimeUnit.SECONDS));
            assertEquals("ready", service.status().state());
            for (int visitor = 0; visitor < 5; visitor++) assertEquals("ready", service.start().state());
            assertEquals(List.of(2026, 2025), order);
            verify(repository, never()).claim(2024);
            assertEquals("idle", forecast.status().state());
            assertTrue(forecast.status().message().contains("historicos"));
            verify(forecastRepository, never()).claim();
            release2025.countDown();
            assertTrue(entered2024.await(5, TimeUnit.SECONDS));
            assertEquals(List.of(2026, 2025, 2024), order);
            assertEquals("ready", service.status().state());
            assertEquals("idle", forecast.status().state());
            verify(forecastRepository, never()).claim();
            release2024.countDown();
            assertTrue(forecastStarted.await(5, TimeUnit.SECONDS));
            assertEquals("loading", forecast.status().state());
            assertEquals(List.of(2026, 2025, 2024), order);
            verify(repository, times(1)).claim(2026);
            verify(repository, times(1)).claim(2025);
        } finally {
            release2025.countDown();
            release2024.countDown();
            releaseForecast.countDown();
            service.close();
            forecast.close();
        }
        assertEquals("ready", forecast.status().state());
        verify(forecastRepository, times(1)).load(forecastLease);
    }

    @Test
    void failedHistoricalYearDoesNotAdvanceOrTakeForegroundOffline() throws Exception {
        var repository = mock(BootstrapRepository.class);
        var lease = mock(BootstrapRepository.Lease.class);
        when(repository.status(2026)).thenReturn(state("ready"));
        when(repository.status(2025)).thenReturn(state("idle"), state("failed"));
        when(repository.claim(2025)).thenReturn(lease);
        when(repository.historicalYears()).thenReturn(List.of(2025, 2024));
        var forecast = mock(ForecastCacheService.class);
        var service = new BootstrapService(repository, forecast);
        service.start();
        service.close();
        assertEquals("ready", service.status().state());
        verify(repository).load(lease);
        verify(repository, never()).claim(2024);
        verifyNoInteractions(forecast);
    }

    @Test
    void doesNotStartHistoryUntilForegroundHasPublished() throws Exception {
        var repository = mock(BootstrapRepository.class);
        var lease = mock(BootstrapRepository.Lease.class);
        when(repository.claim(2026)).thenReturn(lease);
        when(repository.status(2026)).thenReturn(state("failed"));
        var forecast = mock(ForecastCacheService.class);
        var service = new BootstrapService(repository, forecast);
        service.start();
        service.close();
        verify(repository).load(lease);
        verify(repository, never()).historicalYears();
        verifyNoInteractions(forecast);
    }

    @Test
    void doesNotOvertakeYearClaimedByAnotherProcess() throws Exception {
        var repository = mock(BootstrapRepository.class);
        when(repository.status(2026)).thenReturn(state("ready"));
        when(repository.status(2025)).thenReturn(state("loading"));
        when(repository.historicalYears()).thenReturn(List.of(2025, 2024));
        var forecast = mock(ForecastCacheService.class);
        var service = new BootstrapService(repository, forecast);
        service.start();
        service.close();
        assertEquals("ready", service.status().state());
        verify(repository, never()).claim(2024);
        verifyNoInteractions(forecast);
    }

    @Test
    void interruptedFinalHistoricalOwnerDoesNotStartForecastEvenIfYearPublished() throws Exception {
        var repository = mock(BootstrapRepository.class);
        var lease = mock(BootstrapRepository.Lease.class);
        when(repository.status(2026)).thenReturn(state("ready"));
        when(repository.status(2024)).thenReturn(state("idle"), state("ready"));
        when(repository.claim(2024)).thenReturn(lease);
        when(repository.historicalYears()).thenReturn(List.of(2024));
        doAnswer(call -> {
            Thread.currentThread().interrupt();
            return null;
        }).when(repository).load(lease);
        var forecast = mock(ForecastCacheService.class);
        var service = new BootstrapService(repository, forecast);
        service.start();
        service.close();
        verify(repository).load(lease);
        verifyNoInteractions(forecast);
    }

    @Test
    void foreignForegroundOwnerDoesNotDiscoverHistoryOrStartForecast() throws Exception {
        var repository = mock(BootstrapRepository.class);
        when(repository.status(2026)).thenReturn(state("loading"));
        var forecast = mock(ForecastCacheService.class);
        var service = new BootstrapService(repository, forecast);
        service.start();
        service.close();
        assertEquals("loading", service.status().state());
        verifyNoInteractions(forecast);
        verify(repository, never()).historicalYears();
    }
}
