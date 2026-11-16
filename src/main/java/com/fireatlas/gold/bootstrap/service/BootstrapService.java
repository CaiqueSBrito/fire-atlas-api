package com.fireatlas.gold.bootstrap.service;

import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.bootstrap.repository.BootstrapRepository;
import com.fireatlas.gold.common.policy.SourceUnavailableException;
import com.fireatlas.lake.integration.AnnualCacheSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Um executor por processo; o lock compartilhado serializa a carga entre processos e anos. */
@Service
public class BootstrapService {
    private static final Logger log = LoggerFactory.getLogger(BootstrapService.class);
    private final BootstrapRepository repository;
    private final ForecastCacheService forecast;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            runnable -> new Thread(runnable, "annual-grid-bootstrap"));
    private boolean closed;
    private boolean scheduled;

    public BootstrapService(BootstrapRepository repository, ForecastCacheService forecast) {
        this.repository = repository;
        this.forecast = forecast;
    }

    public BootstrapStatus status() { return repository.status(AnnualCacheSource.YEAR); }

    public BootstrapStatus status(int year) { return repository.status(year); }

    public synchronized BootstrapStatus start() {
        if (closed) throw new SourceUnavailableException("Servico de carga anual esta encerrando");
        if (scheduled) return status();
        var lease = repository.claim(AnnualCacheSource.YEAR);
        scheduled = true;
        try {
            executor.execute(new LoadTask(lease));
        } catch (RuntimeException e) {
            scheduled = false;
            if (lease != null) lease.close();
            throw e;
        }
        return status();
    }

    @PreDestroy
    public void close() {
        synchronized (this) {
            closed = true;
            executor.shutdown();
        }
        boolean interrupted = false;
        boolean terminated = false;
        try {
            terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            interrupted = true;
        }
        if (!terminated) {
            discardQueuedLoads();
            while (!executor.isTerminated()) {
                try {
                    executor.awaitTermination(1, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    interrupted = true;
                    discardQueuedLoads();
                }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private void discardQueuedLoads() {
        for (Runnable task : executor.shutdownNow()) {
            if (task instanceof LoadTask load && load.lease != null) load.lease.close();
        }
    }

    private final class LoadTask implements Runnable {
        private final BootstrapRepository.Lease lease;

        private LoadTask(BootstrapRepository.Lease lease) { this.lease = lease; }

        @Override public void run() {
            try {
                if (lease != null) repository.load(lease);
                if (!status().state().equals("ready") || Thread.currentThread().isInterrupted()) return;
                // A descoberta vem do mesmo manifesto Delta usado pelo plano anual, nao do navegador.
                for (int year : repository.historicalYears()) {
                    if (Thread.currentThread().isInterrupted()) return;
                    if (repository.status(year).state().equals("ready")) continue;
                    var historicalLease = repository.claim(year);
                    if (historicalLease == null) {
                        // Outro processo assumiu a fila. Nunca ultrapassar um ano ainda pendente.
                        if (repository.status(year).state().equals("ready")) continue;
                        return;
                    }
                    repository.load(historicalLease);
                    if (!repository.status(year).state().equals("ready")) {
                        log.warn("Carga historica parou em {}; 2026 continua disponivel", year);
                        return;
                    }
                }
                if (Thread.currentThread().isInterrupted()) return;
                try { forecast.start(); }
                catch (Exception e) { log.error("Nao foi possivel iniciar a carga de previsoes", e); }
            } catch (Exception e) {
                log.error("Nao foi possivel continuar a carga historica; os anos prontos continuam disponiveis", e);
            } finally {
                synchronized (BootstrapService.this) {
                    scheduled = false;
                }
            }
        }
    }
}
