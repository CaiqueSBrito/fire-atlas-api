package com.fireatlas.gold.bootstrap.service;

import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.bootstrap.repository.BootstrapRepository;
import com.fireatlas.gold.bootstrap.repository.ForecastCacheRepository;
import com.fireatlas.gold.common.policy.SourceUnavailableException;
import com.fireatlas.lake.integration.AnnualCacheSource;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Previsoes aguardam toda a fila anual; fechar o navegador nao interrompe o COPY. */
@Service
public class ForecastCacheService {
    private static final Logger log = LoggerFactory.getLogger(ForecastCacheService.class);
    private final ForecastCacheRepository repository;
    private final BootstrapRepository annual;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            task -> new Thread(task, "forecast-cache-bootstrap"));
    private boolean closed;
    private boolean scheduled;

    public ForecastCacheService(ForecastCacheRepository repository, BootstrapRepository annual) {
        this.repository = repository;
        this.annual = annual;
    }

    public BootstrapStatus status() {
        var status = repository.status();
        return status.state().equals("idle") ? awaitingHistory(status) : status;
    }

    public synchronized BootstrapStatus start() {
        if (closed) throw new SourceUnavailableException("Servico de carga de previsoes esta encerrando");
        if (scheduled) return status();
        var current = repository.status();
        if (!annual.status(AnnualCacheSource.YEAR).state().equals("ready")) return awaitingHistory(current);
        try {
            for (int year : annual.historicalYears()) {
                if (!annual.status(year).state().equals("ready")) return awaitingHistory(current);
            }
        } catch (IOException e) {
            log.error("Nao foi possivel confirmar os anos historicos antes da previsao", e);
            throw new SourceUnavailableException("Nao foi possivel confirmar a conclusao da carga historica");
        }
        var lease = repository.claim();
        if (lease == null) {
            var status = repository.status();
            if (status.state().equals("ready") || status.state().equals("loading")) return status;
            return waiting(status, "Aguardando liberacao da fila compartilhada de carga anual e de previsoes");
        }
        scheduled = true;
        try {
            executor.execute(new LoadTask(lease));
        } catch (RuntimeException e) {
            scheduled = false;
            lease.close();
            throw e;
        }
        return status();
    }

    private BootstrapStatus awaitingHistory(BootstrapStatus status) {
        return waiting(status, "Aguardando conclusao da carga de 2026 e de todos os anos historicos disponiveis no Delta");
    }

    private BootstrapStatus waiting(BootstrapStatus status, String message) {
        if (status.state().equals("ready")) return status;
        return new BootstrapStatus("idle", status.completedUnits(), status.totalUnits(), status.rowsLoaded(),
                message, null, "preparing", status.finalizationCompleted(), status.finalizationTotal());
    }

    private final class LoadTask implements Runnable {
        private final ForecastCacheRepository.Lease lease;
        private LoadTask(ForecastCacheRepository.Lease lease) { this.lease = lease; }
        @Override public void run() {
            try {
                repository.load(lease);
            } catch (Exception e) {
                log.error("Falha na carga de previsoes; snapshot publicado permanece disponivel", e);
            } finally {
                synchronized (ForecastCacheService.this) { scheduled = false; }
            }
        }
    }

    @PreDestroy
    public void close() {
        synchronized (this) {
            closed = true;
            executor.shutdown();
        }
        boolean interrupted = false;
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) discardQueued();
        } catch (InterruptedException e) {
            interrupted = true;
            discardQueued();
        }
        while (!executor.isTerminated()) {
            try { executor.awaitTermination(1, TimeUnit.DAYS); }
            catch (InterruptedException e) { interrupted = true; discardQueued(); }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private void discardQueued() {
        for (Runnable task : executor.shutdownNow()) {
            if (task instanceof LoadTask load) load.lease.close();
        }
    }
}
