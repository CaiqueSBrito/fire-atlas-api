package com.fireatlas.gold.bootstrap.service;

import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.bootstrap.repository.BootstrapRepository;
import com.fireatlas.gold.common.policy.SourceUnavailableException;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Um executor por processo; o advisory lock no Postgres tambem serializa processos distintos. */
@Service
public class BootstrapService {
    private final BootstrapRepository repository;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "annual-grid-bootstrap-2026");
        return thread;
    });
    private boolean closed;

    public BootstrapService(BootstrapRepository repository) {
        this.repository = repository;
    }

    public BootstrapStatus status() { return repository.status(); }

    public synchronized BootstrapStatus start() {
        if (closed) throw new SourceUnavailableException("Servico de carga anual esta encerrando");
        var lease = repository.claim();
        if (lease != null) {
            try {
                executor.execute(new LoadTask(lease));
            } catch (RuntimeException e) {
                lease.close();
                throw e;
            }
        }
        return repository.status();
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
            if (task instanceof LoadTask load) load.lease.close();
        }
    }

    private final class LoadTask implements Runnable {
        private final BootstrapRepository.Lease lease;

        private LoadTask(BootstrapRepository.Lease lease) { this.lease = lease; }

        @Override public void run() { repository.load(lease); }
    }
}
