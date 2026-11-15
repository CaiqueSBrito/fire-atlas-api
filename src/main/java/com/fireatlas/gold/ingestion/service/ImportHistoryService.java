package com.fireatlas.gold.ingestion.service;

import com.fireatlas.gold.ingestion.entity.ImportRecord;
import com.fireatlas.gold.ingestion.repository.IngestionRepository;
import org.springframework.stereotype.Service;

import java.util.List;

/** Caso de uso de rastreabilidade: historico das importacoes (concluidas e falhas) do data lake. */
@Service("goldImportHistoryService")
public class ImportHistoryService {

    private final IngestionRepository repository;

    public ImportHistoryService(IngestionRepository repository) {
        this.repository = repository;
    }

    public List<ImportRecord> imports() {
        return repository.findImports();
    }
}
