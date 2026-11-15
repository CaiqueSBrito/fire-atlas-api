package com.fireatlas.gold.ingestion.entity;

import java.time.LocalDate;

/** Ultima sincronizacao concluida de uma tabela do data lake: versao do Delta e ultimo dia coberto (nulo sem periodo). */
public record SyncState(String table, long version, LocalDate syncedThrough) {}
