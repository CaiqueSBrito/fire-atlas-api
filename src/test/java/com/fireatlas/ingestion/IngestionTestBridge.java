package com.fireatlas.ingestion;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;

/** Ponte de teste para acionar o gancho interno antes da atualizacao de grade em IngestionRunner. */
public final class IngestionTestBridge {

    private IngestionTestBridge() {}

    public static IngestionRunner.IngestResult ingestOnConnectionWithHook(
            Connection con, InputStream in, String product, String sourceKey,
            DetectionCsvLoader.Filter filter, Runnable beforeGridRefresh) throws IOException, SQLException {
        return IngestionRunner.ingestOnConnection(
                con, in, product, sourceKey, filter, DetectionCsvLoader.NO_EARLY_STOP, beforeGridRefresh);
    }
}
