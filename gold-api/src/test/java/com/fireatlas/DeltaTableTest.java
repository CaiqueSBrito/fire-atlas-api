package com.fireatlas;

import com.fireatlas.ingestion.integration.DeltaTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Leitura do estado de uma tabela Delta (arquivos ativos) a partir do log e do checkpoint, sem banco. */
class DeltaTableTest {

    private static final String PREFIX = "gold/map_grid/";

    private static List<GoldFixtures.Row> rows(int res, String day, long n) {
        return List.of(GoldFixtures.Row.viirs(res, TestH3.cell(-10.5, -55.5, res), day, n, 5.5));
    }

    @Test
    void onlyActiveFilesCountAfterAnOverwrite(@TempDir Path dir) throws Exception {
        var table = GoldFixtures.table(dir, PREFIX)
                .append(rows(3, "2026-09-02", 1))
                .append(rows(3, "2026-09-02", 2).stream().map(r -> r).toList());
        // versao 1 acrescentou outro arquivo na mesma particao; a versao 2 sobrescreve a particao inteira
        table.overwrite(3, 2026, rows(3, "2026-09-03", 9));

        var snap = DeltaTable.load(new GoldFixtures.LocalStorage(dir), PREFIX);
        assertEquals(2, snap.version());
        assertEquals(1, snap.files().size());
        var f = snap.files().get(0);
        assertEquals(3, f.resolution());
        assertEquals(2026, f.year());
        assertEquals(1L, f.numRecords());
        assertEquals(LocalDate.of(2026, 9, 3), f.minDay());
        assertEquals(LocalDate.of(2026, 9, 3), f.maxDay());
        assertTrue(Files.exists(dir.resolve(f.key())), "a chave aponta para o arquivo ativo");
    }

    @Test
    void checkpointPlusLaterCommitsGiveTheSameStateAsFullReplay(@TempDir Path dir) throws Exception {
        var table = GoldFixtures.table(dir, PREFIX)
                .append(rows(3, "2026-09-02", 1))
                .append(rows(4, "2026-09-02", 1));
        table.checkpoint();
        table.append(rows(5, "2026-09-02", 1));          // depois do checkpoint
        table.overwrite(3, 2026, rows(3, "2026-09-04", 7)); // remove um arquivo que estava no checkpoint

        var storage = new GoldFixtures.LocalStorage(dir);
        var withCheckpoint = DeltaTable.load(storage, PREFIX);
        assertEquals(3, withCheckpoint.version());
        assertEquals(List.of(3, 4, 5), withCheckpoint.files().stream().map(DeltaTable.DataFile::resolution)
                .sorted().toList());
        assertEquals(LocalDate.of(2026, 9, 4), withCheckpoint.files().stream()
                .filter(f -> f.resolution() == 3).findFirst().orElseThrow().minDay());

        // sem o checkpoint, o replay completo do log chega ao mesmo estado
        try (var files = Files.walk(dir.resolve(PREFIX).resolve("_delta_log"))) {
            for (Path p : files.filter(x -> x.toString().contains("checkpoint")).toList()) Files.delete(p);
        }
        var replay = DeltaTable.load(storage, PREFIX);
        assertEquals(withCheckpoint.files().stream().map(DeltaTable.DataFile::key).sorted().toList(),
                replay.files().stream().map(DeltaTable.DataFile::key).sorted().toList());
    }

    @Test
    void tableWithoutLogOrWithGapIsRejected(@TempDir Path dir) throws Exception {
        var storage = new GoldFixtures.LocalStorage(dir);
        assertThrows(IOException.class, () -> DeltaTable.load(storage, PREFIX)); // nao e uma tabela Delta

        var table = GoldFixtures.table(dir, PREFIX).append(rows(3, "2026-09-02", 1))
                .append(rows(4, "2026-09-02", 1)).append(rows(5, "2026-09-02", 1));
        Files.delete(dir.resolve(PREFIX).resolve("_delta_log").resolve(String.format("%020d.json", 1)));
        var ex = assertThrows(IOException.class, () -> DeltaTable.load(storage, PREFIX));
        assertTrue(ex.getMessage().contains("incompleto"), ex.getMessage());
    }

    @Test
    void unsupportedReaderProtocolIsRejected(@TempDir Path dir) throws Exception {
        GoldFixtures.table(dir, PREFIX).append(rows(3, "2026-09-02", 1));
        Path v0 = dir.resolve(PREFIX).resolve("_delta_log").resolve(String.format("%020d.json", 0));
        Files.writeString(v0, Files.readString(v0).replace("\"minReaderVersion\":1", "\"minReaderVersion\":3"));
        var ex = assertThrows(IOException.class,
                () -> DeltaTable.load(new GoldFixtures.LocalStorage(dir), PREFIX));
        assertTrue(ex.getMessage().contains("nao suportados"), ex.getMessage());
    }
}
