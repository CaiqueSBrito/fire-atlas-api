package com.fireatlas;

import com.fireatlas.h3.policy.H3Index;
import com.fireatlas.ingestion.integration.DetectionCsvLoader;
import com.fireatlas.ingestion.integration.DetectionRowParser;
import com.fireatlas.ingestion.integration.ParquetCsvReader;
import com.fireatlas.region.entity.BBox;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/** Leitura do Parquet da camada ouro (um foco por linha, H3 pronto), sem banco e sem Spring. */
class ParquetCsvReaderTest {

    private static final String HEADER = "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,"
            + "instrument,confidence,version,bright_t31,frp,daynight,h3";

    private static String row(double lat, double lon, String date, int time, String h3) {
        return lat + "," + lon + ",340.0,0.4,0.4," + date + "," + time + ",SNPP,SNPP,n,2.0NRT,290.0,5.5,D," + h3;
    }

    private static String hex(double lat, double lon) {
        return H3Index.toHex(TestH3.cell(lat, lon));
    }

    private static String csv(String... rows) {
        return HEADER + "\n" + String.join("\n", rows) + "\n";
    }

    private static List<String> lines(Path parquet, DetectionCsvLoader.Filter filter) throws IOException {
        try (var in = ParquetCsvReader.openAsCsv(parquet, filter);
             var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.toList());
        }
    }

    private Path sample(Path dir) throws Exception {
        return ParquetFixtures.parquet(dir, "ouro.parquet", csv(
                row(-10.5, -55.5, "2026-09-02", 1750, hex(-10.5, -55.5)),   // dentro do recorte
                row(-10.6, -55.4, "2026-09-03", 105, hex(-10.6, -55.4)),    // dentro (hora sem zero a esquerda)
                row(20.0, 10.0, "2026-09-02", 1750, hex(20.0, 10.0)),       // fora da regiao
                row(-10.5, -55.5, "2026-10-20", 1750, hex(-10.5, -55.5))),  // fora do periodo
                "{'acq_time': 'INTEGER'}");
    }

    @Test
    void readsParquetAsCsvKeepingColumnsAndH3(@TempDir Path dir) throws Exception {
        List<String> all = lines(sample(dir), null);
        assertEquals(5, all.size()); // cabecalho + 4 focos
        assertEquals(HEADER, all.get(0));
        assertTrue(all.get(1).contains(hex(-10.5, -55.5)));
    }

    @Test
    void filtersByPeriodAndRegionWhileReading(@TempDir Path dir) throws Exception {
        var filter = new DetectionCsvLoader.Filter(new BBox(-56, -11, -55, -10),
                LocalDate.of(2026, 9, 2), LocalDate.of(2026, 9, 3));
        List<String> out = lines(sample(dir), filter);
        assertEquals(3, out.size(), "cabecalho + 2 focos: o de fora da regiao e o de fora do periodo nem sao lidos");
    }

    @Test
    void parquetRowsGoThroughTheSameParserAndKeepTheReadyH3(@TempDir Path dir) throws Exception {
        List<String> out = lines(sample(dir), null);
        var parser = new DetectionRowParser(out.get(0), "VIIRS", "VIIRS_S-NPP", "gold/VIIRS_S-NPP/2026/ouro.parquet");
        var r2 = parser.parse(out.get(2));
        assertTrue(r2.accepted(), r2.rejectReason());
        assertEquals(TestH3.cell(-10.6, -55.4), r2.detection().location().h3());
        assertEquals(Instant.parse("2026-09-03T01:05:00Z"), r2.detection().acquiredAt()); // 105 -> 01:05
        assertEquals("gold/VIIRS_S-NPP/2026/ouro.parquet", r2.detection().source().file());
    }

    @Test
    void acceptsH3StoredAsIntegerColumn(@TempDir Path dir) throws Exception {
        long cell = TestH3.cell(-10.5, -55.5);
        Path p = ParquetFixtures.parquet(dir, "inteiro.parquet",
                csv(row(-10.5, -55.5, "2026-09-02", 1750, Long.toString(cell))),
                "{'acq_time': 'INTEGER', 'h3': 'BIGINT'}");
        List<String> out = lines(p, null);
        var parser = new DetectionRowParser(out.get(0), "VIIRS", "VIIRS_S-NPP", "f");
        assertEquals(cell, parser.parse(out.get(1)).detection().location().h3());
    }

    @Test
    void infoReportsFooterRowsAndPeriod(@TempDir Path dir) throws Exception {
        var info = ParquetCsvReader.info(sample(dir));
        assertEquals(4, info.rows());
        assertEquals(LocalDate.of(2026, 9, 2), info.minDate());
        assertEquals(LocalDate.of(2026, 10, 20), info.maxDate());
    }

    @Test
    void corruptedOrNonParquetFileFailsInsteadOfReturningRows(@TempDir Path dir) throws Exception {
        Path bad = dir.resolve("quebrado.parquet");
        Files.writeString(bad, "isto nao e um arquivo parquet");
        assertThrows(IOException.class, () -> ParquetCsvReader.info(bad));
        assertThrows(IOException.class, () -> ParquetCsvReader.openAsCsv(bad, null));

        // arquivo valido cortado no meio (download incompleto)
        Path good = sample(dir);
        byte[] bytes = Files.readAllBytes(good);
        Path truncated = dir.resolve("cortado.parquet");
        Files.write(truncated, java.util.Arrays.copyOf(bytes, bytes.length / 2));
        assertThrows(IOException.class, () -> ParquetCsvReader.info(truncated));
    }
}
