package com.fireatlas.ingestion.integration;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/** Fonte dos arquivos do data lake (hoje: S3). Permite trocar a fonte e simular falhas em testes. */
public interface LakeSource {

    /**
     * Manifesto de integridade do arquivo: linhas esperadas, periodo coberto e sha256 do original.
     * Em CSV bruto vem do validation.json da FIRMS; em Parquet, do rodape do proprio arquivo.
     *
     * @param countComparable true se o numero de linhas lidas deve ser igual a {@code rows} (arquivo lido por
     *                        inteiro); false quando o filtro e aplicado na leitura (Parquet): o arquivo e valido se
     *                        abriu e foi lido sem erro.
     */
    record Manifest(long rows, LocalDate observedMin, LocalDate observedMax, String sha256,
                    boolean countComparable) {
        public Manifest(long rows, LocalDate observedMin, LocalDate observedMax, String sha256) {
            this(rows, observedMin, observedMax, sha256, true);
        }
    }

    /** Chaves dos arquivos de dados (CSV e Parquet) sob o prefixo. */
    List<String> listKeys(String prefix) throws IOException;

    /**
     * Conteudo do arquivo como CSV (cabecalho + linhas). Em Parquet, o filtro de periodo e regiao e aplicado na
     * leitura. O chamador deve fechar o fluxo.
     */
    InputStream open(String key, DetectionCsvLoader.Filter filter) throws IOException;

    /** Manifesto do arquivo, se a fonte publicar um. */
    Optional<Manifest> manifest(String key) throws IOException;

    /** Libera recursos locais do arquivo (ex.: copia temporaria) quando ele nao sera aberto. */
    default void release(String key) {}
}
