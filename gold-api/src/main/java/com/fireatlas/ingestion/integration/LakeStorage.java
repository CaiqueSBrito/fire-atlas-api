package com.fireatlas.ingestion.integration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** Armazenamento do data lake, somente leitura (hoje: S3). Permite trocar a fonte e simular falhas em testes. */
public interface LakeStorage {

    /** Chaves de todos os objetos sob o prefixo. */
    List<String> list(String prefix) throws IOException;

    /** Conteudo de um objeto pequeno (ex.: entrada do log do Delta). */
    byte[] read(String key) throws IOException;

    /** Copia o objeto para um arquivo temporario local; o chamador libera com {@link #release(Path)}. */
    Path download(String key) throws IOException;

    /** Apaga a copia temporaria devolvida por {@link #download(String)}. */
    void release(Path file);
}
