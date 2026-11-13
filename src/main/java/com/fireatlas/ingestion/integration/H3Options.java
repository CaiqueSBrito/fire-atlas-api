package com.fireatlas.ingestion.integration;

/**
 * Como a importacao le o indice H3 de cada deteccao, que vem PRONTO da fonte (o backend nao o calcula).
 * <ul>
 *   <li>{@code column}: coluna unica com o H3 (hexadecimal ou inteiro de 64 bits), quando a fonte traz uma so;</li>
 *   <li>{@code baseResolution}: resolucao do indice guardado (a mais fina que a fonte entrega). Quando a fonte traz
 *       uma coluna por resolucao (h3_0 ... h3_6), a coluna dessa resolucao e a usada e as mais grossas sao
 *       conferidas contra o "pai" dela.</li>
 * </ul>
 */
public record H3Options(String column, int baseResolution) {

    /** Padrao: coluna "h3", resolucao-base 8. */
    public static final H3Options DEFAULT = new H3Options("h3", 8);

    public H3Options {
        column = column == null || column.isBlank() ? "h3" : column.trim().toLowerCase();
        if (baseResolution < 0 || baseResolution > 15) {
            throw new IllegalArgumentException("resolucao-base H3 deve estar entre 0 e 15: " + baseResolution);
        }
    }

    public H3Options(String column) {
        this(column, DEFAULT.baseResolution());
    }
}
