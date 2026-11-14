package com.fireatlas;

import com.uber.h3core.H3Core;

import java.io.IOException;

/**
 * Somente para testes: faz o papel do pipeline que converte as coordenadas em H3 ANTES de o dado chegar ao S3.
 * O backend nao tem (nem pode ter) essa conversao; os testes precisam dela apenas para fabricar dados de entrada.
 */
final class TestH3 {

    private static final H3Core H3 = init();

    private TestH3() {}

    private static H3Core init() {
        try {
            return H3Core.newInstance();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Celula H3 na resolucao 8 (a que o dado pronto do S3 traz). */
    static long cell(double lat, double lon) {
        return H3.latLngToCell(lat, lon, 8);
    }

    static long cell(double lat, double lon, int res) {
        return H3.latLngToCell(lat, lon, res);
    }
}
