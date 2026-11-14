package com.fireatlas.common.policy;

/** O recurso pedido nao e oferecido com os dados disponiveis (ex.: pontos individuais, que a gold nao tem). */
public class UnavailableException extends RuntimeException {

    public UnavailableException(String message) {
        super(message);
    }
}
