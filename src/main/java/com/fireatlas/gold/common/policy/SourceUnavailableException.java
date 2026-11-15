package com.fireatlas.gold.common.policy;

/**
 * A fonte de dados (data lake consultado pelo Athena) falhou ou passou do tempo. Diferente de
 * {@link UnavailableException}, que diz que o recurso nao existe com os dados atuais: aqui o recurso existe e a
 * consulta e que nao completou, entao a resposta e 503 e faz sentido tentar de novo.
 */
public class SourceUnavailableException extends RuntimeException {

    public SourceUnavailableException(String message) {
        super(message);
    }
}
