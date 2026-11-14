package com.fireatlas.prediction;

/**
 * Excecao lancada quando o provedor preditivo habilitado falha por indisponibilidade
 * de transporte/rede ou por violacao do contrato do protocolo de inferencia.
 *
 * <p>A mensagem publica nunca deve conter URLs internas, IPs, segredos ou stack traces.
 */
public class PredictionProviderException extends RuntimeException {

    public static final String CODE_PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE";
    public static final String CODE_INVALID_PROVIDER_RESPONSE = "INVALID_PROVIDER_RESPONSE";

    private final String errorCode;
    private final String internalDetail;

    public PredictionProviderException(String errorCode, String publicMessage, String internalDetail, Throwable cause) {
        super(publicMessage, cause);
        this.errorCode = errorCode;
        this.internalDetail = internalDetail;
    }

    public static PredictionProviderException unavailable(String publicMessage, String internalDetail, Throwable cause) {
        return new PredictionProviderException(CODE_PROVIDER_UNAVAILABLE, publicMessage, internalDetail, cause);
    }

    public static PredictionProviderException unavailable(String publicMessage, String internalDetail) {
        return new PredictionProviderException(CODE_PROVIDER_UNAVAILABLE, publicMessage, internalDetail, null);
    }

    public static PredictionProviderException invalidResponse(String publicMessage, String internalDetail, Throwable cause) {
        return new PredictionProviderException(CODE_INVALID_PROVIDER_RESPONSE, publicMessage, internalDetail, cause);
    }

    public static PredictionProviderException invalidResponse(String publicMessage, String internalDetail) {
        return new PredictionProviderException(CODE_INVALID_PROVIDER_RESPONSE, publicMessage, internalDetail, null);
    }

    public String errorCode() {
        return errorCode;
    }

    public String internalDetail() {
        return internalDetail;
    }
}
