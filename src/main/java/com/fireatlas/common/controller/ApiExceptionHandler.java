package com.fireatlas.common.controller;

import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.common.policy.ParameterError;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.async.AsyncRequestNotUsableException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.List;

/**
 * Respostas de erro (RFC 7807). Erros de parametro trazem a lista "errors" com cada parametro rejeitado e o motivo
 * (RN06/CA04); falhas internas nao expõem detalhes tecnicos nem dados de conexao.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    private static ProblemDetail badRequest(String detail, List<ParameterError> errors) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
        pd.setTitle("Parametros invalidos");
        pd.setProperty("errors", errors);
        return pd;
    }

    @ExceptionHandler(InvalidParameterException.class)
    public ProblemDetail invalidParameters(InvalidParameterException e) {
        return badRequest(e.getMessage(), e.errors());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail badRequest(IllegalArgumentException e) {
        return badRequest(e.getMessage(), List.of(new ParameterError("request", e.getMessage())));
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ProblemDetail missing(MissingServletRequestParameterException e) {
        String msg = "parametro obrigatorio ausente: " + e.getParameterName();
        return badRequest(msg, List.of(new ParameterError(e.getParameterName(), "obrigatorio")));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail typeMismatch(MethodArgumentTypeMismatchException e) {
        String msg = "valor invalido para o parametro " + e.getName();
        return badRequest(msg, List.of(new ParameterError(e.getName(), "valor invalido: " + e.getValue())));
    }

    /**
     * RN06: falha de banco e registrada (so a classe e a mensagem do driver) e a resposta e generica,
     * sem URL, usuario ou senha de conexao.
     */
    @ExceptionHandler(org.springframework.dao.DataAccessException.class)
    public ProblemDetail dataAccess(org.springframework.dao.DataAccessException e) {
        log.error("Falha de acesso ao banco: {}", e.getClass().getSimpleName(), e);
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "Base de dados temporariamente indisponivel. Tente novamente em instantes.");
    }

    /** Cliente fechou a conexao no meio da resposta: nao e erro do servidor. */
    @ExceptionHandler(AsyncRequestNotUsableException.class)
    public void clientAborted(AsyncRequestNotUsableException e, HttpServletResponse response) {
        log.debug("Cliente encerrou a conexao: {}", e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail unexpected(Exception e) {
        log.error("Erro inesperado", e);
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR,
                "Erro interno ao processar a consulta. Tente novamente.");
    }
}
