package com.fireatlas.api;

import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.context.request.async.AsyncRequestNotUsableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail badRequest(IllegalArgumentException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ProblemDetail missing(MissingServletRequestParameterException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "parametro obrigatorio ausente: " + e.getParameterName());
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail typeMismatch(MethodArgumentTypeMismatchException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "valor invalido para o parametro " + e.getName());
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
