package com.fireatlas.common.policy;

import java.util.List;
import java.util.stream.Collectors;

/** Parametro(s) invalido(s) de uma consulta: a API responde 400 listando cada parametro rejeitado (CA04). */
public class InvalidParameterException extends IllegalArgumentException {

    private final transient List<ParameterError> errors;

    public InvalidParameterException(String parameter, String message) {
        super(message);
        this.errors = List.of(new ParameterError(parameter, message));
    }

    public InvalidParameterException(List<ParameterError> errors) {
        super(errors.stream().map(ParameterError::message).collect(Collectors.joining("; ")));
        this.errors = List.copyOf(errors);
    }

    public List<ParameterError> errors() {
        return errors;
    }
}
