package com.fireatlas.common.policy;

/** Um parametro rejeitado e o motivo. */
public record ParameterError(String parameter, String message) {}
