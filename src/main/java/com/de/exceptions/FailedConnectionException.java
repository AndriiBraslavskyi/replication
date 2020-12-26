package com.de.exceptions;

public class FailedConnectionException extends RuntimeException {
    public FailedConnectionException(String message) {
        super(message);
    }
}
