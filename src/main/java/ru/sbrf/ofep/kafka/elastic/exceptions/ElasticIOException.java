package ru.sbrf.ofep.kafka.elastic.exceptions;

public class ElasticIOException extends Exception {

    private static final long serialVersionUID = -929439659255371487L;

    public ElasticIOException(String message) {
        super(message);
    }

    public ElasticIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public ElasticIOException(Throwable cause) {
        super(cause);
    }
}
