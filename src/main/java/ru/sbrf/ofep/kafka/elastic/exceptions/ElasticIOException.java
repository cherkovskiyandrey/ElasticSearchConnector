package ru.sbrf.ofep.kafka.elastic.exceptions;

public class ElasticIOException extends RecoverableException {

    private static final long serialVersionUID = -929439659255371487L;

    public ElasticIOException(Long offset, String message) {
        super(offset, message);
    }

    public ElasticIOException(Long offset, String message, Throwable cause) {
        super(offset, message, cause);
    }
}
