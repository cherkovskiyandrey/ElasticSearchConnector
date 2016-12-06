package ru.sbrf.ofep.kafka.elastic.domain;

public class FailedDocument {
    private final Document original;
    private final Throwable cause;
    private final String error;

    public FailedDocument(Document original, Throwable cause, String error) {
        this.original = original;
        this.cause = cause;
        this.error = error;
    }

    public Long getOffset() {
        return original.getMetaInfo().getOffset();
    }

    public Document getOriginal() {
        return original;
    }

    public String getError() {
        return error;
    }

    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return "FailedDocument{" +
                "original=" + original +
                ", cause=" + cause +
                ", error='" + error + '\'' +
                '}';
    }
}
