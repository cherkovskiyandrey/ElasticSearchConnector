package ru.sbrf.ofep.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbrf.ofep.kafka.elastic.exceptions.RecoverableException;

import java.util.ArrayList;
import java.util.List;

public enum ErrorPolicy {
    FAIL_FAST("FAIL_FAST") {
        @Override
        void handle(Long offset, String message, Throwable cause) throws RecoverableException {
            throw new IllegalStateException("FAIL_FAST: " + message, cause);
        }
    },
    JUST_LOG("JUST_LOG") {
        @Override
        void handle(Long offset, String message, Throwable cause) throws RecoverableException {
            LOG.warn("JUST_LOG: could not export document: " + message, cause);
        }
    },
    RETRY_FOREVER("RETRY_FOREVER") {
        @Override
        void handle(Long offset, String message, Throwable cause) throws RecoverableException {
            throw new RecoverableException(offset, "RETRY_FOREVER: exception has been arise: " + message, cause);
        }
    },;
    private final static Logger LOG = LoggerFactory.getLogger(ErrorPolicy.class);
    private final static String[] ALL_VALUES_NAME;

    static {
        final List<String> result = new ArrayList<>(values().length);
        for (ErrorPolicy e : values()) {
            result.add(e.getName());
        }
        ALL_VALUES_NAME = result.toArray(new String[0]);
    }

    private final String name;

    ErrorPolicy(String name) {
        this.name = name;
    }

    public static String[] allValuesName() {
        return ALL_VALUES_NAME;
    }

    public static ErrorPolicy of(String s) {
        for (ErrorPolicy id : values()) {
            if (id.getName().equalsIgnoreCase(s)) {
                return id;
            }
        }
        throw new IllegalArgumentException(s);
    }

    public String getName() {
        return name;
    }

    abstract void handle(Long offset, String message, Throwable cause) throws RecoverableException;
}
