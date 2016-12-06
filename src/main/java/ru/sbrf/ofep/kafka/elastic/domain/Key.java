package ru.sbrf.ofep.kafka.elastic.domain;

import java.util.Objects;

public class Key {
    private final String index;
    private final String type;
    private final String id;

    public Key(String index, String type, String idAsString) {
        this.index = index;
        this.type = type;
        this.id = idAsString;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Key key = (Key) o;
        return Objects.equals(index, key.index) &&
                Objects.equals(type, key.type) &&
                Objects.equals(id, key.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, id);
    }

    @Override
    public String toString() {
        return "Key{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
