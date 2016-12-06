package ru.sbrf.ofep.kafka.elastic;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import ru.sbrf.ofep.kafka.elastic.domain.Document;
import ru.sbrf.ofep.kafka.elastic.domain.Key;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class DocumentConverter {
    private final String defaultIndex;
    private final String defaultType;
    private final IdMode idMode;

    public DocumentConverter(String defaultIndex, String defaultType, IdMode idMode) {
        this.defaultIndex = defaultIndex;
        this.defaultType = defaultType;
        this.idMode = idMode;
    }

    public Collection<Document> convertFromSinkRecords(Collection<SinkRecord> sinkRecords) {
        final List<Document> result = new LinkedList<>();
        for (SinkRecord record : sinkRecords) {
            result.add(sinkToDocument(record));
        }
        return result;
    }

    private Document sinkToDocument(SinkRecord record) {
        return new Document(
                extractKey(record),
                extractMetaInfo(record),
                extractContent(record));
    }

    private Key extractKey(SinkRecord record) {
        return new Key(
                StringUtils.isNotEmpty(defaultIndex) ? defaultIndex : record.topic(),
                defaultType,
                idMode.getId(record)
        );
    }

    private Document.MetaInfo extractMetaInfo(SinkRecord record) {
        return new Document.MetaInfo(new TopicPartition(record.topic(), record.kafkaPartition()), record.kafkaOffset());
    }

    private String extractContent(SinkRecord record) {
        switch (record.valueSchema().type()) {
            case STRING:
                return (String) record.value();
            case BYTES:
                return new String((byte[]) record.value(), StandardCharsets.UTF_8);
        }
        throw new DataException(record.valueSchema().type() + " is not supported as the document type.");
    }

    public enum IdMode {
        KAFKA_KEY("KAFKA_KEY") {
            @Override
            public String getId(SinkRecord record) {
                switch (record.keySchema().type()) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case STRING:
                        return String.valueOf(record.key());
                }
                throw new DataException(record.keySchema().type().name() + " is not supported as the document id.");
            }
        },
        KAFKA_DEFAULT("KAFKA_DEFAULT") {
            @Override
            public String getId(SinkRecord record) {
                return record.topic() + "_" + record.kafkaPartition() + "_" + record.kafkaOffset();
            }
        },
        ELASTIC_DEFAULT("ELASTIC_DEFAULT") {
            @Override
            public String getId(SinkRecord record) {
                return null;
            }
        };

        private static final String[] ALL_VALUES_NAME;

        static {
            final List<String> result = new ArrayList<>(values().length);
            for (IdMode idMode : values()) {
                result.add(idMode.getValueName());
            }
            ALL_VALUES_NAME = result.toArray(new String[0]);
        }

        private final String valueName;

        IdMode(String valueName) {
            this.valueName = valueName;
        }

        public static IdMode of(String s) {
            for (IdMode id : values()) {
                if (id.getValueName().equalsIgnoreCase(s)) {
                    return id;
                }
            }
            throw new IllegalArgumentException(s);
        }

        public static String[] allValuesName() {
            return ALL_VALUES_NAME;
        }

        public String getValueName() {
            return valueName;
        }

        public abstract String getId(SinkRecord record);
    }
}
