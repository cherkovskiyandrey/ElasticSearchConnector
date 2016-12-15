package ru.sbrf.ofep.kafka.elastic.convertors;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbrf.ofep.kafka.KafkaKey;
import ru.sbrf.ofep.kafka.config.ElasticConfig;
import ru.sbrf.ofep.kafka.elastic.domain.Document;
import ru.sbrf.ofep.kafka.elastic.domain.Key;
import ru.sbrf.ofep.kafka.elastic.exceptions.ConvertException;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Date;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

public class DocumentConverter {
    private static final Logger LOG = LoggerFactory.getLogger(DocumentConverter.class);

    private static final Gson GSON = new Gson();

    private final ElasticConfig configuration;

    public DocumentConverter(ElasticConfig configuration) {
        this.configuration = configuration;
    }

    public Document convert(SinkRecord record) throws ConvertException {
        try {
            return new Document(
                    extractKey(record),
                    extractMetaInfo(record),
                    extractContent(record));
        } catch (ParseException e) {
            throw new ConvertException(e);
        }
    }

    private Key extractKey(SinkRecord record) throws ParseException, ConvertException {
        final KafkaKey kafkaKey = getKafkaKey(record);
        final String indexName = StringUtils.isNotEmpty(configuration.getDefaultIndex()) ?
                buildIndexName(configuration.getDefaultIndex(), kafkaKey) :
                buildIndexName(record.topic(), kafkaKey);

        return new Key(
                indexName,
                configuration.getDefaultType() != null ? configuration.getDefaultType() : record.topic(),
                configuration.getIdMode().getId(record, kafkaKey)
        );
    }

    private String buildIndexName(String prefix, KafkaKey kafkaKey) throws ParseException {
        if (kafkaKey == null || kafkaKey.getTimestamp() == null) {
            return prefix;
        }

        final Date inputTimestamp = kafkaKey.getTimestamp();
        final String outputTimestamp = configuration.getGenerateIndexSuffixMode().format(inputTimestamp);

        return prefix + (StringUtils.isNotEmpty(outputTimestamp) ? "_" + outputTimestamp : "");
    }

    private static KafkaKey getKafkaKey(SinkRecord record) {
        if (record.key() == null || record.keySchema() != Schema.BYTES_SCHEMA) {
            return null;
        }
        //TODO: переделать через схему
        try {
            return GSON.fromJson(new String((byte[]) record.key()), KafkaKey.class);
        } catch (JsonSyntaxException e) {
            return null;
        }
    }

    private Document.MetaInfo extractMetaInfo(SinkRecord record) {
        return new Document.MetaInfo(new TopicPartition(record.topic(), record.kafkaPartition()), record.kafkaOffset());
    }

    private byte[] extractContent(SinkRecord record) throws ConvertException {
        switch (record.valueSchema().type()) {
            case STRING:
                return record.value() != null ? ((String) record.value()).getBytes(StandardCharsets.UTF_8) : EMPTY_BYTE_ARRAY;
            case BYTES:
                return record.value() != null ? (byte[]) record.value() : EMPTY_BYTE_ARRAY;
        }
        throw new ConvertException(record.valueSchema().type() + " is not supported as the document type.");
    }


}
