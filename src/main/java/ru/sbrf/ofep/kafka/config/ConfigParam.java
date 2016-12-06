package ru.sbrf.ofep.kafka.config;

import org.apache.kafka.common.config.ConfigDef;
import ru.sbrf.ofep.kafka.ErrorPolicy;
import ru.sbrf.ofep.kafka.elastic.DocumentConverter;

public enum ConfigParam {
    CLUSTER_NAME("cluster.name", ConfigDef.Type.STRING, null, null, ConfigDef.Importance.HIGH, "Elasticsearch cluster name`s."),

    CLUSTER_NODES("cluster.nodes", ConfigDef.Type.LIST, null, null, ConfigDef.Importance.HIGH, "List of nodes of elasticsearch cluster. " +
            "Format: host1:port1,host2:port2,..."),

    EXPORT_BUFFER_SIZE("export.buffer.size", ConfigDef.Type.INT, ConfigDef.Range.atLeast(1), 10000, ConfigDef.Importance.LOW,
            "The export buffer between kafka.queue.partition and elasticsearch."),

    EXPORT_BATCH_SIZE("export.batch.size", ConfigDef.Type.INT, ConfigDef.Range.atLeast(1), 1000, ConfigDef.Importance.LOW,
            "The batch size for exporting to elasticsearch."),

    EXPORT_BATCH_TIMEOUT("export.batch.timeout", ConfigDef.Type.INT, ConfigDef.Range.atLeast(10), 30, ConfigDef.Importance.LOW,
            "The timeout for exporting batch to elasticsearch."),

    DEFAULT_INDEX("index.default", ConfigDef.Type.STRING, null, null, ConfigDef.Importance.LOW, "Use this index as elasticsearch default index," +
            " ignore topic name."),

    DEFAULT_TYPE("type.default", ConfigDef.Type.STRING, null, null, ConfigDef.Importance.LOW, "Use this type as elasticsearch default type."),

    ID_MODE("id.mode", ConfigDef.Type.STRING, ConfigDef.ValidString.in(DocumentConverter.IdMode.allValuesName()),
            DocumentConverter.IdMode.KAFKA_DEFAULT.getValueName(), ConfigDef.Importance.LOW,
            "Mode of making id for elasticsearch. KAFKA_KEY - use key value from kafka " +
                    "as id (only for integers and strings). KAFKA_DEFAULT - build id as topic_partition_offset. " +
                    "ELASTIC_DEFAULT - elasticsearch will generate id in its own strategy."),

    MAPPING("mapping", ConfigDef.Type.STRING, null, null, ConfigDef.Importance.LOW, "Mapping for index as JSON string."),

    ERROR_POLICY("error.policy", ConfigDef.Type.STRING, ConfigDef.ValidString.in(ErrorPolicy.allValuesName()),
            ErrorPolicy.RETRY_FOREVER, ConfigDef.Importance.LOW,
            "What to do if exception from elasticsearch is arise which does not connect with transport." +
                    "FAIL_FAST - shutdown current task. JUST_LOG - only write to log and go further." +
                    "RETRY_FOREVER - retry forever."),;

    private final String name;
    private final ConfigDef.Type type;
    private final ConfigDef.Importance importance;
    private final String descriptor;
    private final ConfigDef.Validator validator;
    private final Object defaultValue;

    ConfigParam(String name, ConfigDef.Type type, ConfigDef.Validator validator,
                Object defaultValue, ConfigDef.Importance importance, String descriptor) {
        this.name = name;
        this.type = type;
        this.validator = validator;
        this.defaultValue = defaultValue;
        this.importance = importance;
        this.descriptor = descriptor;
    }

    public static ConfigDef defineAll(ConfigDef orig) {
        ConfigDef result = orig;
        for (ConfigParam c : values()) {
            result = c.define(result);
        }
        return result;
    }

    public String getName() {
        return name;
    }

    public ConfigDef.Type getType() {
        return type;
    }

    public ConfigDef.Importance getImportance() {
        return importance;
    }

    public String getDescriptor() {
        return descriptor;
    }

    public ConfigDef define(ConfigDef orig) {
        return orig.define(name, type, defaultValue, validator, importance, descriptor);
    }
}
