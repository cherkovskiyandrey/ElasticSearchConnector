package ru.sbrf.ofep.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import ru.sbrf.ofep.kafka.config.ConfigParam;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticConnector extends SinkConnector {

    public static final String VERSION = AppInfoParser.getVersion();
    public static final ConfigDef CONFIG_DEF = ConfigParam.defineAll(new ConfigDef());

    private final Map<String, String> config = new HashMap<>();

    @Override
    public String version() {
        return VERSION; //TODO
    }

    @Override
    public void start(Map<String, String> props) {
        config.putAll(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ElasticTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Arrays.asList(config);
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSinkConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
