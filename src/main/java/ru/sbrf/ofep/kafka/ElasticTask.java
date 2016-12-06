package ru.sbrf.ofep.kafka;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbrf.ofep.kafka.config.ConfigParam;
import ru.sbrf.ofep.kafka.elastic.DocumentConverter;
import ru.sbrf.ofep.kafka.elastic.ElasticsearchClient;
import ru.sbrf.ofep.kafka.elastic.exceptions.ExceptionHelper;
import ru.sbrf.ofep.kafka.elastic.transportclient.TransportClientBase;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static ru.sbrf.ofep.kafka.ElasticConnector.CONFIG_DEF;


//TODO: логи + конфиги + схема + новый подход к обработке исключений
//Новый подход: цель всё-таки нам нужна устойчивая система, т.к. в противном случае кто её будет перезагружать?
//Предлагаю ввести 2 типа исключений: Recoverable и Unrecoverable
//Recoverable - потеря соединения с серверу elastic или БД
//Unrecoverable - все ошибки в конфигурации, в том числе и позно задетектированные
//Следовательно
// * допускается поздняя инициализация,
// * если грохнули таблицу в БД - Unrecoverable
// * что если в батче экспорта есть ошибки не связанные с разрывом соединения - то пишем лог, иначе на переповтор:
public class ElasticTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticTask.class);

    private final Map<TopicPartition, PartitionConsumer> consumers = new HashMap<>();
    private AbstractConfig configuration;
    private ElasticsearchClient elasticsearchClient;
    private DocumentConverter documentConverter;
    private ErrorPolicy errorPolicy;
    private boolean init = false;

    private static void initElastic(AbstractConfig configuration, ElasticsearchClient elasticsearchClient) {
        final String index = configuration.getString(ConfigParam.DEFAULT_INDEX.getName()) != null ?
                configuration.getString(ConfigParam.DEFAULT_INDEX.getName()) :
                configuration.getList("topics").get(0);

        elasticsearchClient.createIndexIfNotExists(index);

        final String type = configuration.getString(ConfigParam.DEFAULT_TYPE.getName());
        final String mapping = configuration.getString(ConfigParam.MAPPING.getName());

        if(type != null && mapping != null) {
            elasticsearchClient.putMappingIfNotExists(index, type, mapping);
        }
    }

    @Override
    public String version() {
        return new ElasticConnector().version();
    }

    //For tests
    public void start(Map<String, String> props, ElasticsearchClient elasticsearchClient,
                      DocumentConverter documentConverter, ErrorPolicy errorPolicy) {
        LOG.info("Starting task with properties: " + props);
        this.configuration = new AbstractConfig(CONFIG_DEF, props);
        this.elasticsearchClient = elasticsearchClient;
        this.documentConverter = documentConverter;
        this.errorPolicy = errorPolicy;
        initIfNeed();
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("Starting task with properties: " + props);
        try {
            configuration = new AbstractConfig(CONFIG_DEF, props);
            elasticsearchClient = TransportClientBase.newInstance(configuration);
            documentConverter = new DocumentConverter(
                    configuration.getString(ConfigParam.DEFAULT_INDEX.getName()),
                    configuration.getString(ConfigParam.DEFAULT_TYPE.getName()),
                    DocumentConverter.IdMode.of(configuration.getString(ConfigParam.ID_MODE.getName()))
            );
            errorPolicy = ErrorPolicy.of(configuration.getString(ConfigParam.ERROR_POLICY.getName()));
            initIfNeed();
        } catch (Exception e) {
            throw new ConnectException(e);
        }
    }

    private boolean initIfNeed() {
        if (isInit()) {
            return true;
        }
        try {
            initElastic(configuration, elasticsearchClient);
            setInit(true);
        } catch (Exception e) {
            if (ExceptionHelper.isCauseConnectionLost(e)) {
                LOG.warn("Connection to elasticsearch is lost.", e);
            } else {
                throw e;
            }
        }
        return isInit();
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        final Map<TopicPartition, Collection<SinkRecord>> sinkRecordsByPartitions = splitByPartitions(sinkRecords);
        if (!initIfNeed()) {
            final Map<TopicPartition, Long> rewindOffsets = extractBeginOffsets(sinkRecordsByPartitions);
            shiftOffsetsIfNeed(rewindOffsets);
            return;
        }

        final Map<TopicPartition, Future<Long>> waitableRewindOffsets = asyncPutAll(sinkRecordsByPartitions);
        try {
            final Map<TopicPartition, Long> rewindOffsets = waitAllResults(waitableRewindOffsets);
            shiftOffsetsIfNeed(rewindOffsets);
        } catch (Exception e) {
            throw new ConnectException(e);
        }
    }

    private Map<TopicPartition, Long> extractBeginOffsets(Map<TopicPartition, Collection<SinkRecord>> sinkRecordsByPartitions) {
        final Map<TopicPartition, Long> result = new HashMap<>();
        for(Map.Entry<TopicPartition, Collection<SinkRecord>> item : sinkRecordsByPartitions.entrySet()) {
            result.put(item.getKey(), item.getValue().iterator().next().kafkaOffset());
        }
        return result;
    }

    private void shiftOffsetsIfNeed(Map<TopicPartition, Long> offsets) {
        if(!offsets.isEmpty()) {
            context.offset(offsets);
        }
    }

    private Map<TopicPartition, Long> waitAllResults(Map<TopicPartition, Future<Long>> waitableOffsets)
            throws ExecutionException, InterruptedException {

        final Map<TopicPartition, Long> result = new HashMap<>();
        for (Map.Entry<TopicPartition, Future<Long>> partitionResult : waitableOffsets.entrySet()) {
            final Long offset = partitionResult.getValue().get();
            if (offset != null) {
                result.put(partitionResult.getKey(), offset);
            }
        }
        return result;
    }

    private Map<TopicPartition, Future<Long>> asyncPutAll(Map<TopicPartition, Collection<SinkRecord>> sinkRecordsByPartitions) {
        final Map<TopicPartition, Future<Long>> offsets = new HashMap<>();
        for (Map.Entry<TopicPartition, Collection<SinkRecord>> partition : sinkRecordsByPartitions.entrySet()) {
            final PartitionConsumer consumer = createConsumerIfNeed(partition.getKey());
            final Collection<SinkRecord> records = partition.getValue();
            final Future<Long> result = consumer.asyncPut(records);
            offsets.put(partition.getKey(), result);
        }
        return offsets;
    }

    private PartitionConsumer createConsumerIfNeed(TopicPartition key) {
        if (consumers.get(key) == null) {
            consumers.put(key, new PartitionConsumer(elasticsearchClient, documentConverter, errorPolicy));
        }
        return consumers.get(key);
    }

    private Map<TopicPartition, Collection<SinkRecord>> splitByPartitions(Collection<SinkRecord> sinkRecords) {
        final Map<TopicPartition, Collection<SinkRecord>> result = new HashMap<>();
        for (SinkRecord record : sinkRecords) {
            final TopicPartition key = new TopicPartition(record.topic(), record.kafkaPartition());
            if (result.get(key) == null) {
                result.put(key, new LinkedList<SinkRecord>());
            }
            result.get(key).add(record);
        }
        return result;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (!isInit()) {
            throw new ConnectException("Elasticsearch is not init.");
        }
        final Map<TopicPartition, Future<Long>> waitableRewindOffsets = asyncFlushAll();
        try {
            final Map<TopicPartition, Long> rewindOffsets = waitAllResults(waitableRewindOffsets);
            if (!rewindOffsets.isEmpty()) {
                final Map<TopicPartition, Long> newActualOffsets = mergeOffsets(offsets, rewindOffsets);
                shiftOffsetsIfNeed(newActualOffsets);
                throw new ConnectException("This transactional has some errors and new offset has not been applied. Retrying is needed.");
            }
        } catch (Exception e) {
            throw new ConnectException(e);
        }
    }

    private Map<TopicPartition, Long> mergeOffsets(Map<TopicPartition, OffsetAndMetadata> flushedOffsets,
                                                   Map<TopicPartition, Long> rewindOffsets) {
        final Map<TopicPartition, Long> result = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : flushedOffsets.entrySet()) {
            final Long rewindOffset = rewindOffsets.get(offset.getKey());
            result.put(offset.getKey(), rewindOffset != null ? rewindOffset : offset.getValue().offset());
        }
        return result;
    }

    private Map<TopicPartition, Future<Long>> asyncFlushAll() {
        final Map<TopicPartition, Future<Long>> partitions = new HashMap<>();
        for (Map.Entry<TopicPartition, PartitionConsumer> consumer : consumers.entrySet()) {
            final Future<Long> result = consumer.getValue().asyncFlush();
            partitions.put(consumer.getKey(), result);
        }
        return partitions;
    }

    @Override
    public void stop() {
        destroyConsumers();
        destroyFactory();
    }

    private void destroyConsumers() {
        for (Map.Entry<TopicPartition, PartitionConsumer> consumer : consumers.entrySet()) {
            IOUtils.closeQuietly(consumer.getValue());
        }
    }

    private void destroyFactory() {
        Utils.closeQuietly(elasticsearchClient);
    }

    public boolean isInit() {
        return init;
    }

    public void setInit(boolean init) {
        this.init = init;
    }
}
