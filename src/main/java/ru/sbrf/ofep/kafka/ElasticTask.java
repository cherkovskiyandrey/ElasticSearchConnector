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
import ru.sbrf.ofep.kafka.config.ElasticConfig;
import ru.sbrf.ofep.kafka.elastic.convertors.DocumentConverter;
import ru.sbrf.ofep.kafka.elastic.ElasticsearchClient;
import ru.sbrf.ofep.kafka.elastic.transportclient.TransportClientBase;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.lang.String.format;
import static ru.sbrf.ofep.kafka.ElasticConnector.CONFIG_DEF;


public class ElasticTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticTask.class);
    private static final long DEFAULT_RETRY_TIMEOUT_MS = 1000;

    private final Map<TopicPartition, PartitionConsumer> consumers = new HashMap<>();
    private ElasticConfig configuration;
    private ElasticsearchClient elasticsearchClient;
    private DocumentConverter documentConverter;
    private ErrorPolicy errorPolicy;

    @Override
    public String version() {
        return new ElasticConnector().version();
    }

    //For tests
    public void start(Map<String, String> props, ElasticsearchClient elasticsearchClient,
                      DocumentConverter documentConverter, ErrorPolicy errorPolicy) {
        this.configuration = ElasticConfig.of(new AbstractConfig(CONFIG_DEF, props));
        LOG.info("Starting task with configuration: " + configuration);
        this.elasticsearchClient = elasticsearchClient;
        this.documentConverter = documentConverter;
        this.errorPolicy = errorPolicy;
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("Starting task with properties: " + props);
        try {
            configuration = ElasticConfig.of(new AbstractConfig(CONFIG_DEF, props));
            elasticsearchClient = TransportClientBase.newInstance(configuration);
            documentConverter = new DocumentConverter(configuration);
            errorPolicy = configuration.getErrorPolicy();
        } catch (Exception e) {
            LOG.error("Exceptions when start is running: ", e);
            throw new ConnectException(e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        LOG.trace("Put records size: " + sinkRecords.size());
        final Map<TopicPartition, Collection<SinkRecord>> sinkRecordsByPartitions = splitByPartitions(sinkRecords);
        final Map<TopicPartition, Future<Long>> waitableRewindOffsets = asyncPutAll(sinkRecordsByPartitions);

        try {
            final Map<TopicPartition, Long> rewindOffsets = waitAllResults(waitableRewindOffsets);
            LOG.trace("Rewind offset for batch: " + rewindOffsets.toString());
            shiftOffsetsIfNeed(rewindOffsets);
            if(rewindOffsets.size() == sinkRecordsByPartitions.size()) {
                setRetryTimeout();
            }
        } catch (Exception e) {
            LOG.warn("Exceptions when put is running: ", e);
            throw new ConnectException(e);
        }
    }

    private void setRetryTimeout() {
        context.timeout(DEFAULT_RETRY_TIMEOUT_MS);
    }

    private void shiftOffsetsIfNeed(Map<TopicPartition, Long> offsets) {
        if (!offsets.isEmpty()) {
            context.offset(offsets);
        }
    }

    private Map<TopicPartition, Long> waitAllResults(Map<TopicPartition, Future<Long>> waitableOffsets)
            throws ExecutionException, InterruptedException {
        LOG.trace("Begin wait for result of put.");
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
        LOG.trace("Async put all records according to partitions.");
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
            consumers.put(key, new PartitionConsumer(configuration, elasticsearchClient, documentConverter, errorPolicy));
            LOG.trace("Consumer has been created for partition: " + key);
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
        LOG.trace("Split by partitions: " + result.keySet().toString());
        return result;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOG.trace("Begin flush of: " + offsets.toString());
        final Map<TopicPartition, Future<Long>> waitableRewindOffsets = asyncFlushAll();
        try {
            final Map<TopicPartition, Long> rewindOffsets = waitAllResults(waitableRewindOffsets);
            if (!rewindOffsets.isEmpty()) {
                LOG.trace("RewindOffsets after flush is: " + rewindOffsets.toString());
                final Map<TopicPartition, Long> newActualOffsets = mergeOffsets(offsets, rewindOffsets);
                LOG.trace("Rewind offset in flush: " + newActualOffsets.toString());
                shiftOffsetsIfNeed(newActualOffsets);
                throw new ConnectException("This transactional has some errors and new offset has not been applied. Retrying is needed.");
            }
        } catch (Exception e) {
            LOG.warn("Exceptions when flush is running: ", e);
            throw new ConnectException(e);
        }
    }

    private Map<TopicPartition, Long> mergeOffsets(Map<TopicPartition, OffsetAndMetadata> flushedOffsets,
                                                   Map<TopicPartition, Long> rewindOffsets) {
        LOG.trace(format("Begin merge offset: flushedOffsets[%s] and rewindOffsets[%s]", flushedOffsets.toString(), rewindOffsets.toString()));
        final Map<TopicPartition, Long> result = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : flushedOffsets.entrySet()) {
            final Long rewindOffset = rewindOffsets.get(offset.getKey());
            result.put(offset.getKey(), rewindOffset != null ? rewindOffset : offset.getValue().offset());
        }
        return result;
    }

    private Map<TopicPartition, Future<Long>> asyncFlushAll() {
        LOG.trace("Begin async flush of.");
        final Map<TopicPartition, Future<Long>> partitions = new HashMap<>();
        for (Map.Entry<TopicPartition, PartitionConsumer> consumer : consumers.entrySet()) {
            final Future<Long> result = consumer.getValue().asyncFlush();
            partitions.put(consumer.getKey(), result);
        }
        return partitions;
    }

    @Override
    public void stop() {
        LOG.info("Stop of task.");
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
}
