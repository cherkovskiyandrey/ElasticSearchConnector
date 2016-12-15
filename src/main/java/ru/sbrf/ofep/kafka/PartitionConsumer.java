package ru.sbrf.ofep.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbrf.ofep.kafka.config.ElasticConfig;
import ru.sbrf.ofep.kafka.elastic.convertors.DocumentConverter;
import ru.sbrf.ofep.kafka.elastic.ElasticWriteStream;
import ru.sbrf.ofep.kafka.elastic.ElasticsearchClient;
import ru.sbrf.ofep.kafka.elastic.domain.Document;
import ru.sbrf.ofep.kafka.elastic.domain.FailedDocument;
import ru.sbrf.ofep.kafka.elastic.exceptions.ElasticIOException;
import ru.sbrf.ofep.kafka.elastic.exceptions.ExceptionHelper;
import ru.sbrf.ofep.kafka.elastic.exceptions.RecoverableException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.*;

import static java.lang.String.format;

class PartitionConsumer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DocumentConverter.class);

    private final ElasticConfig configuration;
    private final ElasticsearchClient elasticsearchClient;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final DocumentConverter converter;
    private final ErrorPolicy errorPolicy;
    private final String type;

    private volatile ElasticWriteStream currentElasticWriteStream;
    private Long currentRewindOffset;

    PartitionConsumer(ElasticConfig configuration,
                      ElasticsearchClient elasticsearchClient,
                      DocumentConverter converter,
                      ErrorPolicy errorPolicy) {
        this.configuration = configuration;
        this.elasticsearchClient = elasticsearchClient;
        this.converter = converter;
        this.errorPolicy = errorPolicy;
        this.type = StringUtils.isEmpty(configuration.getDefaultType()) ? configuration.getDefaultType() : configuration.getTopic().get(0);
        recreateIfNeed();
    }

    Future<Long> asyncPut(final Collection<SinkRecord> records) {
        return executor.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return put(records);
            }
        });
    }

    private Long put(Collection<SinkRecord> records) throws InterruptedException {
        LOG.trace("Begin put records: " + records.size());
        resetCurrentRewindOffset();
        recreateIfNeed();
        try {
            checkFailedDocuments();
            final Collection<Document> documents = convert(records);
            createIndexsesAndPutMappingIfNeed(documents);
            currentElasticWriteStream.write(documents);
        } catch (RecoverableException e) {
            return handleRecoverableError(e);
        }
        return null;
    }

    private void createIndexsesAndPutMappingIfNeed(Collection<Document> documents) throws RecoverableException {
        if (StringUtils.isEmpty(configuration.getMapping())) {
            return;
        }

        final Long firstOffset = documents.isEmpty() ? null : documents.iterator().next().getMetaInfo().getOffset();
        final Set<String> uniqueIndexes = extractIndexes(documents);
        try {
            for (String index : uniqueIndexes) {
                if (elasticsearchClient.createIndexIfNotExists(index)) {
                    LOG.trace(format("Index has been created %s and try to put mapping %s", index, configuration.getMapping()));
                    elasticsearchClient.putMappingIfNotExists(index,
                            type,
                            configuration.getMapping());
                }
            }
        } catch (Exception e) {
            LOG.warn("Problem with create index and put mapping: " + uniqueIndexes, e);
            handleError(e, e.getMessage(), firstOffset);
        }
    }

    private void handleError(Throwable e, String message, Long offset) throws RecoverableException {
        if (ExceptionHelper.isCauseConnectionLost(e)) {
            LOG.warn("Reason is problems with connection: " + e.getMessage());
            throw new ElasticIOException(offset, "IO exception has been arise: " + e.getMessage());
        } else {
            errorPolicy.handle(offset, message, e);
        }
    }

    private Set<String> extractIndexes(Collection<Document> documents) {
        final Set<String> result = new HashSet<>();
        for (Document doc : documents) {
            result.add(doc.getKey().getIndex());
        }
        return result;
    }

    private Collection<Document> convert(Collection<SinkRecord> records) throws RecoverableException {
        final Collection<Document> documents = new LinkedList<>();
        final Long firstOffset = records.isEmpty() ? null : records.iterator().next().kafkaOffset();
        for (SinkRecord record : records) {
            try {
                final Document doc = converter.convert(record);
                if (doc.getJsonContent().length == 0) {
                    LOG.warn(format("Empty document: %s; just ignored.", doc));
                    continue;
                }
                documents.add(doc);
            } catch (Exception e) {
                LOG.warn("Conversion problem with record: " + record, e);
                handleError(e, e.getMessage(), firstOffset);
            }
        }
        return documents;
    }


    private Long flush() throws InterruptedException {
        LOG.trace("Flush records");
        if (!isWriterExists()) {
            LOG.trace("Writer does not exists. Current offset for rewind: " + getCurrentRewindOffset());
            return getCurrentRewindOffset();
        }
        try {
            checkFailedDocuments();
            currentElasticWriteStream.flush();
            checkFailedDocuments();
        } catch (RecoverableException e) {
            return handleRecoverableError(e);
        }
        return null;
    }

    private Long handleRecoverableError(RecoverableException e) {
        LOG.warn("Recoverable error: ", e);
        assignCurrentRewindOffset(e.getOffset());
        destroyWriter();
        return getCurrentRewindOffset();
    }

    private void assignCurrentRewindOffset(Long offset) {
        LOG.trace("Remember current offset for rewind: " + offset);
        currentRewindOffset = offset;
    }

    private void resetCurrentRewindOffset() {
        currentRewindOffset = null;
    }

    private void checkFailedDocuments() throws RecoverableException {
        for (FailedDocument doc : currentElasticWriteStream.getAndClearFailedDocuments()) {
            LOG.warn("Failed document has been found: " + doc);
            handleError(doc.getCause(), doc.toString(), doc.getOffset());
        }
    }

    private void destroyWriter() {
        Utils.closeQuietly(currentElasticWriteStream);
        currentElasticWriteStream = null;
    }

    private boolean isWriterExists() {
        return currentElasticWriteStream != null;
    }

    private void recreateIfNeed() {
        if (!isWriterExists()) {
            LOG.trace("Writer does not exists. Try to create new one.");
            currentElasticWriteStream = elasticsearchClient.createNewStream();
        }
    }

    Future<Long> asyncFlush() {
        return executor.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return flush();
            }
        });
    }

    @Override
    public void close() throws IOException {
        LOG.info("Stopping consumer.");
        executor.shutdownNow();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        destroyWriter();
    }

    private Long getCurrentRewindOffset() {
        return currentRewindOffset;
    }
}
