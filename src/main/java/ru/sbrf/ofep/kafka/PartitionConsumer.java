package ru.sbrf.ofep.kafka;

import org.apache.kafka.connect.sink.SinkRecord;
import ru.sbrf.ofep.kafka.elastic.DocumentConverter;
import ru.sbrf.ofep.kafka.elastic.ElasticWriteStream;
import ru.sbrf.ofep.kafka.elastic.ElasticsearchClient;
import ru.sbrf.ofep.kafka.elastic.domain.FailedDocument;
import ru.sbrf.ofep.kafka.elastic.exceptions.ElasticIOException;
import ru.sbrf.ofep.kafka.elastic.exceptions.ExceptionHelper;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

class PartitionConsumer implements Closeable {
    private final ElasticsearchClient elasticsearchClient;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final DocumentConverter converter;
    private final ErrorPolicy errorPolicy;
    private volatile ElasticWriteStream currentElasticWriteStream;
    private Long currentRewindOffset;

    PartitionConsumer(ElasticsearchClient elasticsearchClient, DocumentConverter converter, ErrorPolicy errorPolicy) {
        this.elasticsearchClient = elasticsearchClient;
        this.converter = converter;
        this.errorPolicy = errorPolicy;
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
        resetCurrentRewindOffset();
        recreateIfNeed();
        try {
            checkFailedDocuments();
            currentElasticWriteStream.write(converter.convertFromSinkRecords(records));
        } catch (ElasticIOException e) {
            return handleError(e);
        }
        return null;
    }

    private Long flush() throws InterruptedException {
        if (!isWriterExists()) {
            return getCurrentRewindOffset();
        }
        try {
            checkFailedDocuments();
            currentElasticWriteStream.flush();
            checkFailedDocuments();
        } catch (ElasticIOException e) {
            return handleError(e);
        }
        return null;
    }

    private Long handleError(ElasticIOException e) {
        assignCurrentRewindOffset();
        destroyWriter();
        return getCurrentRewindOffset();
    }

    private void assignCurrentRewindOffset() {
        currentRewindOffset = currentElasticWriteStream.getAndClearFailedDocuments().iterator().next().getOffset();
    }

    private void resetCurrentRewindOffset() {
        currentRewindOffset = null;
    }

    private List<FailedDocument> checkFailedDocuments() throws ElasticIOException {
        final List<FailedDocument> result = new LinkedList<>();
        for (FailedDocument doc : currentElasticWriteStream.getAndClearFailedDocuments()) {
            if (ExceptionHelper.isCauseConnectionLost(doc.getCause())) {
                throw new ElasticIOException("IO exception has been arise: " + doc.getCause());
            } else {
                errorPolicy.handle(doc);
            }
        }
        return result;
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
