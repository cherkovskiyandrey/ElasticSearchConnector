package ru.sbrf.ofep.kafka.elastic.transportclient;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportException;
import ru.sbrf.ofep.kafka.elastic.ElasticWriteStream;
import ru.sbrf.ofep.kafka.elastic.domain.Document;
import ru.sbrf.ofep.kafka.elastic.domain.FailedDocument;
import ru.sbrf.ofep.kafka.elastic.domain.Key;
import ru.sbrf.ofep.kafka.elastic.exceptions.ElasticIOException;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.String.format;

public class TransportClientBaseStream implements ElasticWriteStream, Runnable {
    private final static Document FLUSH_SIGNAL = new Document(null, null, null);
    private final static BulkResponse EMPTY_BULK_RESPONSE = new BulkResponse(new BulkItemResponse[0], 0);

    private final Client client;
    private final ExecutorService executorService;
    private final BlockingQueue<Document> inputQueue;
    private final AtomicReference<CountDownLatch> flushEndResponse = new AtomicReference<>(new CountDownLatch(1));
    private final int batchSize;
    private final TimeValue batchTimeout;
    private final Lock lockForFailedDocs = new ReentrantLock();
    private Queue<FailedDocument> failedDocuments = new LinkedList<>();

    private Future task;

    TransportClientBaseStream(Client client, ExecutorService executorService, int queueSize, int batchSize, TimeValue batchTimeout) {
        this.client = client;
        this.executorService = executorService;
        this.inputQueue = new ArrayBlockingQueue<>(queueSize);
        this.batchSize = batchSize;
        this.batchTimeout = batchTimeout;
    }

    void start() {
        task = executorService.submit(this);
    }

    @Override
    public void write(Collection<Document> documents) throws ElasticIOException, InterruptedException {
        if(task.isDone()) {
            throw new ElasticIOException(null, "Stream is closed");
        }
        for (Document doc : documents) {
            inputQueue.put(doc);
        }
    }

    @Override
    public synchronized void flush() throws ElasticIOException, InterruptedException {
        if(task.isDone()) {
            throw new ElasticIOException(null, "Stream is closed");
        }
        flushEndResponse.set(new CountDownLatch(1));
        inputQueue.put(FLUSH_SIGNAL);
        flushEndResponse.get().await();
    }

    @Override
    public Collection<FailedDocument> getAndClearFailedDocuments() {
        lockForFailedDocs.lock();
        try {
            final Queue<FailedDocument> result = new LinkedList<>(failedDocuments);
            failedDocuments.clear();
            return result;
        } finally {
            lockForFailedDocs.unlock();
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if(!task.cancel(true)) {
            throw new IllegalStateException(format("This task: %s has been already canceled", this));
        }
        flushEndResponse.get().countDown();
    }

    private void putToBatch(BulkRequestBuilder bulkRequest, Document document) {
        final Key key = document.getKey();
        final byte[] source = document.getJsonContent();
        final IndexRequestBuilder createReq = client.prepareIndex(key.getIndex(), key.getType());
        if(key.getId() != null) {
            createReq.setId(key.getId());
        }
        createReq.setSource(source);

        bulkRequest.add(createReq);
    }

    private BulkResponse executeBatch(BulkRequestBuilder bulkRequest) {
        return bulkRequest.numberOfActions() > 0 ? bulkRequest.get(batchTimeout) : EMPTY_BULK_RESPONSE;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            final BulkRequestBuilder bulkRequest = createBulk();
            final Queue<Document> curBatch = new LinkedList<>();
            CountDownLatch notifier = null;
            for (int i = 0; i < batchSize; ++i) {
                final Document document;
                try {
                    document = inputQueue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if(document == FLUSH_SIGNAL) {
                    notifier = flushEndResponse.get();
                    break;
                }
                curBatch.add(document);
                putToBatch(bulkRequest, document);
            }
            try {
                final BulkResponse bulkResponse = executeBatch(bulkRequest);
                makeReport(curBatch, bulkResponse);
            } catch (Exception e) {
                makeReport(curBatch, e);
            }
            if(notifier != null) {
                notifier.countDown();
            }
        }
    }

    private void makeReport(Queue<Document> curBatch, Exception e) {
        final Queue<FailedDocument> failedDocs = new LinkedList<>();
        for(Document doc : curBatch) {
            failedDocs.add(Helper.toFailedDocument(doc, new TransportException(e)));
        }
        addFailedDocuments(failedDocs);
    }

    private BulkRequestBuilder createBulk() {
        final BulkRequestBuilder requestBuilder = client.prepareBulk();
        requestBuilder.setTimeout(batchTimeout); //does not work in some cases
        return requestBuilder;
    }

    private void makeReport(Queue<Document> curBatch, BulkResponse bulkResponse) {
        final Map<Key, Document> map = Helper.asMap(curBatch);
        final Queue<FailedDocument> failedDocs = new LinkedList<>();
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse next : bulkResponse) {
                if (next.isFailed()) {
                    failedDocs.add(Helper.toFailedDocument(map.get(Helper.extractKey(next)), next));
                }
            }
        }
        addFailedDocuments(failedDocs);
    }

    private void addFailedDocuments(Queue<FailedDocument> failedDocs) {
        lockForFailedDocs.lock();
        try {
            failedDocuments.addAll(failedDocs);
        } finally {
            lockForFailedDocs.unlock();
        }
    }
}






