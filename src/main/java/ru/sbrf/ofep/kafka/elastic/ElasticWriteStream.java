package ru.sbrf.ofep.kafka.elastic;

import ru.sbrf.ofep.kafka.elastic.domain.Document;
import ru.sbrf.ofep.kafka.elastic.domain.FailedDocument;
import ru.sbrf.ofep.kafka.elastic.exceptions.ElasticIOException;

import java.io.Closeable;
import java.util.Collection;


public interface ElasticWriteStream extends AutoCloseable {

    /**
     * Try async send documents.
     * If inner queue is full, then wait when queue space will be suitable for save
     * input documents.
     *
     * @param documents
     * @throws ElasticIOException if some unrecoverable problems has been happened (for example connection has been lost)
     * @throws InterruptedException if this thread haas been interrupted
     */
    void write(Collection<Document> documents) throws ElasticIOException, InterruptedException;;

    /**
     * Sync flush all documents from inner buffer.
     *
     * @return
     * @throws ElasticIOException
     * @throws InterruptedException
     */
    void flush() throws ElasticIOException, InterruptedException;

    /**
     * Return current failed documents and forget about it.
     *
     * @return
     */
    Collection<FailedDocument> getAndClearFailedDocuments();

}
