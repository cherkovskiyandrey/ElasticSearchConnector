package ru.sbrf.ofep.kafka;

import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import ru.sbrf.ofep.kafka.elastic.DocumentConverter;
import ru.sbrf.ofep.kafka.elastic.ElasticWriteStream;
import ru.sbrf.ofep.kafka.elastic.ElasticsearchClient;
import ru.sbrf.ofep.kafka.elastic.domain.FailedDocument;
import ru.sbrf.ofep.kafka.elastic.exceptions.ElasticIOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static ru.sbrf.ofep.kafka.DataGenerateHelper.*;

public class PartitionConsumerTest {

    @Test
    public void simplePutTest() throws Exception {
        try(final RecourseBundle recourseBundle = new RecourseBundle(ErrorPolicy.FAIL_FAST)) {

            when(recourseBundle.getElasticWriteStream().getAndClearFailedDocuments()).thenReturn(new ArrayList<FailedDocument>());
            assertNull(recourseBundle.getPartitionConsumer().asyncPut(generateSinkSimple(1)).get());

            final ArgumentCaptor<Collection> collectionArgumentCaptor = ArgumentCaptor.forClass(Collection.class);
            verify(recourseBundle.getElasticWriteStream()).write(collectionArgumentCaptor.capture());

            assertEquals(1, collectionArgumentCaptor.getValue().size());
        }
    }

    @Test
    public void putWithConnectionLostErrorTest() throws Exception {
        try(final RecourseBundle recourseBundle = new RecourseBundle(ErrorPolicy.FAIL_FAST)) {
            when(recourseBundle.getElasticWriteStream().getAndClearFailedDocuments())
                    .thenReturn(generateFailedDocs(1, new MasterNotDiscoveredException(), "MasterNotDiscoveredException"));

            assertEquals(0, recourseBundle.getPartitionConsumer().asyncPut(generateSinkSimple(1)).get().longValue());
        }
    }

    @Test
    public void putWithNoConnectionLostErrorAndRetryPolicyTest() throws Exception {
        try(final RecourseBundle recourseBundle = new RecourseBundle(ErrorPolicy.RETRY_FOREVER);) {
            when(recourseBundle.getElasticWriteStream().getAndClearFailedDocuments())
                    .thenReturn(generateFailedDocs(1, new RuntimeException(), "RuntimeException"));

            assertEquals(0, recourseBundle.getPartitionConsumer().asyncPut(generateSinkSimple(1)).get().longValue());

            when(recourseBundle.getElasticWriteStream().getAndClearFailedDocuments()).thenReturn(new ArrayList<FailedDocument>());
            assertNull(recourseBundle.getPartitionConsumer().asyncPut(generateSinkSimple(1)).get());

            verify(recourseBundle.getElasticsearchClient(), times(2)).createNewStream();
        }
    }

    @Test(expected = ExecutionException.class)
    public void putWithNoConnectionLostErrorAndFailPolicyTest() throws Exception {
        try(final RecourseBundle recourseBundle = new RecourseBundle(ErrorPolicy.FAIL_FAST)) {

            when(recourseBundle.getElasticWriteStream().getAndClearFailedDocuments())
                    .thenReturn(generateFailedDocs(1, new RuntimeException(), "RuntimeException"));

            recourseBundle.getPartitionConsumer().asyncPut(generateSinkSimple(1)).get();
        }
    }

    @Test
    public void putWithNoConnectionLostErrorAndLogPolicyTest() throws Exception {
        try(final RecourseBundle recourseBundle = new RecourseBundle(ErrorPolicy.JUST_LOG);) {
            when(recourseBundle.getElasticWriteStream().getAndClearFailedDocuments())
                    .thenReturn(generateFailedDocs(1, new RuntimeException(), "RuntimeException"));

            assertNull(recourseBundle.getPartitionConsumer().asyncPut(generateSinkSimple(1)).get());
        }
    }

    @Test
    public void simpleFlushWithoutDataTest() throws Exception {
        try(final RecourseBundle recourseBundle = new RecourseBundle(ErrorPolicy.FAIL_FAST)) {

            when(recourseBundle.getElasticWriteStream().getAndClearFailedDocuments()).thenReturn(new ArrayList<FailedDocument>());

            assertNull(recourseBundle.getPartitionConsumer().asyncFlush().get());
            verify(recourseBundle.getElasticWriteStream()).flush();
        }
    }

    @Test
    public void flushAndConnectionLostErrorTest() throws Exception {
        try(final RecourseBundle recourseBundle = new RecourseBundle(ErrorPolicy.RETRY_FOREVER)) {

            when(recourseBundle.getElasticWriteStream().getAndClearFailedDocuments())
                    .thenReturn(new ArrayList<FailedDocument>())
                    .thenReturn(generateFailedDocs(1, new MasterNotDiscoveredException(), "MasterNotDiscoveredException"))
            ;

            doThrow(new ElasticIOException("")).when(recourseBundle.getElasticWriteStream()).flush();

            assertEquals(0, recourseBundle.getPartitionConsumer().asyncFlush().get().longValue());
            verify(recourseBundle.getElasticWriteStream()).flush();
        }
    }

    @Test
    public void flushAfterConnectionLostErrorTest() throws Exception {
        try(final RecourseBundle recourseBundle = new RecourseBundle(ErrorPolicy.RETRY_FOREVER)) {

            when(recourseBundle.getElasticWriteStream().getAndClearFailedDocuments())
                    .thenReturn(generateFailedDocs(1, new MasterNotDiscoveredException(), "MasterNotDiscoveredException"));

            assertEquals(0, recourseBundle.getPartitionConsumer().asyncPut(generateSinkSimple(1)).get().longValue());

            assertEquals(0, recourseBundle.getPartitionConsumer().asyncFlush().get().longValue());
            verify(recourseBundle.getElasticWriteStream(), times(0)).flush();
        }
    }

    private class RecourseBundle implements AutoCloseable {
        private final ElasticsearchClient elasticsearchClient = mock(ElasticsearchClient.class);
        private final ElasticWriteStream elasticWriteStream = mock(ElasticWriteStream.class);
        private final DocumentConverter documentConverter = new DocumentConverter(null, null, DocumentConverter.IdMode.KAFKA_KEY);
        private final PartitionConsumer partitionConsumer;

        RecourseBundle(ErrorPolicy errorPolicy) {
            when(elasticsearchClient.createIndexIfNotExists(anyString())).thenReturn(true);
            when(elasticsearchClient.putMappingIfNotExists(anyString(), anyString(), anyString())).thenReturn(true);
            when(elasticsearchClient.createNewStream()).thenReturn(elasticWriteStream);
            partitionConsumer = new PartitionConsumer(elasticsearchClient, documentConverter, errorPolicy);
        }

        ElasticsearchClient getElasticsearchClient() {
            return elasticsearchClient;
        }

        ElasticWriteStream getElasticWriteStream() {
            return elasticWriteStream;
        }

        DocumentConverter getDocumentConverter() {
            return documentConverter;
        }

        PartitionConsumer getPartitionConsumer() {
            return partitionConsumer;
        }

        @Override
        public void close() throws Exception {
            partitionConsumer.close();
        }
    }

}















