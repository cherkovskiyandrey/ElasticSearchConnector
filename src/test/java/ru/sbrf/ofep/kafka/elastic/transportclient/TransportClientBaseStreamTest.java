package ru.sbrf.ofep.kafka.elastic.transportclient;

import org.apache.kafka.common.TopicPartition;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import ru.sbrf.ofep.kafka.Utils;
import ru.sbrf.ofep.kafka.elastic.ElasticWriteStream;
import ru.sbrf.ofep.kafka.elastic.domain.Document;
import ru.sbrf.ofep.kafka.elastic.domain.FailedDocument;
import ru.sbrf.ofep.kafka.elastic.domain.Key;
import ru.sbrf.ofep.kafka.elastic.exceptions.ElasticIOException;
import ru.sbrf.ofep.kafka.elastic.exceptions.ExceptionHelper;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TransportClientBaseStreamTest extends ESSingleNodeTestCase {

    @Rule
    public ElasticWriteStreamFactory elasticWriteStreamFactory = new ElasticWriteStreamFactory();

    private static void writeData(ElasticWriteStream api, int num) throws ElasticIOException, InterruptedException {
        for (int i = 0; i < num; ++i) {
            api.write(Arrays.asList(
                    new Document(
                            new Key("test", "test", Integer.toString(i)),
                            new Document.MetaInfo(new TopicPartition("test", 1), i),
                            "{\"message\":\"hello world " + i + "\"}")
            ));
        }
    }

    @Test
    public void writeOneValueAndFlushTest() throws Exception {
        final ElasticWriteStream api = elasticWriteStreamFactory.getTransportClientBaseStream();
        writeData(api, 1);
        api.flush();
        assertTrue(api.getAndClearFailedDocuments().isEmpty());

        assertEquals("hello world 0", elasticWriteStreamFactory.getClient().prepareGet("test", "test", "0")
                .get()
                .getSource()
                .get("message")
        );
    }

    @Test
    public void failedDocumentTest() throws Exception {
        tearDownClass();
        try {
            final ElasticWriteStream api = elasticWriteStreamFactory.getTransportClientBaseStream();
            writeData(api, 1);
            api.flush();

            final Collection<FailedDocument> failedDocs = api.getAndClearFailedDocuments();
            assertFalse(failedDocs.isEmpty());
            assertTrue(api.getAndClearFailedDocuments().isEmpty());
            assertTrue(ExceptionHelper.isCauseConnectionLost(failedDocs.iterator().next().getCause()));
        } finally {
            setUpClass();
        }
    }

    @Test
    public void emptyFlushTest() throws Exception {
        final ElasticWriteStream api = elasticWriteStreamFactory.getTransportClientBaseStream();
        api.flush();
        assertTrue(api.getAndClearFailedDocuments().isEmpty());
    }

    @Test
    public void batchTest() throws Exception {
        final ElasticWriteStream api = elasticWriteStreamFactory.getTransportClientBaseStream();
        writeData(api, 10);
        assertTrue(api.getAndClearFailedDocuments().isEmpty());

        TimeUnit.SECONDS.sleep(7);

        assertEquals(10, elasticWriteStreamFactory.getClient()
                .admin()
                .indices()
                .prepareStats()
                .setIndices("test")
                .setTypes("test")
                .get()
                .getIndex("test")
                .getTotal()
                .getDocs()
                .getCount()
        );

    }

    public static class ElasticWriteStreamFactory extends ExternalResource {
        private Client client;
        private ExecutorService executorService;
        private TransportClientBaseStream transportClientBaseStream;

        public Client getClient() {
            return client;
        }

        public TransportClientBaseStream getTransportClientBaseStream() {
            return transportClientBaseStream;
        }

        @Override
        protected void before() throws Throwable {
            client = client();
            executorService = Executors.newSingleThreadExecutor();
            transportClientBaseStream = new TransportClientBaseStream(
                    client, executorService, 100, 10, TimeValue.timeValueSeconds(5));
            transportClientBaseStream.start();
        }

        @Override
        protected void after() {
            Utils.closeQuietly(transportClientBaseStream);
            try {
                terminate(executorService);
            } catch (Exception e) {
            }
        }
    }

}