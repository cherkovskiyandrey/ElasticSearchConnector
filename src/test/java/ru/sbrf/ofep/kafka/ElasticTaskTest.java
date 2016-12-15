package ru.sbrf.ofep.kafka;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import ru.sbrf.ofep.kafka.config.ElasticConfig;
import ru.sbrf.ofep.kafka.elastic.convertors.DocumentConverter;
import ru.sbrf.ofep.kafka.elastic.ElasticWriteStream;
import ru.sbrf.ofep.kafka.elastic.ElasticsearchClient;
import ru.sbrf.ofep.kafka.elastic.convertors.GenerateDocumentIdMode;
import ru.sbrf.ofep.kafka.elastic.domain.FailedDocument;
import ru.sbrf.ofep.kafka.elastic.exceptions.ElasticIOException;

import java.util.*;

import static org.apache.kafka.connect.sink.SinkTask.TOPICS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static ru.sbrf.ofep.kafka.DataGenerateHelper.generateFailedDocs;
import static ru.sbrf.ofep.kafka.DataGenerateHelper.generateSinkByPort;
import static ru.sbrf.ofep.kafka.ElasticConnector.CONFIG_DEF;
import static ru.sbrf.ofep.kafka.config.ConfigParam.*;

public class ElasticTaskTest {

    //TODO: если для стратегии обработки ошибок: FAIL_FAST упасть в flush, то ошибка никуда не будет трансдирована и будет вызван метод put

    //TODO: проверка инициализации

    //TODO: проверка пустой коллекции

    private Map<String, String> settings() {
        final Map<String, String> settings = new HashMap<>();
        settings.put(TOPICS_CONFIG, "topic1");
        settings.put(CLUSTER_NAME.getName(), "test");
        settings.put(CLUSTER_NODES.getName(), "localhost:9300");
        return settings;
    }

    @Test
    public void simplePutTest() throws Exception {
        try (RecourseBundle recourseBundle = new RecourseBundle(settings(), ErrorPolicy.FAIL_FAST)) {
            recourseBundle.getElasticTask().put(generateSinkByPort(1, 1));

            assertEquals(1, recourseBundle.getElasticWriteStreams().size());
            final ArgumentCaptor<Collection> collectionArgumentCaptor = ArgumentCaptor.forClass(Collection.class);
            verify(recourseBundle.getElasticWriteStreams().getFirst()).write(collectionArgumentCaptor.capture());
            assertEquals(1, collectionArgumentCaptor.getValue().size());

            verify(recourseBundle.getSinkTaskContext(), times(0)).offset(anyMap());
        }
    }

    @Test
    public void putWithTwoPartitionsTest() throws Exception {
        try (RecourseBundle recourseBundle = new RecourseBundle(settings(), ErrorPolicy.FAIL_FAST)) {
            recourseBundle.getElasticTask().put(generateSinkByPort(2, 1));

            assertEquals(2, recourseBundle.getElasticWriteStreams().size());
            final ArgumentCaptor<Collection> collectionArgumentCaptor = ArgumentCaptor.forClass(Collection.class);
            verify(recourseBundle.getElasticWriteStreams().getFirst()).write(collectionArgumentCaptor.capture());
            assertEquals(1, collectionArgumentCaptor.getValue().size());
            final ArgumentCaptor<Collection> collectionArgumentCaptor2 = ArgumentCaptor.forClass(Collection.class);
            verify(recourseBundle.getElasticWriteStreams().getLast()).write(collectionArgumentCaptor2.capture());
            assertEquals(1, collectionArgumentCaptor2.getValue().size());

            verify(recourseBundle.getSinkTaskContext(), times(0)).offset(anyMap());
        }
    }


    @Test
    @SuppressWarnings("unchecked")
    public void putWithConnectionLostErrorTest() throws Exception {
        try (RecourseBundle recourseBundle = new RecourseBundle(settings(), ErrorPolicy.FAIL_FAST)) {
            recourseBundle.getElasticTask().put(generateSinkByPort(1, 1));

            assertEquals(1, recourseBundle.getElasticWriteStreams().size());

            when(recourseBundle.getElasticWriteStreams().getLast().getAndClearFailedDocuments())
                    .thenReturn(generateFailedDocs(1, new MasterNotDiscoveredException(), "MasterNotDiscoveredException"));

            recourseBundle.getElasticTask().put(generateSinkByPort(1, 1));

            final ArgumentCaptor<Map> mapArgumentCaptor = ArgumentCaptor.forClass(Map.class);
            verify(recourseBundle.getSinkTaskContext()).offset(mapArgumentCaptor.capture());
            final Map.Entry<TopicPartition, Long> expectedOffset = (Map.Entry<TopicPartition, Long>)
                    mapArgumentCaptor.getValue().entrySet().iterator().next();

            assertEquals(0, expectedOffset.getKey().partition());
            assertEquals(0, expectedOffset.getValue().longValue());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void putWithNoConnectionLostErrorAndRetryPolicyTest() throws Exception {
        try (RecourseBundle recourseBundle = new RecourseBundle(settings(), ErrorPolicy.RETRY_FOREVER)) {
            recourseBundle.getElasticTask().put(generateSinkByPort(1, 1));

            assertEquals(1, recourseBundle.getElasticWriteStreams().size());

            when(recourseBundle.getElasticWriteStreams().getLast().getAndClearFailedDocuments())
                    .thenReturn(generateFailedDocs(1, new RuntimeException(), "RuntimeException"));

            recourseBundle.getElasticTask().put(generateSinkByPort(1, 1));

            final ArgumentCaptor<Map> mapArgumentCaptor = ArgumentCaptor.forClass(Map.class);
            verify(recourseBundle.getSinkTaskContext()).offset(mapArgumentCaptor.capture());
            final Map.Entry<TopicPartition, Long> expectedOffset = (Map.Entry<TopicPartition, Long>)
                    mapArgumentCaptor.getValue().entrySet().iterator().next();

            assertEquals(0, expectedOffset.getKey().partition());
            assertEquals(0, expectedOffset.getValue().longValue());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void putWithNoConnectionLostErrorAndRetryPolicyWithTwoPartitionsTest() throws Exception {
        try (RecourseBundle recourseBundle = new RecourseBundle(settings(), ErrorPolicy.RETRY_FOREVER)) {
            recourseBundle.getElasticTask().put(generateSinkByPort(2, 1));

            assertEquals(2, recourseBundle.getElasticWriteStreams().size());

            when(recourseBundle.getElasticWriteStreams().getFirst().getAndClearFailedDocuments())
                    .thenReturn(generateFailedDocs(1, new RuntimeException(), "RuntimeException"));

            recourseBundle.getElasticTask().put(generateSinkByPort(2, 1));

            final ArgumentCaptor<Map> mapArgumentCaptor = ArgumentCaptor.forClass(Map.class);
            verify(recourseBundle.getSinkTaskContext()).offset(mapArgumentCaptor.capture());
            assertEquals(1, mapArgumentCaptor.getValue().size());
            final Map.Entry<TopicPartition, Long> expectedOffset = (Map.Entry<TopicPartition, Long>)
                    mapArgumentCaptor.getValue().entrySet().iterator().next();

            assertEquals(0, expectedOffset.getValue().longValue());
        }
    }

    @Test(expected = ConnectException.class)
    public void putWithNoConnectionLostErrorAndFailPolicyTest() throws Exception {
        try (RecourseBundle recourseBundle = new RecourseBundle(settings(), ErrorPolicy.FAIL_FAST)) {
            recourseBundle.getElasticTask().put(generateSinkByPort(1, 1));

            assertEquals(1, recourseBundle.getElasticWriteStreams().size());

            when(recourseBundle.getElasticWriteStreams().getLast().getAndClearFailedDocuments())
                    .thenReturn(generateFailedDocs(1, new RuntimeException(), "RuntimeException"));

            recourseBundle.getElasticTask().put(generateSinkByPort(1, 1));
        }
    }

    @Test
    public void putWithNoConnectionLostErrorAndLogPolicyTest() throws Exception {
        try (RecourseBundle recourseBundle = new RecourseBundle(settings(), ErrorPolicy.JUST_LOG)) {
            recourseBundle.getElasticTask().put(generateSinkByPort(1, 1));

            assertEquals(1, recourseBundle.getElasticWriteStreams().size());

            when(recourseBundle.getElasticWriteStreams().getLast().getAndClearFailedDocuments())
                    .thenReturn(generateFailedDocs(1, new RuntimeException(), "RuntimeException"));

            recourseBundle.getElasticTask().put(generateSinkByPort(1, 1));

            verify(recourseBundle.getSinkTaskContext(), times(0)).offset(anyMap());
        }
    }

    @Test
    public void simpleFlushWithoutDataTest() throws Exception {
        try (RecourseBundle recourseBundle = new RecourseBundle(settings(), ErrorPolicy.JUST_LOG)) {
            recourseBundle.getElasticTask().flush(Collections.<TopicPartition, OffsetAndMetadata>emptyMap());
            assertTrue(recourseBundle.getElasticWriteStreams().isEmpty());
        }
    }

    @Test
    public void simpleFlushTest() throws Exception {
        try (RecourseBundle recourseBundle = new RecourseBundle(settings(), ErrorPolicy.JUST_LOG)) {
            recourseBundle.getElasticTask().put(generateSinkByPort(1, 1));
            assertEquals(1, recourseBundle.getElasticWriteStreams().size());

            final Map<TopicPartition, OffsetAndMetadata> offsets = Collections.emptyMap();
            recourseBundle.getElasticTask().flush(offsets);
            assertTrue(offsets.isEmpty());

            verify(recourseBundle.getElasticWriteStreams().getFirst()).flush();
        }
    }

    @Test
    public void flushAndConnectionLostErrorTest() throws Exception {
        try (RecourseBundle recourseBundle = new RecourseBundle(settings(), ErrorPolicy.JUST_LOG)) {
            recourseBundle.getElasticTask().put(generateSinkByPort(1, 1));
            assertEquals(1, recourseBundle.getElasticWriteStreams().size());


            when(recourseBundle.getElasticWriteStreams().getFirst().getAndClearFailedDocuments())
                    .thenReturn(new ArrayList<FailedDocument>())
                    .thenReturn(generateFailedDocs(1, new MasterNotDiscoveredException(), "MasterNotDiscoveredException"))
            ;

            doThrow(new ElasticIOException(0L, "")).when(recourseBundle.getElasticWriteStreams().getFirst()).flush();

            final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition("test", 0), new OffsetAndMetadata(10));

            boolean hasThrowed = false;
            try {
                recourseBundle.getElasticTask().flush(offsets);
            } catch (ConnectException ex) {
                hasThrowed = true;
            }
            assertTrue(hasThrowed);

            final ArgumentCaptor<Map> mapArgumentCaptor = ArgumentCaptor.forClass(Map.class);
            verify(recourseBundle.getSinkTaskContext()).offset(mapArgumentCaptor.capture());
            assertEquals(1, mapArgumentCaptor.getValue().size());
            final Map.Entry<TopicPartition, Long> expectedOffset = (Map.Entry<TopicPartition, Long>)
                    mapArgumentCaptor.getValue().entrySet().iterator().next();

            assertEquals(0, expectedOffset.getKey().partition());
            assertEquals(0, expectedOffset.getValue().longValue());

            verify(recourseBundle.getElasticWriteStreams().getFirst()).flush();
        }
    }

    private static ElasticConfig configuration() {
        return ElasticConfig.of(new AbstractConfig(CONFIG_DEF,
                ImmutableMap.of(
                        TOPICS.getName(), "test",
                        CLUSTER_NAME.getName(), "test",
                        CLUSTER_NODES.getName(), "localhost:9300",
                        ID_MODE.getName(), GenerateDocumentIdMode.KAFKA_KEY.getValueName()
                )
        ));
    }

    private class RecourseBundle implements AutoCloseable {
        private final SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
        private final ElasticsearchClient elasticsearchClient = mock(ElasticsearchClient.class);
        private final LinkedList<ElasticWriteStream> elasticWriteStreams = new LinkedList<>();
        private final DocumentConverter documentConverter = new DocumentConverter(configuration());
        private final ElasticTask elasticTask;

        RecourseBundle(Map<String, String> settings, ErrorPolicy errorPolicy) {
            when(elasticsearchClient.createIndexIfNotExists(anyString())).thenReturn(true);
            when(elasticsearchClient.putMappingIfNotExists(anyString(), anyString(), anyString())).thenReturn(true);
            when(elasticsearchClient.createNewStream()).then(new Answer<ElasticWriteStream>() {
                @Override
                public ElasticWriteStream answer(InvocationOnMock invocation) throws Throwable {
                    elasticWriteStreams.add(mock(ElasticWriteStream.class));
                    return elasticWriteStreams.getLast();
                }
            });

            elasticTask = new ElasticTask();
            elasticTask.initialize(sinkTaskContext);

            elasticTask.start(settings, elasticsearchClient, documentConverter, errorPolicy);
        }

        public SinkTaskContext getSinkTaskContext() {
            return sinkTaskContext;
        }

        public LinkedList<ElasticWriteStream> getElasticWriteStreams() {
            return elasticWriteStreams;
        }

        public ElasticTask getElasticTask() {
            return elasticTask;
        }

        @Override
        public void close() throws Exception {
            elasticTask.stop();
        }
    }
}




