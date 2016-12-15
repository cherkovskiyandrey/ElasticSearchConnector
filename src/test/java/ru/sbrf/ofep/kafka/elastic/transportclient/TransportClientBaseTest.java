package ru.sbrf.ofep.kafka.elastic.transportclient;


import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.AbstractConfig;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;
import ru.sbrf.ofep.kafka.config.ElasticConfig;
import ru.sbrf.ofep.kafka.elastic.ElasticWriteStream;

import java.io.IOException;

import static ru.sbrf.ofep.kafka.ElasticConnector.CONFIG_DEF;
import static ru.sbrf.ofep.kafka.config.ConfigParam.*;

public class TransportClientBaseTest extends ESSingleNodeTestCase {

    private final static String GLOBAL_MAPPING =
            "{ \"test\" : " +
                    "{ \"properties\" : " +
                    "{ " +
                    "\"user\" : " +
                    " {" +
                    " \"type\" : \"string\"," +
                    " \"store\" : \"yes\"" +
                    " }," +

                    "\"postDate\" : " +
                    " {" +
                    " \"type\" : \"date\"," +
                    " \"store\" : \"yes\"" +
                    " }," +

                    "\"number\" : " +
                    " {" +
                    " \"type\" : \"long\"," +
                    " \"store\" : \"yes\"" +
                    " }," +

                    "\"message\" : " +
                    " {" +
                    " \"type\" : \"string\"," +
                    " \"store\" : \"yes\"" +
                    " }" +
                    "}" +
                    "} " +
                    "}";

    private static ElasticConfig configuration() {
        return ElasticConfig.of(new AbstractConfig(CONFIG_DEF,
                ImmutableMap.of(
                        TOPICS.getName(), "test",
                        CLUSTER_NAME.getName(), "test",
                        CLUSTER_NODES.getName(), "localhost:9300",
                        EXPORT_BUFFER_SIZE.getName(), 100,
                        EXPORT_BATCH_SIZE.getName(), 10
                )
        ));
    }

    @Test
    public void createIndexIfNotExistsTest() throws IOException {
        final Client client = client();
        try (final TransportClientBase transportClientBase =
                     new TransportClientBase(client, configuration(), TimeValue.timeValueSeconds(1))) {

            assertTrue(transportClientBase.createIndexIfNotExists("test"));
            assertTrue(client.admin()
                    .indices()
                    .exists(new IndicesExistsRequest("test"))
                    .actionGet()
                    .isExists());
        }
    }

    @Test
    public void putMappingIfNotExistsTest() throws IOException {
        final Client client = client();
        try (final TransportClientBase transportClientBase =
                     new TransportClientBase(client, configuration(), TimeValue.timeValueSeconds(1))) {

            transportClientBase.createIndexIfNotExists("test");

            assertTrue(transportClientBase.putMappingIfNotExists("test", "test", GLOBAL_MAPPING));
            assertNotNull(client.admin()
                    .indices()
                    .getMappings(new GetMappingsRequest())
                    .actionGet()
                    .mappings()
                    .get("test")
                    .get("test"));
        }
    }

    @Test
    public void createNewStreamTest() throws Exception {
        final Client client = client();
        try (final TransportClientBase transportClientBase =
                     new TransportClientBase(client, configuration(), TimeValue.timeValueSeconds(1));
             final ElasticWriteStream elasticWriteStream = transportClientBase.createNewStream()) {
            assertNotNull(elasticWriteStream);
        }
    }

}


















