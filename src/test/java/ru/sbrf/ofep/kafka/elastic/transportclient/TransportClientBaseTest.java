package ru.sbrf.ofep.kafka.elastic.transportclient;


import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;
import ru.sbrf.ofep.kafka.elastic.ElasticWriteStream;

import java.io.IOException;

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

    @Test
    public void createIndexIfNotExistsTest() throws IOException {
        final Client client = client();
        try (final TransportClientBase transportClientBase =
                     new TransportClientBase(client, 100, 10, TimeValue.timeValueSeconds(1))) {

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
                     new TransportClientBase(client, 100, 10, TimeValue.timeValueSeconds(1))) {

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
                     new TransportClientBase(client, 100, 10, TimeValue.timeValueSeconds(1));
             final ElasticWriteStream elasticWriteStream = transportClientBase.createNewStream()) {
            assertNotNull(elasticWriteStream);
        }
    }

}


















