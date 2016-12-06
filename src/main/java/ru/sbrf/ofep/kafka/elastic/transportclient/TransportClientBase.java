package ru.sbrf.ofep.kafka.elastic.transportclient;

import org.apache.kafka.common.config.AbstractConfig;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import ru.sbrf.ofep.kafka.config.ConfigParam;
import ru.sbrf.ofep.kafka.elastic.ElasticWriteStream;
import ru.sbrf.ofep.kafka.elastic.ElasticsearchClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ru.sbrf.ofep.kafka.elastic.transportclient.Helper.extractIp;
import static ru.sbrf.ofep.kafka.elastic.transportclient.Helper.extractPort;

public class TransportClientBase implements ElasticsearchClient {
    private final Client client;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final int queueSize;
    private final int batchSize;
    private final TimeValue batchTimeout;

    TransportClientBase(Client client, int queueSize, int batchSize, TimeValue batchTimeout) {
        this.client = client;
        this.queueSize = queueSize;
        this.batchSize = batchSize;
        this.batchTimeout = batchTimeout;
    }

    public static ElasticsearchClient newInstance(AbstractConfig conf) throws Exception {
        return new TransportClientBase(
                createClient(conf),
                conf.getInt(ConfigParam.EXPORT_BUFFER_SIZE.getName()),
                conf.getInt(ConfigParam.EXPORT_BATCH_SIZE.getName()),
                TimeValue.timeValueSeconds(conf.getInt(ConfigParam.EXPORT_BATCH_TIMEOUT.getName()))
        );
    }

    private static TransportClient createClient(AbstractConfig conf) throws UnknownHostException {
        final Settings.Builder settings = Settings.builder().put("cluster.name", conf.getString(ConfigParam.CLUSTER_NAME.getName()));
        final TransportClient client = TransportClient.builder().settings(settings).build();
        for (String ipPort : conf.getList(ConfigParam.CLUSTER_NODES.getName())) {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(extractIp(ipPort)), extractPort(ipPort)));
        }
        return client;
    }

    @Override
    public ElasticWriteStream createNewStream() {
        final TransportClientBaseStream stream = new TransportClientBaseStream(client, executorService, queueSize, batchSize, batchTimeout);
        stream.start();
        return stream;
    }

    @Override
    public boolean createIndexIfNotExists(String index) {
        boolean isExists = client.admin().indices()
                .exists(new IndicesExistsRequest(index)).actionGet().isExists();
        if (isExists) {
            return false;
        }
        client.admin().indices().prepareCreate(index).get();

        return true;
    }

    @Override
    public boolean putMappingIfNotExists(String index, String type, String mapping) {
        final ImmutableOpenMap<String, MappingMetaData> mappingForIndex = client.admin().indices()
                .getMappings(new GetMappingsRequest()).actionGet().mappings().get(index);

        if(mappingForIndex != null) {
            final MappingMetaData mappingForType = mappingForIndex.get(type);
            if(mappingForType != null) {
                return true;
            }
        }
        final PutMappingRequestBuilder builder = client.admin().indices().preparePutMapping(index);
        if(type != null) {
            builder.setType(type);
        }
        builder.setSource(mapping).get();

        return true;
    }


    @Override
    public void close() throws IOException {
        executorService.shutdownNow();
        try {
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
