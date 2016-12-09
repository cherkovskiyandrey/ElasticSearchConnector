package ru.sbrf.ofep.kafka.elastic.transportclient;


import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TestClient {


    public static void main(String[] args) throws IOException, InterruptedException {

//        {
//
//            Converter JSON_CONVERTER = new JsonConverter();
//            JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
//
//            byte[] result = JSON_CONVERTER.fromConnectData("", null, "dddddddd");
//
//            System.out.println(new String(result, StandardCharsets.UTF_8));
//
//        }



        Settings.Builder settings = Settings.builder()
                .put("cluster.name", "ofep-dev2");

        Client client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.21.25.206"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.21.25.207"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.21.25.208"), 9300));



////        //Создание индекса
        {
            Settings.Builder settingsBuilder = Settings.settingsBuilder();
            settingsBuilder.put("number_of_shards", 5);
            settingsBuilder.put("number_of_replicas", 2);


            client.admin().indices().prepareCreate("articles")
                    .setSettings(settingsBuilder)
                    .get();
        }
//
//        //Создание маппинга
        String globalMapping =
                "{ \"my_type\" : " +
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

        String globalMapping2 =
                "{ \"my_type\" : " +
                        "{ \"properties\" : " +
                        "{ " +

                        "\"id\" : " +
                        " {" +
                        " \"type\" : \"long\"," +
                        " \"store\" : \"yes\"" +
                        " }," +

                        "\"level\" : " +
                        " {" +
                        " \"type\" : \"string\"," +
                        " \"store\" : \"yes\"" +
                        " }," +

                        "\"postDate\" : " +
                        " {" +
                        " \"type\" : \"date\"," +
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


        {
            client.admin().indices().preparePutMapping("articles")
                    .setType("my_type")
                    .setSource(globalMapping)
                    .get();
        }

        //  Индексация документа через готовую JSON строку
//        {
//            String source = jsonBuilder()
//                    .startObject()
//                    .field("user", "Andrey")
//                    //.field("postDate", new Date())
//                    .field("postDate", "-------------------")
//                    .field("message", "чччччччччччччччччччччччччччччччччччччччччччччччччччччччччч")
//                    .endObject().string();
//
//            IndexResponse response = client.prepareIndex("articles", "my_type", "1")
//                    .setSource(source)
//                    .get();
//
//
//            System.out.println(response);
//        }

        //Проверка что индекс уже существует
//        {
//            boolean b = client.admin().indices()
//                    .exists(new IndicesExistsRequest("dddddd")).actionGet().isExists();
//
//            System.out.println(b);
//
//            //"articles", "my_type"
//            boolean bb = client.admin().indices().typesExists(new TypesExistsRequest(new String[]{"articles"}, "my_type")).actionGet().isExists();
//
//            System.out.println(bb);
//
//            //client.admin().indices().getMappings(new GetMappingsRequest()).actionGet().mappings();
//            MappingMetaData metaData = client.admin().indices().getMappings(new GetMappingsRequest()).actionGet().mappings().get("articles").get("my_type");
//
//            String mapping = "{\"my_type\":{\"properties\":{\"message\":{\"type\":\"string\"},\"postDate\":{\"type\":\"date\",\"format\":\"strict_date_optional_time||epoch_millis\"},\"user\":{\"type\":\"string\"}}}}";
//            MappingMetaData mmm = new MappingMetaData(new CompressedXContent(mapping));
//
//            boolean bbb = metaData.equals(mmm);
//
//            System.out.println(bbb);
//        }

        // Batch операции
//        while(true){
//            System.out.println("BEGIN");
//
//            String source = jsonBuilder()
//                    .startObject()
//                    .field("user", "Andrey")
//                    .field("postDate", new Date())
//                    .field("message", "ччччччччччччччччччччччччччччччччччччччччччччччччччччччччччxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
//                    .endObject().string();
//
//
//            BulkProcessor bulkProcessor = BulkProcessor.builder(
//                    client,
//                    new BulkProcessor.Listener() {
//                        @Override
//                        public void beforeBulk(long executionId,
//                                               BulkRequest request) {
//                        }
//
//                        @Override
//                        public void afterBulk(long executionId,
//                                              BulkRequest request,
//                                              BulkResponse response) {
//                            System.out.println("BULK succ: " + response.getItems().length);
//                            if (response.hasFailures()) {
//                                Iterator<BulkItemResponse> itr = response.iterator();
//                                while (itr.hasNext()) {
//                                    BulkItemResponse next = itr.next();
//                                    if (next.isFailed()) {
//                                        System.out.println("Failed item: " + next.getFailure().getMessage());
//                                    }
//                                }
//                            }
//                        }
//
//                        @Override
//                        public void afterBulk(long executionId,
//                                              BulkRequest request,
//                                              Throwable failure) {
//                            System.out.println("BULK failed: " + failure);
//                        }
//                    })
//                    .setBulkActions(10000)
//                    .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
//                    .setFlushInterval(TimeValue.timeValueSeconds(1))
//                    .setConcurrentRequests(1)
//                    .setBackoffPolicy(
//                            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 5)
//                    )
//                    .build();
//
//            System.out.println("Begin add bulks.");
//            long begin = System.nanoTime();
//            for (int i = 0; i < 1000000; ++i) {
//                bulkProcessor.add(new IndexRequest("articles", "my_type", Integer.toString(i)).source(source));
//                System.out.println("==>> " + i);
//            }
//
//            bulkProcessor.close();
//
//            bulkProcessor.awaitClose(1, TimeUnit.DAYS);
//            System.out.println("All time: " + (System.nanoTime() - begin) / 1000000l + " ms.");
//            TimeUnit.SECONDS.sleep(5);
//        }

        // Batch операции низкоуровневое api
        {
            while(true) {
                System.out.println("BEGIN");
                String source = jsonBuilder()
                        .startObject()
                        .field("user", "Andrey")
                        //.field("postDate", new Date())
                        .field("postDate", "------------------")
                        .field("message", "ччччччччччччччччччччччччччччччччччччччччччччччччччччччччччxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
                        .endObject().string();


                BulkRequestBuilder bulkRequest = client.prepareBulk();

                System.out.println("Begin add bulks.");
                long begin = System.nanoTime();
                for (int i = 0; i < 100; ++i) {
                    System.out.println("==>> " + i);
                    //bulkRequest.add(client.prepareIndex("articles", "my_type", Integer.toString(i)).setSource(source));
                    bulkRequest.add(client.prepareIndex("articles", "my_type").setSource(source));
                }
                System.out.println("End add bulks.");
                bulkRequest.setTimeout(TimeValue.timeValueSeconds(300)); // не всегда работает
                try {
                    BulkResponse bulkResponse = bulkRequest.get(TimeValue.timeValueSeconds(300)); // try it
                    if (bulkResponse.hasFailures()) {
                        Iterator<BulkItemResponse> itr = bulkResponse.iterator();
                        while (itr.hasNext()) {
                            BulkItemResponse next = itr.next();
                            if (next.isFailed()) {
                                System.out.println("Failed item: " + next.getFailure().getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println(e);
                    TimeUnit.SECONDS.sleep(3);
                }
                System.out.println("All time: " + (System.nanoTime() - begin) / 1000000l + " ms.");
            }
            //200
        }


        //client.close();
    }
}
