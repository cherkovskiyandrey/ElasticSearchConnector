package ru.sbrf.ofep.kafka.elastic.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Client {

    //TODO: разобраться с форматом json
    public static void main(String[] args) throws IOException {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://10.21.25.206:9200")
                .multiThreaded(true)
                .readTimeout(10000)
                .build());


        JestClient client = factory.getObject();

        // СОздание индекса
        {
            Settings.Builder settingsBuilder = Settings.settingsBuilder();
            settingsBuilder.put("number_of_shards", 5);
            settingsBuilder.put("number_of_replicas", 2);

            client.execute(new CreateIndex.Builder("articles")
                    .settings(settingsBuilder.build().getAsMap())
                    .build());
        }

        //Создание маппинга
        {
            PutMapping putMapping = new PutMapping.Builder(
                    "articles",
                    "my_type",
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
                     "}"
            ).build();
            client.execute(putMapping);
        }

        //  Индексация документа через готовую JSON строку
//        {
//            String source = jsonBuilder()
//                    .startObject()
//                    .field("user", "Andrey")
//                    .field("postDate", new Date())
//                    .field("message", "чччччччччччччччччччччччччччччччччччччччччччччччччччччччччч")
//                    .endObject().string();
//
//            client.execute(new Index.Builder(source)
//                    .index("articles").type("my_type").id("1").build());
//        }

        // Batch операции
        {
            System.out.println("BEGIN");

            String source = jsonBuilder()
                    .startObject()
                    .field("user", "Andrey")
                    .field("postDate", new Date())
                    .field("message", "ччччччччччччччччччччччччччччччччччччччччччччччччччччччччччxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
                    .endObject().string();

            long begin = System.nanoTime();
            for(int k = 0; k < 20; ++k) {
                Bulk.Builder builder = new Bulk.Builder();
                System.out.println("Begin prepare batch");
                for (int i = 0; i < 50000; ++i) {
                    builder.addAction(new Index.Builder(source)
                            .index("articles").type("my_type").id(Integer.toString(50000*k + i)).build());
                }
                System.out.println("End prepare batch");
                System.out.println("Begin build batch");
                Bulk bulk = builder.build();
                System.out.println("End build batch");
                System.out.println("Begin execute batch");
                BulkResult result = client.execute(bulk);
                System.out.println("End execute batch");

                for (BulkResult.BulkResultItem failed : result.getFailedItems()) {
                    System.out.println("FAILED INDEXED: id[" + failed.id + "], error[" + failed.error + "]");
                }
            }
            System.out.println("All time: " + (System.nanoTime() - begin)/1000000l + " ms.");
        }

    }
}









