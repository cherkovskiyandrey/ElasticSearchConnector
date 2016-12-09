package ru.sbrf.ofep.kafka.elastic;

public interface ElasticsearchClient extends AutoCloseable {

    /**
     * Create and return new output stream.
     *
     * @return
     */
    ElasticWriteStream createNewStream();

    /**
     * Create index if not exists already.
     *
     * @param index
     * @throws RuntimeException if any problems with elasticsearch
     * @return false if index already exists
     */
    boolean createIndexIfNotExists(String index);

    /**
     * Put mapping into elastic if not exists.
     * ATTENTION: if mapping already exists method does not compare them.
     *
     * @param index
     * @param type
     * @param mapping
     * @return false if mapping already exists
     */
    boolean putMappingIfNotExists(String index, String type, String mapping);
}
