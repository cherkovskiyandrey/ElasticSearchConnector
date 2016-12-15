package ru.sbrf.ofep.kafka.elastic.domain;


import org.apache.kafka.common.TopicPartition;

import java.nio.charset.StandardCharsets;

public class Document {
    private final Key key;
    private final MetaInfo metaInfo;
    private final byte[] jsonContent;

    public Document(Key key, MetaInfo metaInfo, byte[] jsonContent) {
        this.key = key;
        this.metaInfo = metaInfo;
        this.jsonContent = jsonContent;
    }

    public Key getKey() {
        return key;
    }

    public MetaInfo getMetaInfo() {
        return metaInfo;
    }

    public byte[] getJsonContent() {
        return jsonContent;
    }

    @Override
    public String toString() {
        return "Document{" +
                "key=" + key +
                ", metaInfo=" + metaInfo +
                ", jsonContent='" + new String(jsonContent, StandardCharsets.UTF_8) + '\'' +
                '}';
    }

    public static class MetaInfo {
        private final TopicPartition topicPartition;
        private final long offset;


        public MetaInfo(TopicPartition topicPartition, long offset) {
            this.topicPartition = topicPartition;
            this.offset = offset;
        }

        public TopicPartition getTopicPartition() {
            return topicPartition;
        }

        public long getOffset() {
            return offset;
        }

        @Override
        public String toString() {
            return "MetaInfo{" +
                    "topicPartition=" + topicPartition +
                    ", offset=" + offset +
                    '}';
        }
    }
}
