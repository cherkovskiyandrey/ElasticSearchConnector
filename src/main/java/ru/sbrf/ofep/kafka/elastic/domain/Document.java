package ru.sbrf.ofep.kafka.elastic.domain;


import org.apache.kafka.common.TopicPartition;

public class Document {
    private final Key key;
    private final MetaInfo metaInfo;
    private final String jsonContent;

    public Document(Key key, MetaInfo metaInfo, String jsonContent) {
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

    public String getJsonContent() {
        return jsonContent;
    }

    @Override
    public String toString() {
        return "Document{" +
                "key=" + key +
                ", metaInfo=" + metaInfo +
                ", jsonContent='" + jsonContent + '\'' +
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
