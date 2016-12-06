package ru.sbrf.ofep.kafka.elastic.transportclient;

import org.elasticsearch.action.bulk.BulkItemResponse;
import ru.sbrf.ofep.kafka.elastic.domain.Document;
import ru.sbrf.ofep.kafka.elastic.domain.FailedDocument;
import ru.sbrf.ofep.kafka.elastic.domain.Key;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

class Helper {

    static FailedDocument toFailedDocument(Document orig, BulkItemResponse next) {
        return new FailedDocument(orig,
                next.getFailure().getCause(),
                next.getFailure().getMessage());
    }

    static FailedDocument toFailedDocument(Document orig, Exception e) {
        return new FailedDocument(orig, e, e.getMessage());
    }

    static Map<Key, Document> asMap(Queue<Document> curBatch) {
        final Map<Key, Document> result = new HashMap<>();
        for (Document doc : curBatch) {
            result.put(doc.getKey(), doc);
        }
        return result;
    }

    static Key extractKey(BulkItemResponse next) {
        return new Key(next.getIndex(), next.getType(), next.getId());
    }

    static String extractIp(String ipPort) {
        return ipPort.split(":")[0];
    }

    static int extractPort(String ipPort) {
        return Integer.valueOf(ipPort.split(":")[1]);
    }
}
