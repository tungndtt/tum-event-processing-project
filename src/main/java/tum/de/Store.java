package tum.de;

import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.HashMap;
import java.util.Map;


public class Store {
    private Map<String, RecordMetadata> outEvents;
    private Map<String, Map<String, Consumer.CustomConsumerRecord>> inEventsWrtTopics;
    private static Store store;

    private Store() {
        outEvents = new HashMap<>();
        inEventsWrtTopics = new HashMap<>();
    }

    public static void addOutEvents(String key, RecordMetadata record) {
        if(store == null) store = new Store();
        store.outEvents.put(key, record);
    }

    public static void addInEvents(String topic, String key, Consumer.CustomConsumerRecord record) {
        if(store == null) store = new Store();
        Map<String, Consumer.CustomConsumerRecord> inEvents;
        if(!store.inEventsWrtTopics.containsKey(topic)) {
            inEvents = new HashMap<>();
            store.inEventsWrtTopics.put(topic, inEvents);
        } else {
            inEvents = store.inEventsWrtTopics.get(topic);
        }
        inEvents.put(key, record);
    }

    public static int numOfOutEvents() {
        return store.outEvents.size();
    }

    public static int numOfInEvents(String topic) {
        return store.inEventsWrtTopics.get(topic).size();
    }

    public static double computeSelectivity(String t1, String t2) {
        assert t1 != null && store != null;
        int sizeOfT1 = store.inEventsWrtTopics.get(t1).size();
        int sizeOfT2 = t2 != null? store.inEventsWrtTopics.get(t2).size() : store.outEvents.size();
        return sizeOfT1 * 1.0 / sizeOfT2;
    }

    public static long computeEventLatency(String key) {
        long duration = 0;
        if(store == null) store = new Store();
        else if(store.outEvents.containsKey(key)){
            RecordMetadata metadata = store.outEvents.get(key);
            String topic = metadata.topic();
            if(store.inEventsWrtTopics.containsKey(topic)) {
                Map<String, Consumer.CustomConsumerRecord> topicEvents = store.inEventsWrtTopics.get(topic);
                if(topicEvents.containsKey(key)) {
                    duration = topicEvents.get(key).timestamp - metadata.timestamp();
                }
            }
        }
        return duration;
    }

    public static double computeAvgLatency(String topic) {
        if(store == null) store = new Store();
        else if(store.inEventsWrtTopics.containsKey(topic)) {
            long totDuration = 0;
            Map<String, Consumer.CustomConsumerRecord> topicEvents = store.inEventsWrtTopics.get(topic);
            for(String key: topicEvents.keySet()) {
                totDuration += topicEvents.get(key).timestamp - store.outEvents.get(key).timestamp();
            }
            return totDuration * 1.0 / topicEvents.size();
        }
        return 0.0;
    }
}
