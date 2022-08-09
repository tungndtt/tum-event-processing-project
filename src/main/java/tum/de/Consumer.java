package tum.de;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.List;
import java.util.Properties;


public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private KafkaConsumer<String, String> consumer;

    public Consumer(String groupId) {
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092,127.0.0.1:9093");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("schema.registry.url", "127.0.0.1:8081");
        // create consumer
        consumer = new KafkaConsumer<>(properties);
        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();
        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Close the consumer safely by calling consumer.wakeup()...");
            consumer.wakeup();
            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error("Error occurs while waiting for all threads to be shutdown");
            }
        }));
    }

    public void subscribe(List<String> topics) {
        try {
            // subscribe consumer to our topic(s)
            consumer.subscribe(topics);
            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record: records) {
                    long currentTimestamp = System.currentTimeMillis();
                    /*
                    logger.info("Receive event. \n" +
                        "Topic: " + record.topic() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Key: " + record.key() + "\n" +
                        "Value: " + record.value() + "\n" +
                        "Timestamp: " + record.timestamp() + "\n" +
                        "Current timestamp: " + currentTimestamp
                    );
                     */
                    Store.addInEvents(
                        record.topic(), record.key(),
                        new CustomConsumerRecord(
                            record.topic(), record.partition(),
                            record.key(), record.value(), currentTimestamp
                        )
                    );
                }
            }
        } catch (WakeupException e) {
            logger.info("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            logger.info("Consumer thread is interrupted");
        } finally {
            consumer.close(); // this will also commit the offsets if need be.
            logger.info("The consumer is now gracefully closed.");
        }
    }

    public static class CustomConsumerRecord {
        public String topic;
        public int partition;
        public String key;
        public String value;
        public long timestamp;

        CustomConsumerRecord(String topic, int partition, String key, String value, long timestamp) {
            this.topic = topic;
            this.partition = partition;
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }
    }
}
