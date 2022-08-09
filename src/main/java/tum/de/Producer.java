package tum.de;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.UUID;


public class Producer {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private KafkaProducer<String, String> producer;

    public Producer() {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092,127.0.0.1:9093");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create the producer
        producer = new KafkaProducer<>(properties);

        final Thread mainThread = Thread.currentThread();
        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Close the producer by calling producer.close()...");
            producer.close();
            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error("Error occurs while waiting for all threads to be shutdown");
            }
        }));
    }

    public void publish(String topic, String message) {
        // create a producer record
        String key = UUID.randomUUID().toString();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message);
        // send data - asynchronous
        producer.send(producerRecord, (RecordMetadata recordMetadata, Exception e) -> {
            // executes every time a record is successfully sent or an exception is thrown
            if (e == null) { // the record was successfully sent
                /*
                logger.info("Sent event. \n" +
                    "Topic:" + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Key: " + key + "\n" +
                    "Timestamp: " + recordMetadata.timestamp()
                );
                 */
                Store.addOutEvents(key, recordMetadata);
            } else {
                logger.error("Error while publishing the event: ", e);
            }
        });
    }
}
