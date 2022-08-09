package tum.de;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;


public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws InterruptedException {
        // start running the scripts for Producer & Consumer
        Collection<Callable<Void>> tasks = Arrays.asList(
            startProducer(),
            startConsumer()
        );
        ExecutorService executor = Executors.newFixedThreadPool(tasks.size());
        executor.invokeAll(tasks, 20, TimeUnit.MINUTES); // Timeout of 15 minutes.
        executor.shutdown();
        // make the report
        report();
    }

    private static Callable<Void> startProducer() {
        return () -> {
            try(InputStream fileStream = new FileInputStream("events.json.gz");
                InputStream gzipStream = new GZIPInputStream(fileStream);
                LineIterator iter = IOUtils.lineIterator(gzipStream, "UTF-8");
            ) {
                Producer producer = new Producer();
                while (iter.hasNext()) {
                    String line = iter.nextLine();
                    // publish the message every 10 ms
                    producer.publish("meetup-events", line);
                    Thread.sleep(10);
                }
            } catch(InterruptedException e) {
                logger.info("Thread is interrupted");
            }
            return null;
        };
    }

    private static Callable<Void> startConsumer() {
        return () -> {
            Consumer consumer = new Consumer("meetup-events-consumer");
            consumer.subscribe(Arrays.asList(
                "meetup-events",
                "GERMANY_MEETUP_EVENTS_STREAM",
                "GERMANY_MUNICH_MEETUP_EVENTS_STREAM",
                "MUNICH_MEETUP_EVENTS_STREAM"
            ));
            return null;
        };
    }

    private static void report() {
        int outEvents = Store.numOfOutEvents();
        int inEvents = Store.numOfInEvents("meetup-events");
        int germanyEvents = Store.numOfInEvents("GERMANY_MEETUP_EVENTS_STREAM");
        int germany_munichEvents = Store.numOfInEvents("GERMANY_MUNICH_MEETUP_EVENTS_STREAM");
        int munichEvents = Store.numOfInEvents("MUNICH_MEETUP_EVENTS_STREAM");

        double germany_filter_selectivity = Store.computeSelectivity("GERMANY_MEETUP_EVENTS_STREAM", null);
        double germany_munich_filter_selectivity = Store.computeSelectivity(
            "GERMANY_MUNICH_MEETUP_EVENTS_STREAM",
            "GERMANY_MEETUP_EVENTS_STREAM"
        );
        double munich_filter_selectivity = Store.computeSelectivity("MUNICH_MEETUP_EVENTS_STREAM", null);

        double germany_munich_events_avg_duration = Store.computeAvgLatency(
        "GERMANY_MUNICH_MEETUP_EVENTS_STREAM"
        );
        double munich_events_avg_duration = Store.computeAvgLatency("MUNICH_MEETUP_EVENTS_STREAM");

        // report logging
        logger.info("Statistics: \n" +
            "# of producing events: " + outEvents + ", \n" +
            "# of consuming events: " + inEvents + ", \n" +
            "# of germany meetup events: " + germanyEvents + ", \n" +
            "# of germany-munich meetup events: " + germany_munichEvents + ", \n" +
            "# of munich meetup events: " + munichEvents
        );
        logger.info("Selectivity: \n" +
            "Germany filter: " + germany_filter_selectivity + ", \n" +
            "Germany-Munich filter: " + germany_munich_filter_selectivity + ", \n" +
            "Munich filter: " + munich_filter_selectivity
        );
        logger.info("Average Latency: \n" +
            "Germany-Munich filter: " + germany_munich_events_avg_duration + " ms, \n" +
            "Munich filter: " + munich_events_avg_duration + " ms"
        );
    }
}
