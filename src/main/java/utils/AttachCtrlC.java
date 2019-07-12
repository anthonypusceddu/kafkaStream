package utils;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class AttachCtrlC {

    public static void attach(KafkaStreams streams){
        CountDownLatch latch = new CountDownLatch(1);
        //Map<MetricName, ? extends Metric> metrics = streams.metrics();
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }

    }
}
