package utils;

import org.apache.kafka.streams.KafkaStreams;

import java.util.concurrent.CountDownLatch;

public class AttachCtrlC {

    public static void attach(KafkaStreams streams){
        CountDownLatch latch = new CountDownLatch(1);

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
