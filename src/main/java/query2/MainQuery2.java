package query2;

import model.Post;
import model.PostDeserializer;
import model.PostSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import utils.Config;
import utils.MyEventTimeExtractor;
import utils.TimeSlot;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.ofMinutes;

public class MainQuery2 {

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(),new PostDeserializer())));


        KTable<Windowed<Integer>, Long> count1H = source
                .filter(new Predicate<Long, Post>() {
                    @Override
                    public boolean test(Long aLong, Post post) {
                        return post.getDepth() == 1;
                    }
                })
                .map(new KeyValueMapper<Long, Post, KeyValue<Integer, Integer>>() {
                    @Override
                    public KeyValue<Integer, Integer> apply(Long aLong, Post post) {
                        return new KeyValue<>(TimeSlot.getTimeSlot(post), 1);
                    }
                })
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(ofMinutes(1)))
                .count();

        count1H.toStream().foreach(new ForeachAction<Windowed<Integer>, Long>() {
            @Override
            public void apply(Windowed<Integer> integerWindowed, Long aLong) {
                System.out.println(integerWindowed+"\t"+aLong);
            }
        });
        /*KTable<Windowed<Integer>, Long> count24H = source
                .filter(new Predicate<Long, Post>() {
                    @Override
                    public boolean test(Long aLong, Post post) {
                        return post.getDepth() == 1;
                    }
                })
                .map(new KeyValueMapper<Long, Post, KeyValue<Integer, Integer>>() {
                    @Override
                    public KeyValue<Integer, Integer> apply(Long aLong, Post post) {
                        return new KeyValue<>(TimeSlot.getTimeSlot(post), 1);
                    }
                })
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(24)).grace(ofMinutes(1)))
                .count();*/



        KafkaStreams streams = new KafkaStreams(builder.build(), props);
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
        System.exit(0);
    }


}
