package query1;

import model.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import serDes.*;
import utils.Config;
import utils.MyEventTimeExtractor;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.ofMinutes;

public class MainQuery1 {

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        Comparator<ArticleCount> byCount = Comparator.comparingLong(ArticleCount::getCount);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(),new PostDeserializer())));


        KTable<Windowed<String>, Long> counts = source
                .map((k, v) -> KeyValue.pair(v.getArticleId(), 1))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(ofMinutes(1)))
                .count();

        KTable<Long, PriorityQueue<ArticleCount>> aggregate = counts
                .groupBy((w, c) -> new KeyValue<>(w.window().start(), new ArticleCount(w.key(), c))
                        , Grouped.with(Serdes.Long(), Serdes.serdeFrom(new ArticleCountSerializer(), new ArticleCountDeserializer()))
                )
                .aggregate(
                        () -> new PriorityQueue<>(byCount),

                        (windowedString, articleCount, queue) -> {
                            queue.add(articleCount);
                            return queue;
                        },

                        (windowedString, articleCount, queue) -> {
                            queue.remove(articleCount);
                            return queue;
                        },
                        Materialized.with(Serdes.Long(), new PriorityQueueSerde<>(byCount, new ArticleCountSerializer(), new ArticleCountDeserializer())));

       /* aggregate.toStream().foreach(new ForeachAction<Long, PriorityQueue<ArticleCount>>() {
            @Override
            public void apply(Long key, PriorityQueue<ArticleCount> articleCounts) {
                System.out.println("key: "+ key+"\tlista: "+ articleCounts.toString());
            }
        });*/

        KTable<Long, String> topViewCounts = aggregate
                .mapValues(queue -> {
                    final StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < Config.topN; i++) {
                        ArticleCount record = queue.poll();
                        if (record == null) {
                            break;
                        }
                        sb.append(record.getArticleId());
                        sb.append("\n");
                    }
                    return sb.toString();
                });

        topViewCounts.toStream().to(Config.OutTOPIC, Produced.with(Serdes.Long(), Serdes.String()));


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
