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
import org.apache.kafka.streams.processor.ProcessorContext;
import scala.Tuple2;
import utils.Config;
import utils.MyEventTimeExtractor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.ofMinutes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

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

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(),new PostDeserializer())));


        KStream<Windowed<String>, Long> counts = source
                .map((k, v) -> KeyValue.pair(v.getArticleId(), 1))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(ofMinutes(1)))
                .count()
                .toStream();




        KGroupedStream<Long, Integer> aggregate = counts
                .map((w, c) -> KeyValue.pair(w.window().start(),1))
                .groupByKey(Grouped.with(Serdes.Long(),Serdes.Integer()));

        aggregate.reduce(new Reducer<Integer>() {
            @Override
            public Integer apply(Integer integer, Integer v1) {
                System.out.println("intero1: " + integer + "\t Intero2: " + v1);
                return  integer+v1;
            }
        });
                //.windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(ofMinutes(1)))
                /*.aggregate(new Initializer<ArrayList<ArticleCount>>() {
                    @Override
                    public ArrayList<ArticleCount> apply() {
                        return new ArrayList<>();
                    }
                }, new Aggregator<Long, ArticleCount, ArrayList<ArticleCount>>() {
                    @Override
                    public ArrayList<ArticleCount> apply(Long aLong, ArticleCount articleCount, ArrayList<ArticleCount> articleCounts) {
                        articleCounts.add(articleCount);
                        return articleCounts;
                    }
                }, Materialized.with(Serdes.Long(), Serdes.serdeFrom(new ArrayListSerializer(), new ArrayListDeserializer())));


*/


                /*.groupBy(new KeyValueMapper<Windowed<String>, Long, KeyValue<Long, ArticleCount>>() {
                             @Override
                             public KeyValue<Long, ArticleCount> apply(Windowed<String> w, Long c) {
                                 return KeyValue.pair(w.window().start(), new ArticleCount(w.key(), Math.toIntExact(c)));
                             }
                         },
                        Grouped.with(Serdes.Long(), Serdes.serdeFrom(new ArticleCountSerializer(), new ArticleCountDeserializer())))
                .
        /*counts.toStream()
                .foreach(new ForeachAction<Long, ArrayList<ArticleCount>>() {
                    @Override
                    public void apply(Long key, ArrayList<ArticleCount> articleCounts) {
                        System.out.println("Finestra: "+key+"\t lista: "+articleCounts);
                    }
                });
*/
        // need to override value serde to Long type
        /*counts.toStream()
                //.map((KeyValueMapper<Windowed<String>, Long, KeyValue<String, Long>>) (stringWindowed, v) -> KeyValue.pair(stringWindowed.key(),v))
                // .print();
                .foreach(new ForeachAction<Windowed<String>, Long>() {
                    @Override
                    public void apply(Windowed<String> windowedUserId, Long count) {
                        System.out.println("Finestra: "+windowedUserId.window()+"\t ID: "+ windowedUserId.key()+"\t Count: " +count);
                    }
                });*/

      /*  aggregate.toStream()
                .foreach(new ForeachAction<Long, Long>() {
                    @Override
                    public void apply(Long key, Long aLong2) {
                        System.out.println("Finestra: "+key+"\t count: "+aLong2);

                    }
                });*/
                        //.to(Config.OutTOPIC, Produced.with(Serdes.String(), Serdes.Long()));

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
/*

new Initializer<ArrayList<ArticleCount>>() {
                    @Override
                    public ArrayList<ArticleCount> apply() {
                        return new ArrayList<>();
                    }
                }, new Aggregator<Long, ArticleCount, ArrayList<ArticleCount>>() {
                    @Override
                    public ArrayList<ArticleCount> apply(Long key, ArticleCount articleCount, ArrayList<ArticleCount> articleCounts) {
                        articleCounts.add(articleCount);
                        return articleCounts;
                    }
                }, new Aggregator<Long, ArticleCount, ArrayList<ArticleCount>>() {
                    @Override
                    public ArrayList<ArticleCount> apply(Long key, ArticleCount articleCount, ArrayList<ArticleCount> articleCounts) {
                        return articleCounts;
                    }
                }, Materialized.with(Serdes.Long(), Serdes.serdeFrom(new ArrayListSerializer(), new ArrayListDeserializer())));

 */