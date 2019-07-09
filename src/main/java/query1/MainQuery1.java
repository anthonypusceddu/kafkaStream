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
import utils.AttachCtrlC;
import utils.Config;
import utils.KafkaProperties;
import utils.MyEventTimeExtractor;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.ofMinutes;

public class MainQuery1 {

    public static void main(final String[] args) {
        final Properties props= KafkaProperties.setProperties();


        //serdes per la finestra
        Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        Comparator<ArticleCount> byCount = Comparator.comparingLong(ArticleCount::getCount);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(),new PostDeserializer())));


        /*
          map to (articleID, occurrencies set to 1)
          group by articleID
          tumbling window with lateness set to 1 min
          count the occurrencies
         */
        KTable<Windowed<String>, Long> counts = source
                .map((k, v) -> KeyValue.pair(v.getArticleId(), 1))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(ofMinutes(1)))
                .count();


        /*
          group by window
          aggregate in priority queue already ordered
         */
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


       /*
         map values to take the first 10 articleIDs
        */
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
        AttachCtrlC.attach(streams);

        System.exit(0);
    }
}
