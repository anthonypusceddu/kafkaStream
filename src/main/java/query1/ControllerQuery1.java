package query1;

import model.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.kstream.*;
import serDes.*;
import utils.AttachCtrlC;
import utils.Config;
import utils.KafkaProperties;
import utils.SerDes;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.*;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.time.Duration.ofMinutes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class ControllerQuery1 {
    public static void main(final String[] args){
        final Properties props = KafkaProperties.setProperties(1);
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(), new PostDeserializer())));

          /*
          map to (articleID, occurrencies set to 1)
          group by articleID
          tumbling window with lateness set to 1 min
          count the occurrencies
         */

        KStream<Long, ArticleCount[]> Query1H = Query1.executeQuery(source, 1,3660000);
        KStream<Long, ArticleCount[]> Query24H =Query1.executeQuery(source,24,86460000L);
        KStream<Long, ArticleCount[]> Query7D = Query1.executeQuery(source,24*7,604860000L);

        //counts1H.to(Config.OutTOPIC1, Produced.with(windowedLongSerde, Serdes.serdeFrom(new ArrayListSerializer(),new ArrayListDeserializer())));

        Query1H.foreach(new ForeachAction<Long, ArticleCount[]>() {
            @Override
            public void apply(Long aLong, ArticleCount[] articleCounts) {
                System.out.println("window: "+aLong+"\t"+ Arrays.toString(articleCounts));
            }
        });
        Query24H.foreach(new ForeachAction<Long, ArticleCount[]>() {
            @Override
            public void apply(Long aLong, ArticleCount[] articleCounts) {
                System.out.println("window: "+aLong+"\t"+ Arrays.toString(articleCounts));
            }
        });
        Query7D.foreach(new ForeachAction<Long, ArticleCount[]>() {
            @Override
            public void apply(Long aLong, ArticleCount[] articleCounts) {
                System.out.println("window: "+aLong+"\t"+ Arrays.toString(articleCounts));
            }
        });

        /*KTable<Windowed<String>, Long> counts24H = source
                .map((k, v) -> KeyValue.pair(v.getArticleId(), 1))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(24)).until(86460000L).grace(ofMinutes(1)))
                .count();

        KTable<Windowed<String>, Long> counts7Days = source
                .map((k, v) -> KeyValue.pair(v.getArticleId(), 1))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(7)).until(7*86460000L).grace(ofMinutes(1)))
                .count();

        KTable<Long, String> topViewCounts1H = Query1.executeQuery(counts1H);
        topViewCounts1H.toStream().to(Config.OutTOPIC1, Produced.with(Serdes.Long(), Serdes.String()));

        KTable<Long, String> topViewCounts24H = Query1.executeQuery(counts24H);
        topViewCounts24H.toStream().to(Config.OutTOPIC2, Produced.with(Serdes.Long(), Serdes.String()));

        KTable<Long, String> topViewCounts7D = Query1.executeQuery(counts7Days);
        topViewCounts7D.toStream().to(Config.OutTOPIC3, Produced.with(Serdes.Long(), Serdes.String()));*/

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        AttachCtrlC.attach(streams);

        System.exit(0);
    }
}
