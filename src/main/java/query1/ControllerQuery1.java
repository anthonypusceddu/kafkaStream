package query1;

import model.ArticleCount;
import model.Post;
import model.PostDeserializer;
import model.PostSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.AttachCtrlC;
import utils.Config;
import utils.KafkaProperties;

import java.time.Duration;
import java.util.Comparator;
import java.util.Properties;

import static java.time.Duration.ofMinutes;

public class ControllerQuery1 {
    public static void main(final String[] args) {
        final Properties props = KafkaProperties.setProperties();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(), new PostDeserializer())));

          /*
          map to (articleID, occurrencies set to 1)
          group by articleID
          tumbling window with lateness set to 1 min
          count the occurrencies
         */
        KTable<Windowed<String>, Long> counts1H = source
                .map((k, v) -> KeyValue.pair(v.getArticleId(), 1))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(ofMinutes(1)))
                .count();

        KTable<Windowed<String>, Long> counts24H = source
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
        topViewCounts7D.toStream().to(Config.OutTOPIC3, Produced.with(Serdes.Long(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        AttachCtrlC.attach(streams);

        System.exit(0);
    }
}
