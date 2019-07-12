package query2;

import model.Post;
import model.PostDeserializer;
import model.PostSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.*;

import java.time.Duration;
import java.util.Properties;

import static java.time.Duration.ofMinutes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class MainQuery2 {

    public static void main(final String[] args) {
        final Properties props= KafkaProperties.setProperties();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(),new PostDeserializer())));


        KStream<Integer, Integer> filter = source
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
                });

        KTable<Windowed<Integer>, Long> count1H = filter
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(24)).until(86460000L).grace(ofMinutes(1)))
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()));


        KTable<Windowed<Integer>, Long> count24H = filter
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(7)).until(604860000L).grace(ofMinutes(1)))
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()));


        KTable<Windowed<Integer>, Long> count7D = filter
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(30)).until(2678460000L).grace(ofMinutes(1)))
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()));



        //TODO: change retention time to test 7 days and 1 month
        count1H.toStream().to(Config.OutTOPIC1, Produced.with(Serdes.serdeFrom(new TimeWindowedSerializer<>(), new TimeWindowedDeserializer<>()), Serdes.Long()));
        count24H.toStream().to(Config.OutTOPIC2, Produced.with(Serdes.serdeFrom(new TimeWindowedSerializer<>(), new TimeWindowedDeserializer<>()), Serdes.Long()));
        count7D.toStream().to(Config.OutTOPIC3, Produced.with(Serdes.serdeFrom(new TimeWindowedSerializer<>(), new TimeWindowedDeserializer<>()), Serdes.Long()));

        /*count1H.toStream().foreach(new ForeachAction<Windowed<Integer>, Long>() {
            @Override
            public void apply(Windowed<Integer> integerWindowed, Long aLong) {
                System.out.println(integerWindowed+"\t"+aLong);
            }
        });*/

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
        AttachCtrlC.attach(streams);

        System.exit(0);
    }


}
