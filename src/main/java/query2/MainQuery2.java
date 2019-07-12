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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static java.time.Duration.ofMinutes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class MainQuery2 {

    public static void main(final String[] args) throws IOException {
        final Properties props= KafkaProperties.setProperties(2);


        //stream builder
        StreamsBuilder builder = new StreamsBuilder();


        //producer's serializer
        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(),new PostDeserializer())));


        /*
          filter comments with depth 1
          map to (timeSlot, 1)
         */
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

        /*
           groupByKey
           tumbling window 24 hours
           count elements in window
           suppress to obtain just the last record
         */
        KTable<Windowed<Integer>, Long> count24H = filter
                .groupByKey(Grouped.with(Serdes.Integer(), Serdes.Integer()))
                .windowedBy(TimeWindows.of(Duration.ofHours(24)).until(86460000L).grace(ofMinutes(1)))
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()));



        /*
          same as above with tumbling window 7 days
         */
        KTable<Windowed<Integer>, Long> count7D = filter
                .groupByKey(Grouped.with(Serdes.Integer(), Serdes.Integer()))
                .windowedBy(TimeWindows.of(Duration.ofDays(7)).until(604860000L).grace(ofMinutes(1)))
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()));


        /*
          same as above with window 30 days
         */
        KTable<Windowed<Integer>, Long> count1M = filter
                .groupByKey(Grouped.with(Serdes.Integer(), Serdes.Integer()))
                .windowedBy(TimeWindows.of(Duration.ofDays(30)).until(2678460000L).grace(ofMinutes(1)))
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()));



        /*
          DONE: scrittura

        FileWriter writer1 = new FileWriter("result/Query2/query2_day.txt");
        FileWriter writer2 = new FileWriter("result/Query2/query2_week.txt");
        FileWriter writer3 = new FileWriter("result/Query2/query2_month.txt");
        BufferedWriter bw1 = new BufferedWriter(writer1);
        BufferedWriter bw2 = new BufferedWriter(writer2);
        BufferedWriter bw3 = new BufferedWriter(writer3);

        count24H.toStream().foreach(new ForeachAction<Windowed<Integer>, Long>() {
            @Override
            public void apply(Windowed<Integer> integerWindowed, Long aLong) {
                try {
                    bw1.write(String.valueOf(integerWindowed.window().start()));
                    bw1.write("\t");
                    bw1.write(aLong.toString());
                    bw1.write("\n");
                    bw1.flush();
                } catch (IOException e) {
                    System.err.format("IOException: %s%n", e);
                }
            }
        });

        count7D.toStream().foreach(new ForeachAction<Windowed<Integer>, Long>() {
            @Override
            public void apply(Windowed<Integer> integerWindowed, Long aLong) {
                try {
                    bw2.write(String.valueOf(integerWindowed.window().start()));
                    bw2.write("\t");
                    bw2.write(aLong.toString());
                    bw2.write("\n");
                    bw2.flush();
                } catch (IOException e) {
                    System.err.format("IOException: %s%n", e);
                }
            }
        });


        count1M.toStream().foreach(new ForeachAction<Windowed<Integer>, Long>() {
            @Override
            public void apply(Windowed<Integer> integerWindowed, Long aLong) {
                try {
                    bw3.write(String.valueOf(integerWindowed.window().start()));
                    bw3.write("\t");
                    bw3.write(aLong.toString());
                    bw3.write("\n");
                    bw3.flush();
                } catch (IOException e) {
                    System.err.format("IOException: %s%n", e);
                }
            }
        });

        */

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        AttachCtrlC.attach(streams);

        System.exit(0);
    }


}
