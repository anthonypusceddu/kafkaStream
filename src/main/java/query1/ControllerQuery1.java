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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
    public static void main(final String[] args) throws IOException {
        final Properties props = KafkaProperties.setProperties(1);

        //stream builder
        StreamsBuilder builder = new StreamsBuilder();


        //producer's serializer
        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(), new PostDeserializer())));


        //execution of query1 with 3 windows type
        KStream<Long, ArticleCount[]> Query1H = Query1.executeQuery(source, 1,3660000);
        KStream<Long, ArticleCount[]> Query24H =Query1.executeQuery(source,24,86460000L);
        KStream<Long, ArticleCount[]> Query7D = Query1.executeQuery(source,24*7,604860000L);




        /*
          DONE: scrittura


        FileWriter writer1 = new FileWriter("result/Query1/query1_hour.txt");
        FileWriter writer2 = new FileWriter("result/Query1/query1_day.txt");
        FileWriter writer3 = new FileWriter("result/Query1/query1_week.txt");
        BufferedWriter bw1 = new BufferedWriter(writer1);
        BufferedWriter bw2 = new BufferedWriter(writer2);
        BufferedWriter bw3 = new BufferedWriter(writer3);

        Query1H.foreach(new ForeachAction<Long, ArticleCount[]>() {
            @Override
            public void apply(Long aLong, ArticleCount[] articleCounts) {

                //System.out.println(aLong + "\t" + Arrays.toString(articleCounts));
                try {
                    bw1.write(String.valueOf(aLong));
                    bw1.write("\t");
                    bw1.write(Arrays.toString(articleCounts));
                    bw1.write("\n");
                    bw1.flush();
                } catch (IOException e) {
                    System.err.format("IOException: %s%n", e);
                }
            }
        });
        Query24H.foreach(new ForeachAction<Long, ArticleCount[]>() {
            @Override
            public void apply(Long aLong, ArticleCount[] articleCounts) {

                //System.out.println(aLong + "\t" + Arrays.toString(articleCounts));
                try {
                    bw2.write(String.valueOf(aLong));
                    bw2.write("\t");
                    bw2.write(Arrays.toString(articleCounts));
                    bw2.write("\n");
                    bw2.flush();
                } catch (IOException e) {
                    System.err.format("IOException: %s%n", e);
                }
            }
        });
        Query7D.foreach(new ForeachAction<Long, ArticleCount[]>() {
            @Override
            public void apply(Long aLong, ArticleCount[] articleCounts) {

                //System.out.println(aLong + "\t" + Arrays.toString(articleCounts));
                try {
                    bw3.write(String.valueOf(aLong));
                    bw3.write("\t");
                    bw3.write(Arrays.toString(articleCounts));
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
