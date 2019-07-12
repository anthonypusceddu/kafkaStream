package query3;

import model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import scala.Tuple5;
import utils.AttachCtrlC;
import utils.Config;
import utils.KafkaProperties;

import java.util.Properties;

public class MainQuery3 {

    public static void main(final String[] args) {
        final Properties props= KafkaProperties.setProperties();


        StreamsBuilder builder = new StreamsBuilder();


        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(),new PostDeserializer())));


        KStream<Long,Tuple5<Integer, Integer, Integer, Integer, Integer>> stringInputStream = source
                .mapValues(new ValueMapper< Post, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple5<Integer, Integer, Integer, Integer, Integer> apply(Post post) {

                        if (post.isEditorsSelection() && post.getCommentType().equals("comment"))
                            post.setRecommendations(post.getRecommendations() + post.getRecommendations() * 10 / 100);

                        return new Tuple5<Integer, Integer, Integer, Integer, Integer>(post.getUserID(), post.getDepth(), post.getRecommendations(), post.getInReplyTo(), post.getCommentID());


                        //return new KeyValue<>(TimeSlot.getTimeSlot(post), 1);
                    }
                });

        KStream<Long,Tuple5<Integer, Integer, Integer, Integer, Integer>> out= stringInputStream
                .filter(new Predicate<Long, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean test(Long l,Tuple5<Integer, Integer, Integer, Integer, Integer> tuple ) {
                        if(tuple._1()!= -1 && tuple._2()!=-1)
                            return  true;
                        return false;
                    }
                });


        // create store
        StoreBuilder<KeyValueStore<Long,Tuple5<Integer, Integer, Integer, Integer, Integer>>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("myProcessorState"),
                        Serdes.Long(), Serdes.serdeFrom(new TupleSerializer(),new TupleDeserializer()));
        // register store
        builder.addStateStore(keyValueStoreBuilder);

        out.process(new MyProcessFunction(),"myProcessorState");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        AttachCtrlC.attach(streams);

        System.exit(0);
    }
}
