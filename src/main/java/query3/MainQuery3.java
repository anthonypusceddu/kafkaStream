package query3;

import model.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import scala.Tuple5;
import scala.Tuple6;
import utils.AttachCtrlC;
import utils.Config;
import utils.KafkaProperties;
import utils.TimeSlot;

import java.util.Properties;

public class MainQuery3 {

    public static void main(final String[] args) {
        final Properties props= KafkaProperties.setProperties();


        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(),new PostDeserializer())));


        KStream<Long,Tuple6<Integer, Integer, Integer, Integer, Integer,Long>> stringInputStream = source
                .mapValues(new ValueMapper< Post, Tuple6<Integer, Integer, Integer, Integer, Integer,Long>>() {
                    @Override
                    public Tuple6<Integer, Integer, Integer, Integer, Integer,Long> apply(Post post) {

                        if (post.isEditorsSelection() && post.getCommentType().equals("comment"))
                            post.setRecommendations(post.getRecommendations() + post.getRecommendations() * 10 / 100);

                        return new Tuple6<Integer, Integer, Integer, Integer, Integer,Long>(post.getUserID(), post.getDepth(), post.getRecommendations(), post.getInReplyTo(), post.getCommentID(),post.getCreateDate());


                        //return new KeyValue<>(TimeSlot.getTimeSlot(post), 1);
                    }
                });

        KStream<Long,Tuple6<Integer, Integer, Integer, Integer, Integer,Long>> out= stringInputStream
                .filter(new Predicate<Long, Tuple6<Integer, Integer, Integer, Integer, Integer,Long>>() {
                    @Override
                    public boolean test(Long l,Tuple6<Integer, Integer, Integer, Integer, Integer,Long> tuple ) {
                        if(tuple._1()!= -1 && tuple._2()!=-1)
                            return  true;
                        return false;
                    }
                });

        //out.to(Config.OutTOPIC, Produced.with(Serdes.Long(),Serdes.serdeFrom(new TupleSerializer(),new TupleDeserializer())));


        /*out.foreach(new ForeachAction<Long, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
            public void apply(Long key, Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
                System.out.println(key + ": " + value._2());
            }
        });*/
        // create store
        StoreBuilder<KeyValueStore<Long,Tuple6<Integer, Integer, Integer, Integer, Integer,Long>>> keyValueStoreBuilder =
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
