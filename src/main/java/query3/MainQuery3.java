package query3;

import model.Post;
import model.PostDeserializer;
import model.PostSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import scala.Tuple5;
import utils.Config;
import utils.KafkaProperties;
import utils.TimeSlot;

import java.util.Properties;

public class MainQuery3 {

    public static void main(final String[] args) {
        final Properties props= KafkaProperties.setProperties();

        Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, Post> source = builder.stream(Config.TOPIC,
                Consumed.with(Serdes.Long(), Serdes.serdeFrom(new PostSerializer(),new PostDeserializer())));


        KStream<Long,Tuple5<Integer, Integer, Integer, Integer, Integer>> count1H = source
                .mapValues(new ValueMapper< Post, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple5<Integer, Integer, Integer, Integer, Integer> apply(Post post) {

                        if (post.isEditorsSelection() && post.getCommentType().equals("comment"))
                            post.setRecommendations(post.getRecommendations() + post.getRecommendations() * 10 / 100);
                        return new Tuple5<Integer, Integer, Integer, Integer, Integer>(post.getUserID(), post.getDepth(), post.getRecommendations(), post.getInReplyTo(), post.getCommentID());


                        //return new KeyValue<>(TimeSlot.getTimeSlot(post), 1);
                    }
                });
    }
}
