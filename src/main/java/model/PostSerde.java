package model;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class PostSerde implements Serde<Post> {
    @Override
    public Serializer<Post> serializer() {
        return new PostSerializer();
    }

    @Override
    public Deserializer<Post> deserializer() {
        return new PostDeserializer();
    }
}
