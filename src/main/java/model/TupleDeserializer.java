package model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import scala.Tuple5;

import java.util.Map;

public class TupleDeserializer implements Deserializer<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Tuple5<Integer, Integer, Integer, Integer, Integer> deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        Tuple5<Integer, Integer, Integer, Integer, Integer> tuple = null;
        try {
            tuple = mapper.readValue(bytes, Tuple5.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return tuple;
    }

    @Override
    public void close() {

    }
}
