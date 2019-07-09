package model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import scala.Tuple5;
import scala.Tuple6;

import java.util.Map;

public class TupleDeserializer implements Deserializer<Tuple6<Integer, Integer, Integer, Integer, Integer,Long>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Tuple6<Integer, Integer, Integer, Integer, Integer,Long> deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        Tuple6<Integer, Integer, Integer, Integer, Integer,Long> tuple = null;
        try {
            tuple = mapper.readValue(bytes, Tuple6.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return tuple;
    }

    @Override
    public void close() {

    }
}
