package model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import scala.Tuple5;
import scala.Tuple6;

import java.util.Map;

public class TupleSerializer implements Serializer<Tuple6<Integer, Integer, Integer, Integer, Integer,Long>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, Tuple6<Integer, Integer, Integer, Integer, Integer,Long> tuple) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(tuple).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
