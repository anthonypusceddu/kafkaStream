package serDes;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.ArticleCount;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ArticleCountSerializer implements Serializer<ArticleCount> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, ArticleCount articleCount) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(articleCount).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
