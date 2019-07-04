package serDes;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.ArticleCount;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class ArticleCountDeserializer implements Deserializer<ArticleCount> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public ArticleCount deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        ArticleCount articleCount = null;
        try {
            articleCount = mapper.readValue(bytes, ArticleCount.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return articleCount;
    }

    @Override
    public void close() {

    }
}
