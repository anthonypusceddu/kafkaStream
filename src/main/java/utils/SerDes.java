package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.ArticleCount;

public class SerDes {


    public static byte[] serializeArticleCount(ArticleCount articleCount){
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(articleCount).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    public static ArticleCount deserializeArticleCount(byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        ArticleCount articleCount = null;
        try {
            articleCount = mapper.readValue(bytes, ArticleCount.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return articleCount;
    }
}
