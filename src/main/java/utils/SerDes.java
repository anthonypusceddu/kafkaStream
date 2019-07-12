package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.ArticleCount;

import java.io.*;

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

    public static byte[] serialize(Object object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {

            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(object);
            oos.flush();
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bos.toByteArray();
    }

    public static Object deserialize(byte[] bytes) {
        ObjectInputStream is = null;
        Object object = null;
        try {
            is = new ObjectInputStream(new ByteArrayInputStream(bytes));
            object = is.readObject();
            is.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return object;
    }
}
