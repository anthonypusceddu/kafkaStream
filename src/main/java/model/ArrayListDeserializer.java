package model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import utils.SerDes;


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class ArrayListDeserializer implements Deserializer<ArrayList<ArticleCount>> {

    @Override
    public void configure(Map<java.lang.String, ?> configs, boolean isKey) {
    }

    @Override
    public ArrayList<ArticleCount> deserialize(String s, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        final ArrayList<ArticleCount> arrayList = new ArrayList<>();
        final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));

        try {
            final int records = dataInputStream.readByte();
            for (int i = 0; i < records; i++) {
                final byte[] valueBytes = new byte[dataInputStream.readByte()];
                dataInputStream.read(valueBytes);
                arrayList.add(SerDes.deserializeArticleCount(valueBytes));
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize ArrayList", e);
        }

        return arrayList;
    }

    @Override
    public void close() {
    }
}