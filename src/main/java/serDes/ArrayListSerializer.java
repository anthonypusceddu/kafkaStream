package serDes;


import model.ArticleCount;
import org.apache.kafka.common.serialization.Serializer;
import utils.SerDes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class ArrayListSerializer implements Serializer<ArrayList<ArticleCount>> {

    // Default constructor needed by Kafka
    public ArrayListSerializer() {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, ArrayList<ArticleCount> queue) {
        final int size = queue.size();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);
        final Iterator<ArticleCount> iterator = queue.iterator();
        try {
            dos.writeInt(size);
            while (iterator.hasNext()) {
                final byte[] bytes = SerDes.serializeArticleCount(iterator.next());
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to serDes ArrayList", e);
        }
        return baos.toByteArray();
    }

    @Override
    public void close() {
    }
}
