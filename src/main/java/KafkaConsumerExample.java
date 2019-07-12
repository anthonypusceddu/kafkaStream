import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import utils.Config;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Config.kafkaBrokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singleton(Config.OutTOPIC1));
        return consumer;
    }

    private static void runConsumer(BufferedWriter bw1) throws InterruptedException, IOException {


        final Consumer<Long, String> consumer = createConsumer();

        final int giveUp = 100000;   int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(1000));

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                try {
                    bw1.write(String.valueOf(record.key()));
                    if ( record.value()!= null) {
                        bw1.write(record.value());
                    }
                    bw1.write("\n");
                    bw1.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        FileWriter writer1 = new FileWriter("result/Query1/query1_day.txt");
        BufferedWriter bw1 = new BufferedWriter(writer1);
        KafkaConsumerExample.runConsumer(bw1);
    }

}

