package consumer;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ConsumerOrder {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "35.228.123.148:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "object_group");
properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024"); // ожидание накопление на сервере такого кол-ва данных
properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000");
        KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Collections.singletonList("topic_object"));
        try {
            while (true) {
                ConsumerRecords<Integer, String> orders = kafkaConsumer.poll(Duration.ofSeconds(60));
                for (ConsumerRecord<Integer, String> order : orders) {
                    System.out.printf("Product name %d = %s \n", order.key(), order.value());
                }
            }
        } finally {
            kafkaConsumer.close();
        }

    }
}
