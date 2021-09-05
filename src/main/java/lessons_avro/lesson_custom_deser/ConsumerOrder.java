package lessons_avro.lesson_custom_deser;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerOrder {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "35.228.123.148:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "object_group");
        properties.setProperty("schema.registry.url", "http://35.228.123.148:8081");
        properties.setProperty("specific.avro.reader", "true");

        KafkaConsumer<Integer, com.zateev.kafka.avro.SomeObject> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList("topic_object"));

        ConsumerRecords<Integer, com.zateev.kafka.avro.SomeObject> records = kafkaConsumer.poll(Duration.ofSeconds(60));
        for (ConsumerRecord<Integer, com.zateev.kafka.avro.SomeObject> x: records){
            com.zateev.kafka.avro.SomeObject someObject = x.value();
            System.out.printf("Product name %d = %d \n",x.key(), someObject.getAge());
            System.out.printf("%s, %s \n",someObject.getName(), someObject.getSurname());
        }
        kafkaConsumer.close();
    }
}
