package lesson_custom_deser;


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
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "lesson_custom_deser.SomeObjectDeseriliazer");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "object_group");
        KafkaConsumer<Integer, SomeObject> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList("topic_object"));

        ConsumerRecords<Integer, SomeObject> records = kafkaConsumer.poll(Duration.ofSeconds(60));
        for (ConsumerRecord<Integer,SomeObject> x: records){
            SomeObject someObject = x.value();
            System.out.printf("Product name %d = %d \n",x.key(), someObject.getAge());
            System.out.printf("%s, %s \n",someObject.getName(), someObject.getSurname());
        }
        kafkaConsumer.close();
    }
}
