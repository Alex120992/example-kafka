package lessons4.lesson_custom_deser;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
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

        KafkaConsumer<Integer, GenericRecord> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList("GenericRecordAvroTopic"));

        ConsumerRecords<Integer, GenericRecord> records = kafkaConsumer.poll(Duration.ofSeconds(60));
        for (ConsumerRecord<Integer, GenericRecord> x: records){
            GenericRecord someObject = x.value();
            System.out.printf("Product name %d = %d \n",x.key(), someObject.get("age"));
            System.out.printf("%s, %s \n",someObject.get("name"), someObject.get("surname"));
        }
        kafkaConsumer.close();
    }
}
