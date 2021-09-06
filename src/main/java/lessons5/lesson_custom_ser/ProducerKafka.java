package lessons5.lesson_custom_ser;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerKafka {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "35.228.123.148:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "lessons5.lesson_custom_ser.SomeObjectSerializer");
        properties.setProperty("partitioner.class", VIPartitioner.class.getName());

        KafkaProducer <Integer, SomeObject> kafkaProducer = new KafkaProducer<>(properties);

        ProducerRecord<Integer, SomeObject> record = new ProducerRecord<>("SomeObjectPartitionedTopic",7,
                new SomeObject("Sergey","Gom", 66));
        try {
            Future<RecordMetadata> send = kafkaProducer.send(record, new OrderCallback());
            System.out.println("Send message");
        } catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}
