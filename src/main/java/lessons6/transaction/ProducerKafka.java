package lessons6.transaction;

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
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "lessons6.transaction.SomeObjectSerializer");
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-producer-1"); //any
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-producer-1"); //any
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000"); // блокирует метод на определнное время
        // , если буфер полный ProducerConfig.BUFFER_MEMORY_CONFIG

        KafkaProducer <Integer, SomeObject> kafkaProducer = new KafkaProducer<>(properties);
        kafkaProducer.initTransactions();

        ProducerRecord<Integer, SomeObject> record = new ProducerRecord<>("SomeObjectPartitionedTopic",7,
                new SomeObject("Sergey","Gom", 66));
        ProducerRecord<Integer, SomeObject> record2 = new ProducerRecord<>("SomeObjectPartitionedTopic", 3,
                new SomeObject("Andrey", "Ivanov", 44));
        try {
            kafkaProducer.beginTransaction();
            Future<RecordMetadata> send = kafkaProducer.send(record, new OrderCallback());
            Future<RecordMetadata> send2 = kafkaProducer.send(record2, new OrderCallback());
            System.out.println("Send message");
        } catch (Exception e){
            kafkaProducer.abortTransaction();
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}
