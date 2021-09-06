package lessons4.customschema_avro;

import com.zateev.kafka.avro.SomeObject;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.Parser;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerKafka {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "35.228.123.148:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://35.228.123.148:8081");

        KafkaProducer<Integer, GenericRecord> kafkaProducer = new KafkaProducer<>(properties);
        Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "  \"namespace\": \"com.zateev.kafka.avro\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"SomeObject\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                "    {\"name\": \"surname\", \"type\": \"string\"},\n" +
                "    {\"name\": \"age\", \"type\": \"int\"}\n" +
                "  ]\n" +
                "}");
        GenericRecord someObject = new GenericData.Record(schema);
        someObject.put("name","Aleksey");
        someObject.put("surname","Ivanov");
        someObject.put("age",33);


        ProducerRecord<Integer, GenericRecord> record = new ProducerRecord<>("GenericRecordAvroTopic", 1,
                someObject);
        try {
            Future<RecordMetadata> send = kafkaProducer.send(record, new OrderCallback());
            System.out.println("Send message");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
}
