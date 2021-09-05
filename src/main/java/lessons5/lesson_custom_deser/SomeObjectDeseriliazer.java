package lessons5.lesson_custom_deser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class SomeObjectDeseriliazer implements Deserializer<SomeObject> {


    @Override
    public SomeObject deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        SomeObject someObject = null;
        try {
            someObject = objectMapper.readValue(data, SomeObject.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return someObject;
    }
}
