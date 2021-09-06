import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class OrderCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        System.out.printf("topic: %s \n", metadata.topic());
        System.out.printf("partition: %d \n", metadata.partition());
        System.out.printf("offset: %d \n", metadata.offset());
        if (exception!=null){
            exception.printStackTrace();
        }
    }
}
