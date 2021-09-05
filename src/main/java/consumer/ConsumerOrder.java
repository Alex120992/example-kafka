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
//        properties.setProperty("enable.auto.commit", "false"); // отключение автокоммита для poll метода
//        properties.setProperty("auto.commit.interval.ms","7000");//по умолч автокоммит каждые 5 секунд, так меняем его значение
//      проблемы с автокоммитами при балансировке.
        properties.setProperty("auto.commit.offset", "false"); // для того чтобы offset не коммитились автоматически


        KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // настройка ребалансировки
        class RebalanceHandler implements ConsumerRebalanceListener {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//                Таким образом, этот метод будет вызываться всякий раз, когда запускается
//                перебалансировка и до того, как разделы будут переделана именно у этого потребителя.
//                Таким образом, если на стороне потребителя есть какие-либо операции по очистке, а также если есть какие-либо записи
//                    , смещения которые еще не зафиксированы, это отличное место для фиксации этих зачетов.
                kafkaConsumer.commitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        }

        kafkaConsumer.subscribe(Collections.singletonList("topic_object"), new RebalanceHandler());
        try {
            while (true) {
                ConsumerRecords<Integer, String> orders = kafkaConsumer.poll(Duration.ofSeconds(60));
                for (ConsumerRecord<Integer, String> order : orders) {
                    System.out.printf("Product name %d = %s \n", order.key(), order.value());
                    //                kafkaConsumer.commitSync();// будет коммитить текущий offset сразу после выполнения
//                kafkaConsumer.commitAsync();// commit будет отправлен асинхронно и у нас будет выполняться следующий offset
//               Но если наш коммит потерпит неудачу, то он не будет восстановлен и при балансировке может начат дублирование так как он начнет с упавшего значения
                    kafkaConsumer.commitAsync(Collections.singletonMap(new TopicPartition(order.topic(), order.partition())
                                    , new OffsetAndMetadata(order.offset() + 1)) // при балансировке будет считан следующий топик
                            , new OffsetCommitCallback() { // когда коммит завершит работу то получим этот коллбэк
                                @Override
                                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                                    if (exception != null) {
                                        //TODO логирование
                                    }
                                }
                            });
                }

            }
        } finally {
            kafkaConsumer.close();
        }

    }
}
