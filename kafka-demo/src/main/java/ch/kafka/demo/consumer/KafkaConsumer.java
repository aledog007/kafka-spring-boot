package ch.kafka.demo.consumer;

import ch.kafka.demo.model.CustomMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "testTopic", groupId = "myGroup")
    public void listen(CustomMessage message) {
        System.out.println("Received message in group myGroup: " + message);
    }
}
