package ch.kafka.demo.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "testTopic", groupId = "myGroup")
    public void listen(String message) {
        System.out.println("Received message in group myGroup: " + message);
    }
}
