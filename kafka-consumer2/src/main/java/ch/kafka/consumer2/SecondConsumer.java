package ch.kafka.consumer2;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class SecondConsumer {

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @KafkaListener(topics = "testTopic", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(CustomMessage message) {
        System.out.println("[consumer2] Received message in group " + groupId + ": " + message);
    }
}
