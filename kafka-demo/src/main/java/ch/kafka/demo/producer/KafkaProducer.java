package ch.kafka.demo.producer;

import ch.kafka.demo.model.CustomMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    public static final String TOPIC = "testTopic";

    @Autowired
    private KafkaTemplate<String, CustomMessage> kafkaTemplate;

    public void sendMessage(CustomMessage message) {
        kafkaTemplate.send(TOPIC, message.getId(), message);
    }
}
