package ch.kafka.demo.controller;

import ch.kafka.demo.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {

    @Autowired
    private KafkaProducer producer;

    @GetMapping("/send")
    public String sendMessage(@RequestParam("message") String message) {
        producer.sendMessage(message);
        return "Message sent";
    }
}
