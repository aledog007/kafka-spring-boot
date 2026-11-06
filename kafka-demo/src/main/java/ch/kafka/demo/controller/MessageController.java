package ch.kafka.demo.controller;

import ch.kafka.demo.model.CustomMessage;
import ch.kafka.demo.producer.KafkaProducer;
import java.time.LocalDateTime;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {

    @Autowired
    private KafkaProducer producer;

    /** Teil 2: einfacher Text, die ID wird automatisch vergeben. */
    @GetMapping("/send")
    public String sendMessage(@RequestParam("message") String message) {
        producer.sendMessage(new CustomMessage(UUID.randomUUID().toString(), message, LocalDateTime.now()));
        return "Message sent";
    }

    /** Teil 3: strukturierte Nachricht mit eigener ID und aktuellem Zeitstempel. */
    @GetMapping("/send-custom")
    public CustomMessage sendCustomMessage(@RequestParam("id") String id,
                                           @RequestParam("message") String message) {
        CustomMessage customMessage = new CustomMessage(id, message, LocalDateTime.now());
        producer.sendMessage(customMessage);
        return customMessage;
    }
}
