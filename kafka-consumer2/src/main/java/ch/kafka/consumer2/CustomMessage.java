package ch.kafka.consumer2;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CustomMessage {
    private String id;
    private String message;
    private LocalDateTime timestamp;
}
