package kafka.repaso.kafka.Controller;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.Date;

@Deprecated
@AllArgsConstructor
@NoArgsConstructor
public class ErrorMessage {
    private Date timestamp;
    private String message;
    private String details;
}
