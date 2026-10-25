package kafka.repaso.kafka.domain;

import lombok.*;

import java.math.BigDecimal;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class CreateProductRestModel {
    private String title;
    private BigDecimal price;
    private Integer quantity;
}
