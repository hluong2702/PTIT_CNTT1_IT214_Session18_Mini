package org.example.ptit_cntt1_it214_session18_mini.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderSagaEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum EventType {
        ORDER_CREATED,
        STOCK_DEDUCTED,
        PAYMENT_PROCESSED,
        PAYMENT_FAILED,
        STOCK_COMPENSATED,
        ORDER_CONFIRMED,
        ORDER_CANCELLED
    }

    private String eventId;
    private EventType eventType;
    private Long orderId;
    private Long customerId;
    private Long productId;
    private Integer quantity;
    private BigDecimal amount;
    private String status;
    private String details;
    private LocalDateTime timestamp;
}
