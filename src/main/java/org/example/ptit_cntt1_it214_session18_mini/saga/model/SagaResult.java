package org.example.ptit_cntt1_it214_session18_mini.saga.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.ptit_cntt1_it214_session18_mini.order.model.OrderStatus;

import java.math.BigDecimal;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SagaResult {
    private Long orderId;
    private OrderStatus finalStatus;
    private boolean success;
    private String message;
    private BigDecimal amount;
    private Integer remainingStock;
    private List<String> executionLogs;
}
