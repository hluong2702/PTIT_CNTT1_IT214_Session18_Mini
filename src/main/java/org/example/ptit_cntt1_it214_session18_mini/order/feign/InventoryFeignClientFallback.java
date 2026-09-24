package org.example.ptit_cntt1_it214_session18_mini.order.feign;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
@Slf4j
public class InventoryFeignClientFallback implements InventoryFeignClient {

    @Override
    public ProductDto getProductById(Long id) {
        log.warn("[CIRCUIT BREAKER FALLBACK] Inventory Service is unavailable during getProductById(id={}). Returning fallback product.", id);
        return ProductDto.builder()
                .id(id)
                .code("FALLBACK_TEMP")
                .name("Tạm ngưng phục vụ (Fallback Mode)")
                .price(BigDecimal.ZERO)
                .stockQuantity(0)
                .description("Dịch vụ kho tạm thời không phản hồi. Vui lòng thử lại sau.")
                .build();
    }

    @Override
    public ProductDto deductStock(Long productId, Integer quantity) {
        log.error("[CIRCUIT BREAKER FALLBACK] Inventory Service is down during deductStock. Rejecting operation to prevent cascading failure.");
        throw new RuntimeException("Inventory service circuit is OPEN/UNAVAILABLE: Cannot deduct stock for product " + productId);
    }

    @Override
    public ProductDto compensateStock(Long productId, Integer quantity) {
        log.warn("[CIRCUIT BREAKER FALLBACK] Inventory Service is down during compensateStock. Queuing compensation event.");
        return ProductDto.builder()
                .id(productId)
                .name("Compensate Queued")
                .stockQuantity(0)
                .build();
    }
}
