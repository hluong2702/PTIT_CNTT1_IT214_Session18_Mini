package org.example.ptit_cntt1_it214_session18_mini.order.service;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.ptit_cntt1_it214_session18_mini.inventory.model.Product;
import org.example.ptit_cntt1_it214_session18_mini.inventory.service.InventoryService;
import org.example.ptit_cntt1_it214_session18_mini.order.feign.ProductDto;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryClientService {

    private final InventoryService inventoryService;
    private final AtomicBoolean simulateInventoryDown = new AtomicBoolean(false);

    public void setSimulateInventoryDown(boolean down) {
        this.simulateInventoryDown.set(down);
        log.warn("[SIMULATION] Inventory Service DOWN simulation flag set to: {}", down);
    }

    public boolean isSimulateInventoryDown() {
        return simulateInventoryDown.get();
    }

    /**
     * Gọi sang Inventory Service với Resilience4j Circuit Breaker:
     * - Trạng thái CLOSED: Mọi lệnh gọi đều bình thường.
     * - Nếu thất bại vượt ngưỡng -> Trạng thái OPEN: Không gọi hàm mà nhảy thẳng vào fallbackDeductStock.
     * - Sau thời gian chờ -> Trạng thái HALF-OPEN: Cho phép vài request thử nghiệm.
     */
    @CircuitBreaker(name = "inventoryService", fallbackMethod = "fallbackDeductStock")
    public ProductDto deductStock(Long productId, Integer quantity) {
        if (simulateInventoryDown.get()) {
            log.error("[CIRCUIT BREAKER TARGET FAIL] Inventory Service simulated connection crash / timeout!");
            throw new RuntimeException("Connection refused: inventory-service:8082 is down!");
        }

        Product product = inventoryService.deductStock(productId, quantity);
        return mapToDto(product);
    }

    @CircuitBreaker(name = "inventoryService", fallbackMethod = "fallbackCompensateStock")
    public ProductDto compensateStock(Long productId, Integer quantity) {
        Product product = inventoryService.compensateStock(productId, quantity);
        return mapToDto(product);
    }

    @CircuitBreaker(name = "inventoryService", fallbackMethod = "fallbackGetProduct")
    public ProductDto getProductById(Long productId) {
        if (simulateInventoryDown.get()) {
            log.error("[CIRCUIT BREAKER TARGET FAIL] Inventory Service simulated timeout on getProductById");
            throw new RuntimeException("Connection timeout to inventory-service");
        }
        Product product = inventoryService.getProductById(productId);
        return mapToDto(product);
    }

    // ================= FALLBACK METHODS =================

    public ProductDto fallbackDeductStock(Long productId, Integer quantity, Throwable throwable) {
        log.error("[CIRCUIT BREAKER FALLBACK ACTIVATED] deductStock failed. Reason: {}. Protecting system against cascading failure!",
                throwable.getMessage());
        throw new RuntimeException("Circuit Breaker Fallback: inventory-service is currently unavailable. " + throwable.getMessage());
    }

    public ProductDto fallbackCompensateStock(Long productId, Integer quantity, Throwable throwable) {
        log.warn("[CIRCUIT BREAKER FALLBACK ACTIVATED] compensateStock fallback triggered: {}", throwable.getMessage());
        return ProductDto.builder()
                .id(productId)
                .name("Compensate Pending Fallback")
                .stockQuantity(0)
                .build();
    }

    public ProductDto fallbackGetProduct(Long productId, Throwable throwable) {
        log.warn("[CIRCUIT BREAKER FALLBACK ACTIVATED] getProductById fallback triggered: {}", throwable.getMessage());
        return ProductDto.builder()
                .id(productId)
                .code("CIRCUIT_FALLBACK")
                .name("Sản phẩm tạm thời không thể truy xuất (Circuit OPEN)")
                .price(BigDecimal.ZERO)
                .stockQuantity(0)
                .description("Hệ thống phát hiện lỗi dây chuyền từ inventory-service và kích hoạt fallback.")
                .build();
    }

    private ProductDto mapToDto(Product product) {
        return ProductDto.builder()
                .id(product.getId())
                .code(product.getCode())
                .name(product.getName())
                .price(product.getPrice())
                .stockQuantity(product.getStockQuantity())
                .description(product.getDescription())
                .build();
    }
}
