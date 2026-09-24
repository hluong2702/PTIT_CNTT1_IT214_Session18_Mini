package org.example.ptit_cntt1_it214_session18_mini;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.example.ptit_cntt1_it214_session18_mini.inventory.model.Product;
import org.example.ptit_cntt1_it214_session18_mini.inventory.repository.ProductRepository;
import org.example.ptit_cntt1_it214_session18_mini.order.feign.ProductDto;
import org.example.ptit_cntt1_it214_session18_mini.order.service.InventoryClientService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class CircuitBreakerResilienceTest {

    @Autowired
    private InventoryClientService inventoryClientService;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    private Product sampleProduct;

    @BeforeEach
    void setUp() {
        inventoryClientService.setSimulateInventoryDown(false);
        sampleProduct = productRepository.findByCode("IPHONE-16PM")
                .orElseGet(() -> productRepository.save(Product.builder()
                        .code("IPHONE-16PM")
                        .name("iPhone 16 Pro Max")
                        .price(new BigDecimal("32990000"))
                        .stockQuantity(15)
                        .build()));
    }

    @Test
    @DisplayName("CÂU 2 - TEST 1: Khi Inventory Service bình thường (Circuit CLOSED) -> Gọi trực tiếp thành công")
    void testNormalCallWhenServiceHealthy() {
        ProductDto dto = inventoryClientService.getProductById(sampleProduct.getId());
        assertNotNull(dto);
        assertEquals(sampleProduct.getName(), dto.getName());

        CircuitBreaker cb = circuitBreakerRegistry.circuitBreaker("inventoryService");
        assertNotNull(cb);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState(), "Trạng thái ban đầu phải là CLOSED");
    }

    @Test
    @DisplayName("CÂU 2 - TEST 2: Khi Inventory Service lỗi/ngừng (Fallback Activated) -> Kích hoạt Fallback an toàn")
    void testFallbackActivatedWhenInventoryDown() {
        // GIVEN: Giả lập Inventory Service bị sập / timeout
        inventoryClientService.setSimulateInventoryDown(true);

        // WHEN: Gọi getProductById
        ProductDto fallbackResult = inventoryClientService.getProductById(sampleProduct.getId());

        // THEN: Fallback method trả về dữ liệu an toàn, không ném 500 ra ngoài
        assertNotNull(fallbackResult);
        assertEquals("CIRCUIT_FALLBACK", fallbackResult.getCode());
        assertTrue(fallbackResult.getName().contains("Circuit OPEN") || fallbackResult.getName().contains("Fallback"));

        // Phục hồi lại trạng thái
        inventoryClientService.setSimulateInventoryDown(false);
    }
}
