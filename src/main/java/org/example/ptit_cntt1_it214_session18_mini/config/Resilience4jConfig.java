package org.example.ptit_cntt1_it214_session18_mini.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Cấu hình Resilience4j Circuit Breaker cho giao tiếp giữa Order-Service và Inventory-Service:
 * 3 trạng thái hoạt động:
 * 1. CLOSED: Hoạt động bình thường. Nếu tỷ lệ lỗi >= 50% trong 10 cuộc gọi -> chuyển sang OPEN.
 * 2. OPEN: Chặn tất cả request, kích hoạt Fallback ngay lập tức để chống Cascading Failure. Chờ 5 giây -> chuyển sang HALF-OPEN.
 * 3. HALF-OPEN: Cho phép 2 request thăm dò đi qua. Nếu thành công -> chuyển về CLOSED; nếu thất bại -> quay lại OPEN.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class Resilience4jConfig {

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50.0f)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(4)
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .permittedNumberOfCallsInHalfOpenState(2)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        CircuitBreaker cb = registry.circuitBreaker("inventoryService", config);

        // Ghi log chuyển đổi trạng thái Circuit Breaker
        cb.getEventPublisher()
                .onStateTransition(event -> log.warn("[CIRCUIT BREAKER STATE CHANGE] State transitioned: {} -> {}",
                        event.getStateTransition().getFromState(), event.getStateTransition().getToState()))
                .onCallNotPermitted(event -> log.warn("[CIRCUIT BREAKER OPEN] Call blocked by Circuit Breaker: {}", event.getCircuitBreakerName()))
                .onError(event -> log.error("[CIRCUIT BREAKER ERROR] Recorded failure in Circuit Breaker: {}", event.getThrowable().getMessage()))
                .onSuccess(event -> log.debug("[CIRCUIT BREAKER SUCCESS] Call succeeded"));

        return registry;
    }
}
