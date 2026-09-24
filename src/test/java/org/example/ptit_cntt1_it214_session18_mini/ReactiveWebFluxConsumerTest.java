package org.example.ptit_cntt1_it214_session18_mini;

import org.example.ptit_cntt1_it214_session18_mini.kafka.consumer.ShopMartWebFluxEventStream;
import org.example.ptit_cntt1_it214_session18_mini.kafka.event.OrderSagaEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@SpringBootTest
class ReactiveWebFluxConsumerTest {

    @Autowired
    private ShopMartWebFluxEventStream webFluxEventStream;

    @Test
    @DisplayName("CÂU 3 (BONUS) - TEST: Xử lý sự kiện Saga phân tán bằng Reactive WebFlux (Non-blocking Streams)")
    void testReactiveWebFluxConsumer() {
        Flux<OrderSagaEvent> eventFlux = webFluxEventStream.getEventFlux();

        OrderSagaEvent sampleEvent = OrderSagaEvent.builder()
                .eventId("TEST-EVT-001")
                .eventType(OrderSagaEvent.EventType.STOCK_COMPENSATED)
                .orderId(999L)
                .customerId(888L)
                .productId(1L)
                .quantity(2)
                .amount(new BigDecimal("70000000"))
                .status("COMPENSATED")
                .details("Reactive event test")
                .timestamp(LocalDateTime.now())
                .build();

        // Sử dụng StepVerifier để kiểm tra tính toàn vẹn của luồng Reactive Stream bất đồng bộ
        StepVerifier.create(eventFlux)
                .then(() -> webFluxEventStream.emitEvent(sampleEvent))
                .expectNextMatches(evt -> evt.getOrderId().equals(999L)
                        && evt.getEventType() == OrderSagaEvent.EventType.STOCK_COMPENSATED)
                .thenCancel()
                .verify();
    }
}
