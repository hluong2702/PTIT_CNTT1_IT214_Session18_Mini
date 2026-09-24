package org.example.ptit_cntt1_it214_session18_mini.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.example.ptit_cntt1_it214_session18_mini.kafka.event.OrderSagaEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * WebFlux Reactive Event Stream:
 * Triển khai tư duy Reactive / Async (Loose-coupling) theo yêu cầu nâng cao Câu 3.
 * Nhận các sự kiện từ Kafka và phát ra luồng Flux bất đồng bộ (Server-Sent Events / Reactive Stream).
 */
@Component
@Slf4j
public class ShopMartWebFluxEventStream {

    private final Sinks.Many<OrderSagaEvent> eventSink = Sinks.many().multicast().onBackpressureBuffer();
    private final List<OrderSagaEvent> eventHistory = Collections.synchronizedList(new ArrayList<>());

    public void emitEvent(OrderSagaEvent event) {
        log.info("[WEBFLUX REACTIVE STREAM] Emitting event: {} for Order #{}",
                event.getEventType(), event.getOrderId());
        eventHistory.add(event);
        eventSink.tryEmitNext(event);
    }

    public Flux<OrderSagaEvent> getEventFlux() {
        return eventSink.asFlux();
    }

    public List<OrderSagaEvent> getEventHistory() {
        return new ArrayList<>(eventHistory);
    }

    public void clearHistory() {
        eventHistory.clear();
    }
}
