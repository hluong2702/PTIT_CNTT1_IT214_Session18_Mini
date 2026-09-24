package org.example.ptit_cntt1_it214_session18_mini.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.example.ptit_cntt1_it214_session18_mini.kafka.consumer.ShopMartWebFluxEventStream;
import org.example.ptit_cntt1_it214_session18_mini.kafka.event.OrderSagaEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
@Slf4j
public class ShopMartKafkaProducer {

    public static final String TOPIC_ORDER_EVENTS = "order-saga-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ShopMartWebFluxEventStream webFluxEventStream;

    public ShopMartKafkaProducer(
            @Autowired(required = false) KafkaTemplate<String, Object> kafkaTemplate,
            ShopMartWebFluxEventStream webFluxEventStream) {
        this.kafkaTemplate = kafkaTemplate;
        this.webFluxEventStream = webFluxEventStream;
    }

    public void publishEvent(OrderSagaEvent event) {
        if (event.getEventId() == null) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getTimestamp() == null) {
            event.setTimestamp(LocalDateTime.now());
        }

        log.info("[KAFKA PRODUCER] Publishing event {} to topic '{}' for Order ID={}: {}",
                event.getEventType(), TOPIC_ORDER_EVENTS, event.getOrderId(), event.getDetails());

        // Gửi đến Kafka broker nếu broker đang kết nối
        if (kafkaTemplate != null) {
            try {
                kafkaTemplate.send(TOPIC_ORDER_EVENTS, String.valueOf(event.getOrderId()), event);
                log.info("[KAFKA PRODUCER] Event successfully dispatched to Kafka broker");
            } catch (Exception ex) {
                log.warn("[KAFKA PRODUCER] Kafka broker unreachable, event retained in reactive memory bus: {}", ex.getMessage());
            }
        }

        // Luôn phát tới Reactive WebFlux Event Stream để phục vụ SSE và reactive consumer
        webFluxEventStream.emitEvent(event);
    }
}
