package org.example.ptit_cntt1_it214_session18_mini.kafka.consumer;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.ptit_cntt1_it214_session18_mini.kafka.event.OrderSagaEvent;
import org.example.ptit_cntt1_it214_session18_mini.kafka.producer.ShopMartKafkaProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class ShopMartKafkaConsumer {

    private final ShopMartWebFluxEventStream webFluxEventStream;

    @PostConstruct
    public void initReactiveConsumer() {
        // Consumer Reactive WebFlux: Đăng ký lắng nghe sự kiện bất đồng bộ
        webFluxEventStream.getEventFlux().subscribe(event -> {
            log.info("[WEBFLUX REACTIVE CONSUMER] Asynchronously processed event: {} for Order #{}",
                    event.getEventType(), event.getOrderId());
        });
        log.info("[WEBFLUX REACTIVE CONSUMER] Initialized reactive event listener successfully.");
    }

    @KafkaListener(
            topics = ShopMartKafkaProducer.TOPIC_ORDER_EVENTS,
            groupId = "shopmart-saga-group",
            autoStartup = "${spring.kafka.consumer.auto-startup:false}"
    )
    public void onKafkaMessage(OrderSagaEvent event) {
        log.info("[KAFKA BROKER CONSUMER] Received message from Kafka broker: Event={}, Order={}",
                event.getEventType(), event.getOrderId());
    }
}
