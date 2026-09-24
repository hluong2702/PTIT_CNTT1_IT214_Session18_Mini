package org.example.ptit_cntt1_it214_session18_mini.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.example.ptit_cntt1_it214_session18_mini.kafka.producer.ShopMartKafkaProducer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Slf4j
public class KafkaTopicConfig {

    @Bean
    @ConditionalOnProperty(name = "spring.kafka.bootstrap-servers")
    public NewTopic orderEventsTopic() {
        log.info("[KAFKA CONFIG] Creating NewTopic '{}' with 3 partitions and 1 replica.",
                ShopMartKafkaProducer.TOPIC_ORDER_EVENTS);
        return TopicBuilder.name(ShopMartKafkaProducer.TOPIC_ORDER_EVENTS)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
