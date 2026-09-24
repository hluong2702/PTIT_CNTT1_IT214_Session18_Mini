package org.example.ptit_cntt1_it214_session18_mini.order.controller;

import lombok.RequiredArgsConstructor;
import org.example.ptit_cntt1_it214_session18_mini.kafka.consumer.ShopMartWebFluxEventStream;
import org.example.ptit_cntt1_it214_session18_mini.kafka.event.OrderSagaEvent;
import org.example.ptit_cntt1_it214_session18_mini.order.dto.OrderRequest;
import org.example.ptit_cntt1_it214_session18_mini.order.model.Order;
import org.example.ptit_cntt1_it214_session18_mini.order.repository.OrderRepository;
import org.example.ptit_cntt1_it214_session18_mini.order.service.InventoryClientService;
import org.example.ptit_cntt1_it214_session18_mini.saga.model.SagaResult;
import org.example.ptit_cntt1_it214_session18_mini.saga.orchestrator.ShopMartSagaOrchestrator;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/order")
@RequiredArgsConstructor
public class OrderController {

    private final ShopMartSagaOrchestrator sagaOrchestrator;
    private final OrderRepository orderRepository;
    private final ShopMartWebFluxEventStream webFluxEventStream;
    private final InventoryClientService inventoryClientService;

    @PostMapping("/create")
    public ResponseEntity<SagaResult> createOrder(@RequestBody OrderRequest request) {
        SagaResult result = sagaOrchestrator.executeOrderSaga(request);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Order> getOrderById(@PathVariable Long id) {
        return orderRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<Order>> getAllOrders() {
        return ResponseEntity.ok(orderRepository.findAll());
    }

    /**
     * WebFlux Reactive Event Stream Endpoint:
     * Phát luồng sự kiện Server-Sent Events (SSE) theo chuẩn Reactive Streams
     */
    @GetMapping(value = "/events/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<OrderSagaEvent> streamOrderEvents() {
        return webFluxEventStream.getEventFlux();
    }

    @GetMapping("/events/history")
    public ResponseEntity<List<OrderSagaEvent>> getEventHistory() {
        return ResponseEntity.ok(webFluxEventStream.getEventHistory());
    }

    @PostMapping("/circuit-breaker/simulate-down")
    public ResponseEntity<Map<String, Object>> toggleCircuitBreakerDown(@RequestParam boolean down) {
        inventoryClientService.setSimulateInventoryDown(down);
        return ResponseEntity.ok(Map.of(
                "simulateInventoryDown", down,
                "message", down ? "Simulating inventory-service DOWN (Calls will fail)" : "Inventory-service back UP"
        ));
    }
}
