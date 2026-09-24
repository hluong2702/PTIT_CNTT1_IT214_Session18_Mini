package org.example.ptit_cntt1_it214_session18_mini.order.controller;

import lombok.RequiredArgsConstructor;
import org.example.ptit_cntt1_it214_session18_mini.order.dto.OrderRequest;
import org.example.ptit_cntt1_it214_session18_mini.order.model.Order;
import org.example.ptit_cntt1_it214_session18_mini.order.service.InventoryClientService;
import org.example.ptit_cntt1_it214_session18_mini.order.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final InventoryClientService inventoryClientService;

    @PostMapping("/create")
    public ResponseEntity<Order> createOrder(@RequestBody OrderRequest request) {
        Order order = orderService.createOrder(request);
        return ResponseEntity.ok(order);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Order> getOrderById(@PathVariable Long id) {
        return ResponseEntity.ok(orderService.getOrderById(id));
    }

    @GetMapping
    public ResponseEntity<List<Order>> getAllOrders() {
        return ResponseEntity.ok(orderService.getAllOrders());
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
