package org.example.ptit_cntt1_it214_session18_mini.inventory.controller;

import lombok.RequiredArgsConstructor;
import org.example.ptit_cntt1_it214_session18_mini.inventory.model.Product;
import org.example.ptit_cntt1_it214_session18_mini.inventory.service.InventoryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/inventory")
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryService inventoryService;

    @GetMapping("/products/{id}")
    public ResponseEntity<Product> getProductById(@PathVariable Long id) {
        return ResponseEntity.ok(inventoryService.getProductById(id));
    }

    @GetMapping("/products")
    public ResponseEntity<List<Product>> getAllProducts() {
        return ResponseEntity.ok(inventoryService.getAllProducts());
    }

    @PostMapping("/deduct")
    public ResponseEntity<Product> deductStock(
            @RequestParam Long productId,
            @RequestParam Integer quantity) {
        return ResponseEntity.ok(inventoryService.deductStock(productId, quantity));
    }

    @PostMapping("/compensate")
    public ResponseEntity<Product> compensateStock(
            @RequestParam Long productId,
            @RequestParam Integer quantity) {
        return ResponseEntity.ok(inventoryService.compensateStock(productId, quantity));
    }

    @DeleteMapping("/cache/{id}")
    public ResponseEntity<Map<String, String>> evictCache(@PathVariable Long id) {
        inventoryService.evictProductCache(id);
        return ResponseEntity.ok(Map.of("message", "Cache evicted for product id: " + id));
    }
}
