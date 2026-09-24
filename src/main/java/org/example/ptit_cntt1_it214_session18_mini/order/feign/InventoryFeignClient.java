package org.example.ptit_cntt1_it214_session18_mini.order.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "inventory-service", fallback = InventoryFeignClientFallback.class)
public interface InventoryFeignClient {

    @GetMapping("/api/inventory/products/{id}")
    ProductDto getProductById(@PathVariable("id") Long id);

    @PostMapping("/api/inventory/deduct")
    ProductDto deductStock(@RequestParam("productId") Long productId, @RequestParam("quantity") Integer quantity);

    @PostMapping("/api/inventory/compensate")
    ProductDto compensateStock(@RequestParam("productId") Long productId, @RequestParam("quantity") Integer quantity);
}
