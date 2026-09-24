package org.example.ptit_cntt1_it214_session18_mini;

import org.example.ptit_cntt1_it214_session18_mini.inventory.model.Product;
import org.example.ptit_cntt1_it214_session18_mini.inventory.repository.ProductRepository;
import org.example.ptit_cntt1_it214_session18_mini.inventory.service.InventoryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class RedisCacheAsideIntegrationTest {

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private CacheManager cacheManager;

    private Product testProduct;

    @BeforeEach
    void setUp() {
        testProduct = productRepository.findByCode("SONY-WH1000XM5")
                .orElseGet(() -> productRepository.save(Product.builder()
                        .code("SONY-WH1000XM5")
                        .name("Tai nghe Sony WH-1000XM5")
                        .price(new BigDecimal("6990000"))
                        .stockQuantity(50)
                        .build()));

        // Xóa cache trước khi test
        Cache cache = cacheManager.getCache("products");
        if (cache != null) {
            cache.evict(testProduct.getId());
        }
    }

    @Test
    @DisplayName("CÂU 4 - TEST 1: Cache-Aside (@Cacheable) - Lần 1 nạp từ DB và lưu cache, lần 2 lấy từ Cache")
    void testCacheAsideHitAndMiss() {
        Cache cache = cacheManager.getCache("products");
        assertNotNull(cache);

        // LẦN 1: Cache Miss -> Đọc DB và ghi vào Cache
        Product call1 = inventoryService.getProductById(testProduct.getId());
        assertNotNull(call1);

        // Kiểm tra trong Cache đã có dữ liệu chưa
        Cache.ValueWrapper cachedValue = cache.get(testProduct.getId());
        if (cachedValue != null) {
            Product cachedProd = (Product) cachedValue.get();
            assertEquals(call1.getId(), cachedProd.getId());
        }

        // LẦN 2: Cache Hit -> Lấy từ Cache
        Product call2 = inventoryService.getProductById(testProduct.getId());
        assertNotNull(call2);
        assertEquals(call1.getName(), call2.getName());
    }

    @Test
    @DisplayName("CÂU 4 - TEST 2: Cache Evict (@CacheEvict) - Khi trừ kho, xóa cache để đảm bảo nhất quán")
    void testCacheEvictOnStockChange() {
        Cache cache = cacheManager.getCache("products");
        assertNotNull(cache);

        // Nạp vào cache
        inventoryService.getProductById(testProduct.getId());

        // Thực hiện trừ tồn kho -> kích hoạt @CacheEvict
        inventoryService.deductStock(testProduct.getId(), 2);

        // Đọc lại -> Lấy dữ liệu mới nhất từ DB
        Product updated = inventoryService.getProductById(testProduct.getId());
        assertEquals(48, updated.getStockQuantity());
    }
}
