package org.example.ptit_cntt1_it214_session18_mini.inventory.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.ptit_cntt1_it214_session18_mini.inventory.exception.InsufficientStockException;
import org.example.ptit_cntt1_it214_session18_mini.inventory.model.Product;
import org.example.ptit_cntt1_it214_session18_mini.inventory.repository.ProductRepository;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.NoSuchElementException;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    private final ProductRepository productRepository;

    /**
     * Cache-Aside:
     * - Khi gọi lần đầu (Cache Miss): truy vấn từ Database, ghi log [DB QUERY], sau đó lưu vào Redis.
     * - Khi gọi các lần tiếp theo (Cache Hit): lấy trực tiếp từ Redis mà KHÔNG chạy vào hàm này (không có log [DB QUERY]).
     */
    @Cacheable(value = "products", key = "#id")
    @Transactional(readOnly = true)
    public Product getProductById(Long id) {
        log.info("[DB QUERY] Accessing Database to fetch Product id: {}", id);
        return productRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Product not found with id: " + id));
    }

    @Transactional(readOnly = true)
    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }

    /**
     * Cập nhật cache trực tiếp sau khi lưu hoặc update sản phẩm
     */
    @CachePut(value = "products", key = "#result.id")
    @Transactional
    public Product saveOrUpdateProduct(Product product) {
        log.info("[INVENTORY] Saving/Updating product: {} (ID: {})", product.getName(), product.getId());
        return productRepository.save(product);
    }

    /**
     * Trừ tồn kho sản phẩm khi đặt hàng:
     * - Xóa cache (@CacheEvict) để tránh dirty data.
     * - Trừ trực tiếp trong DB.
     */
    @CacheEvict(value = "products", key = "#productId")
    @Transactional
    public Product deductStock(Long productId, Integer quantity) {
        log.info("[INVENTORY] Attempting to deduct stock: Product ID={}, Quantity={}", productId, quantity);
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new NoSuchElementException("Product not found with id: " + productId));

        if (product.getStockQuantity() < quantity) {
            log.error("[INVENTORY FAILED] Insufficient stock for product '{}'. Current: {}, Requested: {}",
                    product.getName(), product.getStockQuantity(), quantity);
            throw new InsufficientStockException("Insufficient stock for product " + product.getName()
                    + ". Available: " + product.getStockQuantity() + ", Requested: " + quantity);
        }

        product.setStockQuantity(product.getStockQuantity() - quantity);
        Product updated = productRepository.save(product);
        log.info("[INVENTORY SUCCESS] Stock deducted successfully. Product ID={}, Remaining Stock={}",
                productId, updated.getStockQuantity());
        return updated;
    }

    /**
     * Bù trừ tồn kho (Compensating Transaction) trong Saga:
     * - Khi thanh toán thất bại, hệ thống gọi hàm này để hoàn trả lại số lượng đã trừ vào kho.
     * - Xóa cache (@CacheEvict) để cập nhật giá trị mới nhất.
     */
    @CacheEvict(value = "products", key = "#productId")
    @Transactional
    public Product compensateStock(Long productId, Integer quantity) {
        log.warn("[SAGA COMPENSATION] Compensating inventory: Restoring {} units to Product ID={}",
                quantity, productId);
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new NoSuchElementException("Product not found with id: " + productId));

        product.setStockQuantity(product.getStockQuantity() + quantity);
        Product restored = productRepository.save(product);
        log.info("[SAGA COMPENSATION COMPLETED] Stock restored successfully. Product ID={}, Restored Stock={}",
                productId, restored.getStockQuantity());
        return restored;
    }

    /**
     * Xóa cache thủ công
     */
    @CacheEvict(value = "products", key = "#id")
    public void evictProductCache(Long id) {
        log.info("[CACHE EVICT] Evicting product cache for key: {}", id);
    }
}
