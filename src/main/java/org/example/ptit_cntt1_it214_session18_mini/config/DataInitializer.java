package org.example.ptit_cntt1_it214_session18_mini.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.ptit_cntt1_it214_session18_mini.inventory.model.Product;
import org.example.ptit_cntt1_it214_session18_mini.inventory.repository.ProductRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class DataInitializer implements CommandLineRunner {

    private final ProductRepository productRepository;

    @Override
    public void run(String... args) {
        if (productRepository.count() == 0) {
            log.info("[DATA INITIALIZER] Seeding initial ShopMart products into database...");

            List<Product> initialProducts = List.of(
                    Product.builder()
                            .code("LAPTOP-XPS15")
                            .name("Laptop Dell XPS 15 (i7, 32GB RAM)")
                            .price(new BigDecimal("35000000"))
                            .stockQuantity(20)
                            .description("Laptop cao cấp đồ họa và lập trình")
                            .build(),
                    Product.builder()
                            .code("IPHONE-16PM")
                            .name("iPhone 16 Pro Max 256GB")
                            .price(new BigDecimal("32990000"))
                            .stockQuantity(15)
                            .description("Flagship mới nhất của Apple")
                            .build(),
                    Product.builder()
                            .code("SONY-WH1000XM5")
                            .name("Tai nghe chống ồn Sony WH-1000XM5")
                            .price(new BigDecimal("6990000"))
                            .stockQuantity(50)
                            .description("Tai nghe chống ồn hàng đầu thị trường")
                            .build(),
                    Product.builder()
                            .code("KEYBOARD-MX")
                            .name("Bàn phím cơ Logitech MX Mechanical")
                            .price(new BigDecimal("3490000"))
                            .stockQuantity(40)
                            .description("Bàn phím cơ gõ êm cho lập trình viên")
                            .build()
            );

            productRepository.saveAll(initialProducts);
            log.info("[DATA INITIALIZER] Successfully seeded {} products.", initialProducts.size());
        }
    }
}
