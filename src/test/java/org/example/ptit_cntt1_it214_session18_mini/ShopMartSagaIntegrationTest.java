package org.example.ptit_cntt1_it214_session18_mini;

import org.example.ptit_cntt1_it214_session18_mini.inventory.model.Product;
import org.example.ptit_cntt1_it214_session18_mini.inventory.repository.ProductRepository;
import org.example.ptit_cntt1_it214_session18_mini.kafka.consumer.ShopMartWebFluxEventStream;
import org.example.ptit_cntt1_it214_session18_mini.kafka.event.OrderSagaEvent;
import org.example.ptit_cntt1_it214_session18_mini.order.dto.OrderRequest;
import org.example.ptit_cntt1_it214_session18_mini.order.model.Order;
import org.example.ptit_cntt1_it214_session18_mini.order.model.OrderStatus;
import org.example.ptit_cntt1_it214_session18_mini.order.repository.OrderRepository;
import org.example.ptit_cntt1_it214_session18_mini.payment.model.PaymentStatus;
import org.example.ptit_cntt1_it214_session18_mini.payment.model.PaymentTransaction;
import org.example.ptit_cntt1_it214_session18_mini.payment.repository.PaymentTransactionRepository;
import org.example.ptit_cntt1_it214_session18_mini.saga.model.SagaResult;
import org.example.ptit_cntt1_it214_session18_mini.saga.orchestrator.ShopMartSagaOrchestrator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class ShopMartSagaIntegrationTest {

    @Autowired
    private ShopMartSagaOrchestrator sagaOrchestrator;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private PaymentTransactionRepository paymentRepository;

    @Autowired
    private ShopMartWebFluxEventStream webFluxEventStream;

    private Product testProduct;

    @BeforeEach
    void setUp() {
        webFluxEventStream.clearHistory();

        // Chuẩn bị sản phẩm kiểm thử với 20 sản phẩm tồn kho
        Optional<Product> prodOpt = productRepository.findByCode("LAPTOP-XPS15");
        if (prodOpt.isPresent()) {
            testProduct = prodOpt.get();
            testProduct.setStockQuantity(20);
            productRepository.save(testProduct);
        } else {
            testProduct = productRepository.save(Product.builder()
                    .code("LAPTOP-XPS15")
                    .name("Laptop Dell XPS 15")
                    .price(new BigDecimal("35000000"))
                    .stockQuantity(20)
                    .build());
        }
    }

    @Test
    @DisplayName("CÂU 3 - TEST 1: Luồng Saga thành công (Order Created -> Inventory Deducted -> Payment Success -> Order CONFIRMED)")
    void testSagaOrderSuccessFlow() {
        // GIVEN: Đặt mua 2 chiếc Laptop Dell XPS 15, thanh toán bình thường
        int purchaseQty = 2;
        int initialStock = testProduct.getStockQuantity();

        OrderRequest request = OrderRequest.builder()
                .customerId(101L)
                .productId(testProduct.getId())
                .quantity(purchaseQty)
                .unitPrice(testProduct.getPrice())
                .simulatePaymentFailure(false)
                .simulateInventoryDown(false)
                .build();

        // WHEN: Kích hoạt Saga Orchestrator
        SagaResult result = sagaOrchestrator.executeOrderSaga(request);

        // THEN:
        assertNotNull(result);
        assertTrue(result.isSuccess(), "Saga phải kết thúc thành công");
        assertEquals(OrderStatus.CONFIRMED, result.getFinalStatus(), "Đơn hàng phải ở trạng thái CONFIRMED");

        // 1. Kiểm tra tồn kho đã bị trừ chính xác 2 chiếc (20 - 2 = 18)
        Product updatedProduct = productRepository.findById(testProduct.getId()).orElseThrow();
        assertEquals(initialStock - purchaseQty, updatedProduct.getStockQuantity(),
                "Tồn kho phải giảm chính xác bằng số lượng đặt");

        // 2. Kiểm tra Order trong Database có trạng thái CONFIRMED
        Order savedOrder = orderRepository.findById(result.getOrderId()).orElseThrow();
        assertEquals(OrderStatus.CONFIRMED, savedOrder.getStatus());
        assertEquals("SAGA_SUCCESS", savedOrder.getSagaStep());

        // 3. Kiểm tra giao dịch Payment thành công
        PaymentTransaction payment = paymentRepository.findByOrderId(result.getOrderId()).orElseThrow();
        assertEquals(PaymentStatus.SUCCESS, payment.getStatus());

        // 4. Kiểm tra chuỗi sự kiện Event-driven Saga phát ra WebFlux/Kafka
        List<OrderSagaEvent> events = webFluxEventStream.getEventHistory();
        assertTrue(events.stream().anyMatch(e -> e.getEventType() == OrderSagaEvent.EventType.ORDER_CREATED));
        assertTrue(events.stream().anyMatch(e -> e.getEventType() == OrderSagaEvent.EventType.STOCK_DEDUCTED));
        assertTrue(events.stream().anyMatch(e -> e.getEventType() == OrderSagaEvent.EventType.PAYMENT_PROCESSED));
        assertTrue(events.stream().anyMatch(e -> e.getEventType() == OrderSagaEvent.EventType.ORDER_CONFIRMED));
    }

    @Test
    @DisplayName("CÂU 3 - TEST 2: Luồng Saga Rollback (Payment Thất Bại -> Bù Trừ Hoàn Trả Kho -> Order CANCELLED -> Dữ Liệu Nhất Quán)")
    void testSagaOrderRollbackWhenPaymentFails() {
        // GIVEN: Đặt mua 3 chiếc Laptop, cố tình giả lập lỗi thanh toán (simulatePaymentFailure = true)
        int purchaseQty = 3;
        int initialStock = testProduct.getStockQuantity();

        OrderRequest request = OrderRequest.builder()
                .customerId(102L)
                .productId(testProduct.getId())
                .quantity(purchaseQty)
                .unitPrice(testProduct.getPrice())
                .simulatePaymentFailure(true) // Cố tình gây lỗi thanh toán để test Rollback
                .simulateInventoryDown(false)
                .build();

        // WHEN: Kích hoạt Saga Orchestrator
        SagaResult result = sagaOrchestrator.executeOrderSaga(request);

        // THEN:
        assertNotNull(result);
        assertFalse(result.isSuccess(), "Saga phải thất bại do lỗi thanh toán");
        assertEquals(OrderStatus.CANCELLED, result.getFinalStatus(), "Đơn hàng phải bị chuyển sang CANCELLED");

        // 1. MINH CHỨNG ROLLBACK BÙ TRỪ KHO:
        // Ban đầu trừ 3 chiếc, sau đó bước thanh toán fail -> kích hoạt Compensating Transaction hoàn trả lại 3 chiếc!
        // Tồn kho cuối cùng phải bằng đúng số lượng ban đầu (initialStock)
        Product finalProduct = productRepository.findById(testProduct.getId()).orElseThrow();
        assertEquals(initialStock, finalProduct.getStockQuantity(),
                "Tồn kho phải được khôi phục 100% về ban đầu sau khi kích hoạt Compensating Transaction!");

        // 2. Kiểm tra Order trong Database đã bị CANCELLED
        Order savedOrder = orderRepository.findById(result.getOrderId()).orElseThrow();
        assertEquals(OrderStatus.CANCELLED, savedOrder.getStatus());
        assertEquals("SAGA_ROLLED_BACK", savedOrder.getSagaStep());
        assertTrue(savedOrder.getFailureReason().contains("Payment"));

        // 3. Kiểm tra giao dịch Payment ghi nhận FAILED
        PaymentTransaction payment = paymentRepository.findByOrderId(result.getOrderId()).orElseThrow();
        assertEquals(PaymentStatus.FAILED, payment.getStatus());

        // 4. Kiểm tra sự kiện Rollback phát ra: PAYMENT_FAILED, STOCK_COMPENSATED, ORDER_CANCELLED
        List<OrderSagaEvent> events = webFluxEventStream.getEventHistory();
        assertTrue(events.stream().anyMatch(e -> e.getEventType() == OrderSagaEvent.EventType.PAYMENT_FAILED));
        assertTrue(events.stream().anyMatch(e -> e.getEventType() == OrderSagaEvent.EventType.STOCK_COMPENSATED));
        assertTrue(events.stream().anyMatch(e -> e.getEventType() == OrderSagaEvent.EventType.ORDER_CANCELLED));
    }
}
