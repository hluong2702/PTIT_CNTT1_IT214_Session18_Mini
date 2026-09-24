package org.example.ptit_cntt1_it214_session18_mini.saga.orchestrator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.ptit_cntt1_it214_session18_mini.kafka.event.OrderSagaEvent;
import org.example.ptit_cntt1_it214_session18_mini.kafka.producer.ShopMartKafkaProducer;
import org.example.ptit_cntt1_it214_session18_mini.order.dto.OrderRequest;
import org.example.ptit_cntt1_it214_session18_mini.order.feign.ProductDto;
import org.example.ptit_cntt1_it214_session18_mini.order.model.Order;
import org.example.ptit_cntt1_it214_session18_mini.order.model.OrderStatus;
import org.example.ptit_cntt1_it214_session18_mini.order.repository.OrderRepository;
import org.example.ptit_cntt1_it214_session18_mini.order.service.InventoryClientService;
import org.example.ptit_cntt1_it214_session18_mini.payment.service.PaymentService;
import org.example.ptit_cntt1_it214_session18_mini.saga.model.SagaResult;
import org.example.ptit_cntt1_it214_session18_mini.saga.model.SagaStep;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * ShopMart Saga Orchestrator:
 * Điều phối giao dịch phân tán xuyên suốt các Microservices (Order, Inventory, Payment).
 * Đảm bảo tính nhất quán cuối cùng (Eventual Consistency) bằng cơ chế Compensating Transactions.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ShopMartSagaOrchestrator {

    private final OrderRepository orderRepository;
    private final InventoryClientService inventoryClientService;
    private final PaymentService paymentService;
    private final ShopMartKafkaProducer kafkaProducer;

    public SagaResult executeOrderSaga(OrderRequest request) {
        List<String> logs = new ArrayList<>();
        log.info("================================================================================");
        log.info("[SAGA ORCHESTRATOR STARTED] Initiating distributed order transaction for Customer #{}", request.getCustomerId());
        logs.add("Saga started for customer: " + request.getCustomerId());

        // Lấy thông tin giá sản phẩm nếu chưa có
        BigDecimal unitPrice = request.getUnitPrice();
        if (unitPrice == null) {
            ProductDto prod = inventoryClientService.getProductById(request.getProductId());
            unitPrice = prod.getPrice();
        }
        BigDecimal totalPrice = unitPrice.multiply(BigDecimal.valueOf(request.getQuantity()));

        // ================= STEP 1: CREATE ORDER (PENDING) =================
        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .totalPrice(totalPrice)
                .status(OrderStatus.PENDING)
                .sagaStep(SagaStep.ORDER_INITIALIZED.name())
                .build();
        order = orderRepository.save(order);
        log.info("[SAGA STEP 1] Created Order #{} in PENDING status. Total: {}", order.getId(), totalPrice);
        logs.add(String.format("Step 1 [ORDER]: Order #%d created in PENDING status (Amount: %s)", order.getId(), totalPrice));

        kafkaProducer.publishEvent(OrderSagaEvent.builder()
                .eventType(OrderSagaEvent.EventType.ORDER_CREATED)
                .orderId(order.getId())
                .customerId(order.getCustomerId())
                .productId(order.getProductId())
                .quantity(order.getQuantity())
                .amount(totalPrice)
                .status("PENDING")
                .details("Order created in PENDING state, awaiting inventory reservation")
                .build());

        // ================= STEP 2: DEDUCT INVENTORY (FEIGN + RESILIENCE4J) =================
        boolean inventoryReserved = false;
        try {
            inventoryClientService.setSimulateInventoryDown(request.isSimulateInventoryDown());
            ProductDto updatedProduct = inventoryClientService.deductStock(order.getProductId(), order.getQuantity());
            inventoryReserved = true;

            order.setSagaStep(SagaStep.INVENTORY_DEDUCTED.name());
            orderRepository.save(order);
            log.info("[SAGA STEP 2 SUCCESS] Deducted {} units of Product #{}. Remaining: {}",
                    order.getQuantity(), order.getProductId(), updatedProduct.getStockQuantity());
            logs.add(String.format("Step 2 [INVENTORY]: Deducted %d units of Product #%d (Remaining in stock: %d)",
                    order.getQuantity(), order.getProductId(), updatedProduct.getStockQuantity()));

            kafkaProducer.publishEvent(OrderSagaEvent.builder()
                    .eventType(OrderSagaEvent.EventType.STOCK_DEDUCTED)
                    .orderId(order.getId())
                    .customerId(order.getCustomerId())
                    .productId(order.getProductId())
                    .quantity(order.getQuantity())
                    .amount(totalPrice)
                    .status("INVENTORY_RESERVED")
                    .details("Stock successfully deducted from inventory-service")
                    .build());
        } catch (Exception ex) {
            log.error("[SAGA STEP 2 FAILED] Failed to deduct stock: {}", ex.getMessage());
            logs.add(String.format("Step 2 [INVENTORY FAILED]: %s", ex.getMessage()));

            order.setStatus(OrderStatus.CANCELLED);
            order.setSagaStep(SagaStep.SAGA_ROLLED_BACK.name());
            order.setFailureReason("Inventory deduction failed: " + ex.getMessage());
            orderRepository.save(order);

            kafkaProducer.publishEvent(OrderSagaEvent.builder()
                    .eventType(OrderSagaEvent.EventType.ORDER_CANCELLED)
                    .orderId(order.getId())
                    .status("CANCELLED")
                    .details("Order cancelled due to inventory error: " + ex.getMessage())
                    .build());

            return SagaResult.builder()
                    .orderId(order.getId())
                    .finalStatus(OrderStatus.CANCELLED)
                    .success(false)
                    .message("Saga aborted at Inventory step: " + ex.getMessage())
                    .amount(totalPrice)
                    .executionLogs(logs)
                    .build();
        }

        // ================= STEP 3: PAYMENT PROCESSING =================
        try {
            log.info("[SAGA STEP 3] Requesting payment of {} for Order #{}", totalPrice, order.getId());
            logs.add(String.format("Step 3 [PAYMENT]: Requesting payment of %s", totalPrice));

            paymentService.processPayment(order.getId(), order.getCustomerId(), totalPrice, request.isSimulatePaymentFailure());

            order.setSagaStep(SagaStep.PAYMENT_SUCCESS.name());
            log.info("[SAGA STEP 3 SUCCESS] Payment processed successfully for Order #{}", order.getId());
            logs.add(String.format("Step 3 [PAYMENT SUCCESS]: Charge of %s confirmed", totalPrice));

            kafkaProducer.publishEvent(OrderSagaEvent.builder()
                    .eventType(OrderSagaEvent.EventType.PAYMENT_PROCESSED)
                    .orderId(order.getId())
                    .amount(totalPrice)
                    .status("PAYMENT_SUCCESS")
                    .details("Payment successfully processed by payment-service")
                    .build());

            // ================= STEP 4: SAGA CONFIRMATION (HAPPY PATH) =================
            order.setStatus(OrderStatus.CONFIRMED);
            order.setSagaStep(SagaStep.SAGA_SUCCESS.name());
            orderRepository.save(order);
            log.info("[SAGA COMPLETED - SUCCESS] Order #{} confirmed! Distributed transaction finished successfully.", order.getId());
            logs.add(String.format("Step 4 [CONFIRMED]: Order #%d CONFIRMED! Saga completed successfully.", order.getId()));

            kafkaProducer.publishEvent(OrderSagaEvent.builder()
                    .eventType(OrderSagaEvent.EventType.ORDER_CONFIRMED)
                    .orderId(order.getId())
                    .status("CONFIRMED")
                    .details("Order CONFIRMED. Saga transaction committed across all microservices.")
                    .build());

            ProductDto currentProd = inventoryClientService.getProductById(order.getProductId());

            return SagaResult.builder()
                    .orderId(order.getId())
                    .finalStatus(OrderStatus.CONFIRMED)
                    .success(true)
                    .message("Order completed and confirmed successfully via Saga!")
                    .amount(totalPrice)
                    .remainingStock(currentProd.getStockQuantity())
                    .executionLogs(logs)
                    .build();

        } catch (Exception paymentEx) {
            // ================= STEP 5: COMPENSATING TRANSACTIONS (ROLLBACK PATH) =================
            log.warn("--------------------------------------------------------------------------------");
            log.warn("[SAGA PAYMENT FAILED] Payment processing failed: {}. Triggering COMPENSATING TRANSACTIONS!",
                    paymentEx.getMessage());
            logs.add(String.format("Step 3 [PAYMENT FAILED]: %s. Initiating compensating transactions (Rollback)!", paymentEx.getMessage()));

            kafkaProducer.publishEvent(OrderSagaEvent.builder()
                    .eventType(OrderSagaEvent.EventType.PAYMENT_FAILED)
                    .orderId(order.getId())
                    .status("PAYMENT_FAILED")
                    .details("Payment failed: " + paymentEx.getMessage())
                    .build());

            // Bù trừ kho (Compensating Inventory): Hoàn lại số lượng đã trừ
            if (inventoryReserved) {
                log.warn("[SAGA COMPENSATING STEP 1] Restoring {} units of Product #{} back to inventory...",
                        order.getQuantity(), order.getProductId());
                ProductDto restored = inventoryClientService.compensateStock(order.getProductId(), order.getQuantity());
                logs.add(String.format("Compensating Step [INVENTORY ROLLBACK]: Restored %d units to Product #%d. Restored stock: %d",
                        order.getQuantity(), order.getProductId(), restored.getStockQuantity()));

                kafkaProducer.publishEvent(OrderSagaEvent.builder()
                        .eventType(OrderSagaEvent.EventType.STOCK_COMPENSATED)
                        .orderId(order.getId())
                        .productId(order.getProductId())
                        .quantity(order.getQuantity())
                        .status("STOCK_COMPENSATED")
                        .details(String.format("Compensated %d units back to inventory. Stock restored.", order.getQuantity()))
                        .build());
            }

            // Hủy đơn hàng (Cancel Order)
            order.setStatus(OrderStatus.CANCELLED);
            order.setSagaStep(SagaStep.SAGA_ROLLED_BACK.name());
            order.setFailureReason("Rollback caused by Payment failure: " + paymentEx.getMessage());
            orderRepository.save(order);
            log.warn("[SAGA ROLLBACK COMPLETED] Order #{} transitioned to CANCELLED. Data consistency preserved 100%!", order.getId());
            logs.add(String.format("Saga Rollback [ORDER CANCELLED]: Order #%d marked as CANCELLED. All microservices consistent!", order.getId()));

            kafkaProducer.publishEvent(OrderSagaEvent.builder()
                    .eventType(OrderSagaEvent.EventType.ORDER_CANCELLED)
                    .orderId(order.getId())
                    .status("CANCELLED")
                    .details("Order rolled back to CANCELLED. Data integrity intact.")
                    .build());

            ProductDto currentProd = inventoryClientService.getProductById(order.getProductId());

            return SagaResult.builder()
                    .orderId(order.getId())
                    .finalStatus(OrderStatus.CANCELLED)
                    .success(false)
                    .message("Order cancelled due to payment failure. Inventory was safely restored via Compensating Transaction!")
                    .amount(totalPrice)
                    .remainingStock(currentProd.getStockQuantity())
                    .executionLogs(logs)
                    .build();
        }
    }
}
