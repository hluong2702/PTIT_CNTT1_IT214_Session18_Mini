package org.example.ptit_cntt1_it214_session18_mini.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.ptit_cntt1_it214_session18_mini.order.dto.OrderRequest;
import org.example.ptit_cntt1_it214_session18_mini.order.feign.ProductDto;
import org.example.ptit_cntt1_it214_session18_mini.order.model.Order;
import org.example.ptit_cntt1_it214_session18_mini.order.model.OrderStatus;
import org.example.ptit_cntt1_it214_session18_mini.order.repository.OrderRepository;
import org.example.ptit_cntt1_it214_session18_mini.payment.service.PaymentService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.NoSuchElementException;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final InventoryClientService inventoryClientService;
    private final PaymentService paymentService;

    @Transactional
    public Order createOrder(OrderRequest request) {
        log.info("Creating order for customer id: {}, product id: {}, quantity: {}",
                request.getCustomerId(), request.getProductId(), request.getQuantity());

        BigDecimal unitPrice = request.getUnitPrice();
        if (unitPrice == null) {
            ProductDto product = inventoryClientService.getProductById(request.getProductId());
            unitPrice = product.getPrice();
        }
        BigDecimal totalPrice = unitPrice.multiply(BigDecimal.valueOf(request.getQuantity()));

        // Trừ tồn kho thông qua FeignClient + Circuit Breaker
        inventoryClientService.setSimulateInventoryDown(request.isSimulateInventoryDown());
        inventoryClientService.deductStock(request.getProductId(), request.getQuantity());

        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .totalPrice(totalPrice)
                .status(OrderStatus.PENDING)
                .build();
        order = orderRepository.save(order);

        // Xử lý thanh toán
        paymentService.processPayment(order.getId(), order.getCustomerId(), totalPrice, request.isSimulatePaymentFailure());

        order.setStatus(OrderStatus.CONFIRMED);
        return orderRepository.save(order);
    }

    public Order getOrderById(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Order not found with id: " + id));
    }

    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }
}
