package org.example.ptit_cntt1_it214_session18_mini.payment.controller;

import lombok.RequiredArgsConstructor;
import org.example.ptit_cntt1_it214_session18_mini.payment.model.PaymentTransaction;
import org.example.ptit_cntt1_it214_session18_mini.payment.service.PaymentService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api/payment")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping("/process")
    public ResponseEntity<PaymentTransaction> processPayment(
            @RequestParam Long orderId,
            @RequestParam Long customerId,
            @RequestParam BigDecimal amount,
            @RequestParam(defaultValue = "false") boolean simulateFailure) {
        return ResponseEntity.ok(paymentService.processPayment(orderId, customerId, amount, simulateFailure));
    }

    @PostMapping("/refund")
    public ResponseEntity<PaymentTransaction> refundPayment(
            @RequestParam Long orderId,
            @RequestParam BigDecimal amount) {
        return ResponseEntity.ok(paymentService.refundPayment(orderId, amount));
    }

    @GetMapping("/transactions/{orderId}")
    public ResponseEntity<PaymentTransaction> getPaymentByOrderId(@PathVariable Long orderId) {
        return paymentService.getPaymentByOrderId(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
