package org.example.ptit_cntt1_it214_session18_mini.payment.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.ptit_cntt1_it214_session18_mini.payment.exception.PaymentFailedException;
import org.example.ptit_cntt1_it214_session18_mini.payment.model.PaymentStatus;
import org.example.ptit_cntt1_it214_session18_mini.payment.model.PaymentTransaction;
import org.example.ptit_cntt1_it214_session18_mini.payment.repository.PaymentTransactionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private final PaymentTransactionRepository paymentRepository;

    private static final BigDecimal BALANCE_LIMIT = new BigDecimal("500000000"); // 500 triệu VND limit

    @Transactional(noRollbackFor = PaymentFailedException.class)
    public PaymentTransaction processPayment(Long orderId, Long customerId, BigDecimal amount, boolean simulateFailure) {
        log.info("[PAYMENT] Processing payment for Order ID={}, Customer ID={}, Amount={}",
                orderId, customerId, amount);

        // Kiểm tra điều kiện thất bại thanh toán (để minh chứng kịch bản Rollback của Saga)
        if (simulateFailure || amount.compareTo(BALANCE_LIMIT) > 0) {
            String reason = simulateFailure ? "Simulated Payment Gateway Error (Timeout/Declined)"
                    : "Insufficient customer account balance (Balance < " + amount + ")";
            log.error("[PAYMENT FAILED] Transaction rejected for Order ID={}. Reason: {}", orderId, reason);

            PaymentTransaction failedTx = PaymentTransaction.builder()
                    .orderId(orderId)
                    .customerId(customerId)
                    .amount(amount)
                    .status(PaymentStatus.FAILED)
                    .failureReason(reason)
                    .build();
            paymentRepository.save(failedTx);

            throw new PaymentFailedException(reason);
        }

        PaymentTransaction successTx = PaymentTransaction.builder()
                .orderId(orderId)
                .customerId(customerId)
                .amount(amount)
                .status(PaymentStatus.SUCCESS)
                .build();

        PaymentTransaction saved = paymentRepository.save(successTx);
        log.info("[PAYMENT SUCCESS] Transaction completed successfully. TX_ID={}, Order ID={}",
                saved.getId(), orderId);
        return saved;
    }

    @Transactional
    public PaymentTransaction refundPayment(Long orderId, BigDecimal amount) {
        log.warn("[PAYMENT REFUND] Executing payment refund compensation for Order ID={}, Amount={}",
                orderId, amount);

        Optional<PaymentTransaction> existingTx = paymentRepository.findByOrderId(orderId);
        PaymentTransaction refundTx;
        if (existingTx.isPresent()) {
            refundTx = existingTx.get();
            refundTx.setStatus(PaymentStatus.REFUNDED);
        } else {
            refundTx = PaymentTransaction.builder()
                    .orderId(orderId)
                    .customerId(1L)
                    .amount(amount)
                    .status(PaymentStatus.REFUNDED)
                    .failureReason("Compensating refund")
                    .build();
        }

        PaymentTransaction saved = paymentRepository.save(refundTx);
        log.info("[PAYMENT REFUND COMPLETED] Order ID={} refunded successfully", orderId);
        return saved;
    }

    public Optional<PaymentTransaction> getPaymentByOrderId(Long orderId) {
        return paymentRepository.findByOrderId(orderId);
    }
}
