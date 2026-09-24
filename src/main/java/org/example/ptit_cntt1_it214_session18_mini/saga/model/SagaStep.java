package org.example.ptit_cntt1_it214_session18_mini.saga.model;

public enum SagaStep {
    ORDER_INITIALIZED,
    INVENTORY_DEDUCTED,
    PAYMENT_SUCCESS,
    PAYMENT_FAILED,
    INVENTORY_COMPENSATED,
    SAGA_SUCCESS,
    SAGA_ROLLED_BACK
}
