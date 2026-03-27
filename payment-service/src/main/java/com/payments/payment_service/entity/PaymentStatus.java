package com.payments.payment_service.entity;

public enum PaymentStatus {
    PENDING,
    PROCESSING,
    SUCCESS,
    FAILED,
    FRAUD_REJECTED,
    RECONCILED
}
