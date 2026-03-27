package com.payments.payment_service.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "webhook_deliveries", indexes = {
    @Index(name = "idx_wh_delivery_status", columnList = "status"),
    @Index(name = "idx_wh_delivery_payment_id", columnList = "payment_id"),
    @Index(name = "idx_wh_delivery_next_retry", columnList = "next_retry_at")
})
@Getter
@Setter
@NoArgsConstructor
public class WebhookDelivery {

    @Id
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "webhook_endpoint_id", nullable = false)
    private UUID webhookEndpointId;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;

    /**
     * Idempotency key for this specific delivery to prevent duplicate sends.
     */
    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private WebhookDeliveryStatus status;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "last_response_code")
    private Integer lastResponseCode;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "next_retry_at")
    private LocalDateTime nextRetryAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "last_attempted_at")
    private LocalDateTime lastAttemptedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        if (status == null) {
            status = WebhookDeliveryStatus.PENDING;
        }
    }
}
