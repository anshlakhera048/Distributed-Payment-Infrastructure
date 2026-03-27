package com.payments.payment_service.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "webhook_endpoints", indexes = {
    @Index(name = "idx_webhook_merchant_id", columnList = "merchant_id")
})
@Getter
@Setter
@NoArgsConstructor
public class WebhookEndpoint {

    @Id
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "merchant_id", nullable = false)
    private UUID merchantId;

    @Column(nullable = false)
    private String url;

    /**
     * HMAC-SHA256 signing secret. Used to sign the webhook payload so
     * the merchant can verify authenticity.
     */
    @Column(nullable = false)
    private String secret;

    /**
     * Comma-separated list of subscribed events.
     * e.g. "payment.processed,payment.failed"
     */
    @Column(nullable = false)
    private String events;

    @Column(nullable = false)
    private boolean active;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        active = true;
    }
}
