package com.payments.payment_service.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Transactional outbox pattern entity.
 * Events are written to this table within the same DB transaction as the
 * business operation. A separate scheduler polls and publishes to Kafka,
 * guaranteeing exactly-once-like delivery (at-least-once + idempotent consumers).
 */
@Entity
@Table(name = "outbox_events", indexes = {
    @Index(name = "idx_outbox_status", columnList = "status"),
    @Index(name = "idx_outbox_created_at", columnList = "created_at")
})
@Getter
@Setter
@NoArgsConstructor
public class OutboxEvent {

    @Id
    @Column(updatable = false, nullable = false)
    private UUID id;

    /**
     * Kafka topic name: payment.created, payment.processed, payment.failed, etc.
     */
    @Column(name = "event_type", nullable = false)
    private String eventType;

    /**
     * Kafka partition key (paymentId).
     */
    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    /**
     * JSON-serialized event payload.
     */
    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxEventStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    /**
     * Kafka partition key — controls which partition this event lands on.
     * Default: paymentId. Override with userId for per-user ordering.
     * If null, falls back to aggregateId in the outbox publisher.
     */
    @Column(name = "partition_key")
    private String partitionKey;

    /**
     * Per-request correlation ID (per-hop tracing).
     * Stored explicitly so the publisher never needs to parse the JSON payload.
     */
    @Column(name = "correlation_id")
    private String correlationId;

    /**
     * End-to-end distributed trace ID.
     * Generated once at the HTTP entry point and propagated unchanged across all
     * Kafka hops and service boundaries. Stored explicitly to avoid JSON parsing
     * in the outbox publisher — the publisher reads this field directly.
     */
    @Column(name = "trace_id")
    private String traceId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "published_at")
    private LocalDateTime publishedAt;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        if (status == null) {
            status = OutboxEventStatus.NEW;
        }
        if (retryCount == 0) {
            retryCount = 0;
        }
    }
}
