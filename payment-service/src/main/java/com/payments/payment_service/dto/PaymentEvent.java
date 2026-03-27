package com.payments.payment_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Canonical event object serialized into Kafka and the outbox table.
 *
 * Schema versioning:
 *  - "version" allows consumers to handle backwards-incompatible changes safely.
 *  - "eventId" is a UUID generated per-event, used as the idempotency key
 *    in the processed_events table. Consumers MUST use this for deduplication.
 *
 * Tracing fields:
 *  - "traceId"       — spans the entire distributed operation across all services.
 *                      One traceId covers: HTTP request → payment.created → fraud pipeline
 *                      → payment.processed → webhook delivery.
 *  - "correlationId" — per-request / per-hop identifier. Useful for correlating log
 *                      entries within a single service call. May be re-generated at
 *                      each service boundary whereas traceId is preserved end-to-end.
 *
 * Backwards-compatibility contract:
 *  - New fields MUST have default values / be nullable (Jackson ignores unknown fields).
 *  - Removing a field requires a version bump (v1 → v2) and a dual-read consumer.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {

    /** Schema version — increment on breaking changes. Current: v1 */
    @Builder.Default
    private String version = "v1";

    /** Globally unique event ID — used for exactly-once deduplication. */
    private String eventId;

    /**
     * Distributed trace ID — propagated across all services and Kafka hops.
     * Set once at the HTTP entry point (CorrelationIdFilter) and never changed.
     * Extracted from / injected into the x-trace-id Kafka header + HTTP header.
     */
    private String traceId;

    private UUID paymentId;
    private UUID userId;
    private UUID merchantId;
    private BigDecimal amount;
    private String currency;
    private String status;
    private String idempotencyKey;
    private String description;
    private String correlationId;
    private String eventType;
    private LocalDateTime createdAt;
    private LocalDateTime eventTimestamp;
}
