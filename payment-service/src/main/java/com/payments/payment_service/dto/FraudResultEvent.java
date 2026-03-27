package com.payments.payment_service.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Kafka event DTO for the {@code fraud.result} topic.
 *
 * Published by: Python fraud-service Kafka producer
 * Consumed by:  {@link com.payments.payment_service.kafka.FraudResultConsumer}
 *
 * Uses snake_case to match the Python serialization convention.
 * Mirrors {@link FraudCheckResponse} but adds {@code eventId} for idempotency
 * and {@code correlationId} for distributed tracing.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class FraudResultEvent {

    /** Echoed from the original fraud.request eventId — dedup key in FraudResultConsumer. */
    private String eventId;

    private UUID paymentId;
    private boolean fraud;
    private String reason;
    private double score;

    /** "rule" | "ml" | "combined" — which pipeline stage made the decision. */
    private String stage;

    /** True when the service returned a circuit-breaker fallback result. */
    private boolean fallback;

    private String correlationId;
}
