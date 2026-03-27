package com.payments.payment_service.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Kafka event DTO for the {@code fraud.request} topic.
 *
 * Uses snake_case JSON naming so the Python fraud-service Pydantic models
 * can deserialize the payload without field aliasing.
 *
 * Published by: {@link com.payments.payment_service.kafka.PaymentEventConsumer}
 * Consumed by:  Python fraud-service Kafka consumer
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class FraudRequestEvent {

    /** Unique event ID — propagated to fraud.result for end-to-end idempotency. */
    private String eventId;

    private UUID paymentId;
    private UUID userId;
    private BigDecimal amount;
    private String currency;
    private String correlationId;
}
