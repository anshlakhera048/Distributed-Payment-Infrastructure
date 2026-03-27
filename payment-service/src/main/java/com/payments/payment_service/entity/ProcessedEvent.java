package com.payments.payment_service.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * Deduplication table for Kafka consumer exactly-once semantics.
 *
 * Before any consumer processes an event, it inserts a row here.
 * The PRIMARY KEY constraint on event_id makes a second insert fail
 * immediately — the consumer then skips processing and acks the message.
 *
 * This guarantees:
 *   - No duplicate ledger writes
 *   - No duplicate state transitions
 *   - No duplicate webhook dispatches
 *
 * The check+insert is done within the SAME @Transactional boundary as
 * the business operation, so the guarantee is atomic.
 */
@Entity
@Table(name = "processed_events")
@Getter
@Setter
@NoArgsConstructor
public class ProcessedEvent {

    /**
     * Composite logical event ID: "<topic>:<partitionKey>:<eventId>"
     * Using all three parts prevents collisions across topics and offsets.
     */
    @Id
    @Column(name = "event_id", length = 512, updatable = false, nullable = false)
    private String eventId;

    @Column(name = "consumer_group", nullable = false)
    private String consumerGroup;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Column(name = "processed_at", nullable = false, updatable = false)
    private LocalDateTime processedAt;

    @PrePersist
    protected void onCreate() {
        processedAt = LocalDateTime.now();
    }

    public ProcessedEvent(String eventId, String consumerGroup, String topic) {
        this.eventId = eventId;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
    }
}
