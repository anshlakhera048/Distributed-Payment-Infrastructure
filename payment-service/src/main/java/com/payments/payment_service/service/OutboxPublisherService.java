package com.payments.payment_service.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payments.payment_service.entity.OutboxEvent;
import com.payments.payment_service.entity.OutboxEventStatus;
import com.payments.payment_service.kafka.KafkaHeaderUtil;
import com.payments.payment_service.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Transactional Outbox Pattern publisher.
 *
 * Polls the outbox_events table for NEW events and publishes them to Kafka.
 * This guarantees that no event is lost even if the original service crashes
 * between persisting the business state and calling Kafka.
 *
 * Only marks an event PUBLISHED once Kafka confirms receipt.
 * After {@code maxRetries} failures the event is marked FAILED for manual review.
 */
@Service
public class OutboxPublisherService {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherService.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.outbox.max-retries}")
    private int maxRetries;

    public OutboxPublisherService(
        OutboxEventRepository outboxEventRepository,
        KafkaTemplate<String, String> kafkaTemplate,
        ObjectMapper objectMapper
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    // ObjectMapper still needed for JSON serialization in createOutboxEvent().
    // It is NOT used to parse payloads for metadata — trace/correlation IDs are
    // stored as explicit columns in OutboxEvent and read directly.

    @Scheduled(fixedDelayString = "${app.outbox.polling-interval-ms}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> events =
            outboxEventRepository.findTop50ByStatusOrderByCreatedAtAsc(OutboxEventStatus.NEW);

        if (events.isEmpty()) {
            return;
        }

        log.debug("Publishing {} outbox events to Kafka", events.size());

        for (OutboxEvent event : events) {
            try {
                // All three trace IDs come from OutboxEvent explicit columns —
                // no JSON payload parsing required. This avoids coupling the publisher
                // to the payload schema and is resilient to schema evolution.
                //
                // EFFECTIVELY-ONCE DELIVERY:
                //   idempotent producer (enable.idempotence=true, acks=all) eliminates broker-side
                //   duplicates. Consumers deduplicate via processed_events table:
                //   at-least-once delivery + idempotent consumers = effectively-once semantics.
                String eventId       = event.getId().toString();
                String correlationId = event.getCorrelationId() != null ? event.getCorrelationId() : eventId;
                String traceId       = event.getTraceId()       != null ? event.getTraceId()       : correlationId;

                // KafkaHeaderUtil enforces the standard three-header contract
                // (x-event-id, x-correlation-id, x-trace-id) across all producers.
                RecordHeaders headers = KafkaHeaderUtil.buildHeaders(eventId, correlationId, traceId);

                // Partition key strategy: use partitionKey field (userId) if set, else aggregateId (paymentId).
                // userId partitioning: co-locates all events for a user on the same partition,
                // enabling sequential per-user processing and velocity tracking without coordination.
                // See KafkaConfig and README for hot-user mitigation strategy.
                String partitionKey = event.getPartitionKey() != null
                    ? event.getPartitionKey()
                    : event.getAggregateId();

                ProducerRecord<String, String> record = new ProducerRecord<>(
                    event.getEventType(),
                    null,
                    partitionKey,
                    event.getPayload(),
                    headers
                );

                kafkaTemplate.send(record)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Kafka publish failed for outbox event id={} topic={}: {}",
                                event.getId(), event.getEventType(), ex.getMessage());
                        }
                    });

                event.setStatus(OutboxEventStatus.PUBLISHED);
                event.setPublishedAt(LocalDateTime.now());

            } catch (Exception ex) {
                int retries = event.getRetryCount() + 1;
                event.setRetryCount(retries);
                event.setLastError(ex.getMessage());

                if (retries >= maxRetries) {
                    log.error("Outbox event id={} exhausted {} retries — marking FAILED",
                        event.getId(), maxRetries, ex);
                    event.setStatus(OutboxEventStatus.FAILED);
                } else {
                    log.warn("Outbox event id={} publish attempt {} failed: {}",
                        event.getId(), retries, ex.getMessage());
                }
            }

            outboxEventRepository.save(event);
        }
    }

    /**
     * Creates an outbox event atomically within the caller's @Transactional boundary.
     *
     * <p>All three trace identifiers are stored as explicit columns (not inside the
     * JSON payload). The publisher reads them directly — no JSON parsing required.
     *
     * @param eventType     Kafka topic name
     * @param aggregateId   Business aggregate ID (paymentId) — for audit trail
     * @param partitionKey  Kafka partition key — use userId for per-user ordering;
     *                      null falls back to aggregateId at publish time
     * @param payload       Event payload (will be JSON-serialized)
     * @param correlationId per-hop correlation ID
     * @param traceId       end-to-end distributed trace ID; propagated from Payment.traceId
     */
    @Transactional
    public void createOutboxEvent(
        String eventType,
        String aggregateId,
        String partitionKey,
        Object payload,
        String correlationId,
        String traceId
    ) {
        try {
            String json = objectMapper.writeValueAsString(payload);

            OutboxEvent event = new OutboxEvent();
            event.setId(java.util.UUID.randomUUID());
            event.setEventType(eventType);
            event.setAggregateId(aggregateId);
            event.setPartitionKey(partitionKey);
            event.setPayload(json);
            event.setStatus(OutboxEventStatus.NEW);
            event.setRetryCount(0);
            event.setCorrelationId(correlationId);
            event.setTraceId(traceId);

            outboxEventRepository.save(event);
            log.debug("Created outbox event type={} aggregateId={} partitionKey={} traceId={}",
                eventType, aggregateId, partitionKey, traceId);

        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox payload for type=" + eventType, e);
        }
    }
}
