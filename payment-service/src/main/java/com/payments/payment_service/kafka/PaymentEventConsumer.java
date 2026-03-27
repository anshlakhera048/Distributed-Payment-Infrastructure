package com.payments.payment_service.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payments.payment_service.dto.FraudRequestEvent;
import com.payments.payment_service.dto.PaymentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Consumes {@code payment.created} events and publishes to {@code fraud.request}.
 *
 * Async fraud pipeline (Upgrade 5):
 *   payment.created → [this consumer] → fraud.request
 *                                               ↓
 *                                      Python fraud-service (Kafka consumer)
 *                                               ↓
 *                                          fraud.result
 *                                               ↓
 *                                      {@link FraudResultConsumer} → processPaymentResult()
 *
 * The correlation ID (x-correlation-id) and event ID (x-event-id) are propagated
 * as Kafka headers into fraud.request so the full trace is observable end-to-end.
 *
 * Exactly-once guarantee:
 *   The idempotency check happens in {@link FraudResultConsumer} /
 *   {@link com.payments.payment_service.service.PaymentService#processPaymentResult}
 *   — not here. This consumer is idempotent by design: publishing the same
 *   fraud.request event twice is safe because {@code FraudResultConsumer} deduplicates
 *   the result via {@code IdempotentConsumerService.tryMarkProcessed(eventId)}.
 */
@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);
    private static final String DLQ_TOPIC = "payment.created.DLQ";
    private static final String CONSUMER_GROUP = "payment-processor-group";
    private static final String FRAUD_REQUEST_TOPIC = "fraud.request";

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public PaymentEventConsumer(
        ObjectMapper objectMapper,
        KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
        topics = "payment.created",
        groupId = CONSUMER_GROUP,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handlePaymentCreated(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            PaymentEvent event = objectMapper.readValue(record.value(), PaymentEvent.class);

            // Extract correlation ID: prefer Kafka header → fallback to payload field → generate
            String correlationId = KafkaHeaderUtil.extractHeader(record, KafkaHeaderUtil.HEADER_CORRELATION_ID);
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = event.getCorrelationId();
            }
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = UUID.randomUUID().toString();
            }

            // Derive a stable event ID for the fraud.request dispatch
            String eventId = KafkaHeaderUtil.extractHeader(record, KafkaHeaderUtil.HEADER_EVENT_ID);
            if (eventId == null || eventId.isBlank()) {
                eventId = event.getEventId();
            }
            if (eventId == null || eventId.isBlank()) {
                eventId = record.topic() + ":" + record.partition() + ":" + record.offset();
            }

            // Extract traceId: prefer Kafka header → payload field → fall back to correlationId.
            // traceId spans the full distributed trace; correlationId is per-hop.
            String traceId = KafkaHeaderUtil.extractHeader(record, KafkaHeaderUtil.HEADER_TRACE_ID);
            if (traceId == null || traceId.isBlank()) {
                traceId = event.getTraceId();
            }
            if (traceId == null || traceId.isBlank()) {
                traceId = correlationId;
            }

            MDC.put("traceId",     traceId);
            MDC.put("correlationId", correlationId);
            MDC.put("paymentId",  event.getPaymentId().toString());
            MDC.put("eventId",    eventId);

            log.info("Received payment.created eventId={} paymentId={} — dispatching to fraud.request",
                eventId, event.getPaymentId());

            if (!"v1".equals(event.getVersion())) {
                log.warn("Unknown event schema version={} for eventId={} — processing with best-effort",
                    event.getVersion(), eventId);
            }

            // Build the fraud request event (snake_case JSON for Python service)
            FraudRequestEvent fraudRequest = FraudRequestEvent.builder()
                .eventId(eventId)
                .paymentId(event.getPaymentId())
                .userId(event.getUserId())
                .amount(event.getAmount())
                .currency(event.getCurrency())
                .correlationId(correlationId)
                .build();

            String fraudRequestJson = objectMapper.writeValueAsString(fraudRequest);

            // Propagate all three standard headers into fraud.request.
            // KafkaHeaderUtil is the single source of truth for header names.
            final String finalEventId = eventId;
            RecordHeaders headers = KafkaHeaderUtil.buildHeaders(eventId, correlationId, traceId);

            // Partition key: userId — co-locates all payment events for a user on the same
            // partition, enabling ordered per-user processing. See KafkaConfig for full rationale.
            String partitionKey = event.getUserId() != null
                ? event.getUserId().toString()
                : event.getPaymentId().toString(); // defensive fallback for malformed events

            ProducerRecord<String, String> fraudRecord = new ProducerRecord<>(
                FRAUD_REQUEST_TOPIC, null,
                partitionKey, fraudRequestJson,
                headers
            );

            kafkaTemplate.send(fraudRecord).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish fraud.request for eventId={} paymentId={}: {}",
                        finalEventId, event.getPaymentId(), ex.getMessage(), ex);
                } else {
                    log.debug("Published fraud.request eventId={} paymentId={} partition={} offset={}",
                        finalEventId, event.getPaymentId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                }
            });

            ack.acknowledge();
            log.info("payment.created processed — fraud.request enqueued. eventId={}", eventId);

        } catch (Exception e) {
            log.error("Unrecoverable error on payment.created offset={} partition={}: {}",
                record.offset(), record.partition(), e.getMessage(), e);

            kafkaTemplate.send(DLQ_TOPIC, record.key(), record.value())
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        log.error("CRITICAL: Failed to send to DLQ! Manual intervention required. key={}",
                            record.key(), ex);
                    } else {
                        log.warn("Sent to DLQ: topic={} key={}", DLQ_TOPIC, record.key());
                    }
                });

            ack.acknowledge();
        } finally {
            MDC.remove("traceId");
            MDC.remove("correlationId");
            MDC.remove("paymentId");
            MDC.remove("eventId");
        }
    }

}


