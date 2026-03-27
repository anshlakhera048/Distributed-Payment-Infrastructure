package com.payments.payment_service.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payments.payment_service.dto.PaymentEvent;
import com.payments.payment_service.service.PaymentNotificationService;
import com.payments.payment_service.websocket.WebSocketBroadcaster;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Consumes {@code payment.processed} and {@code payment.failed} events.
 *
 * ============================================================
 * TRANSACTION BOUNDARY — EXACTLY-ONCE GUARANTEE
 * ============================================================
 * This consumer does NOT have any @Transactional logic itself.
 * All transactional work (dedup insert + webhook dispatch) is delegated to
 * {@link PaymentNotificationService#dispatchNotification}, which Spring injects
 * as a proxied bean — ensuring @Transactional is honoured correctly.
 *
 * ❌ ANTI-PATTERN (what we do NOT do):
 *   Calling a @Transactional method from within the SAME class (this.method())
 *   bypasses Spring's AOP proxy → @Transactional is silently ignored →
 *   IdempotentConsumerService's Propagation.MANDATORY throws → every message goes to DLQ.
 *
 * ✅ CORRECT PATTERN (what we DO):
 *   Delegate to a SEPARATE @Service bean (PaymentNotificationService) which is
 *   injected through its Spring proxy → @Transactional is applied correctly.
 *
 * ============================================================
 * WEBSO CKET BROADCAST — INTENTIONALLY OUTSIDE TX
 * ============================================================
 * webSocketBroadcaster.broadcast() runs AFTER the DB transaction commits.
 * This ensures WebSocket connections never block a DB connection open.
 * WebSocket delivery is fire-and-forget (best-effort) — a missed broadcast
 * does not affect payment correctness. Clients can always poll GET /payments/:id.
 */
@Component
public class PaymentProcessedConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessedConsumer.class);
    private static final String DLQ_TOPIC = "payment.processed.DLQ";
    private static final String CONSUMER_GROUP = "payment-notification-group";

    private final ObjectMapper objectMapper;
    private final PaymentNotificationService notificationService;
    private final WebSocketBroadcaster webSocketBroadcaster;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public PaymentProcessedConsumer(
        ObjectMapper objectMapper,
        PaymentNotificationService notificationService,
        WebSocketBroadcaster webSocketBroadcaster,
        KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.objectMapper = objectMapper;
        this.notificationService = notificationService;
        this.webSocketBroadcaster = webSocketBroadcaster;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
        topics = {"payment.processed", "payment.failed"},
        groupId = CONSUMER_GROUP,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handlePaymentResult(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            PaymentEvent event = objectMapper.readValue(record.value(), PaymentEvent.class);

            String correlationId = extractHeader(record, "x-correlation-id");
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = event.getCorrelationId();
            }
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = UUID.randomUUID().toString();
            }

            String eventId = extractHeader(record, "x-event-id");
            if (eventId == null || eventId.isBlank()) {
                eventId = event.getEventId();
            }
            if (eventId == null || eventId.isBlank()) {
                eventId = record.topic() + ":" + record.partition() + ":" + record.offset();
            }

            // traceId: spans the entire distributed trace (broader than correlationId)
            String traceId = extractHeader(record, "x-trace-id");
            if (traceId == null || traceId.isBlank()) {
                traceId = event.getTraceId() != null ? event.getTraceId() : correlationId;
            }

            MDC.put("correlationId", correlationId);
            MDC.put("traceId", traceId);
            MDC.put("paymentId", event.getPaymentId().toString());
            MDC.put("eventId", eventId);

            log.info("Received {} eventId={} paymentId={}", record.topic(), eventId, event.getPaymentId());

            // ---------------------------------------------------------------
            // Transactional dispatch via injected proxy bean — NOT self-invocation.
            // dispatchNotification() handles:
            //   1. Dedup check (processed_events insert, Propagation.MANDATORY)
            //   2. Webhook dispatch (DB write)
            // Both are atomic within a single DB transaction.
            // ---------------------------------------------------------------
            final String rawPayload = record.value();
            boolean dispatched = notificationService.dispatchNotification(
                record.topic(), event, eventId, correlationId
            );

            // WebSocket broadcast is OUTSIDE the DB transaction (fire-and-forget).
            // Only broadcast for first-time processing; duplicates are silently skipped.
            if (dispatched) {
                webSocketBroadcaster.broadcast(rawPayload);
            }

            ack.acknowledge();
            log.info("{} fully processed eventId={} paymentId={}", record.topic(), eventId, event.getPaymentId());

        } catch (Exception e) {
            log.error("Unrecoverable error processing {} offset={} partition={}: {}",
                record.topic(), record.offset(), record.partition(), e.getMessage(), e);

            kafkaTemplate.send(DLQ_TOPIC, record.key(), record.value())
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        log.error("CRITICAL: Failed to send to DLQ! key={}", record.key(), ex);
                    } else {
                        log.warn("Sent to DLQ: topic={} key={}", DLQ_TOPIC, record.key());
                    }
                });

            ack.acknowledge();
        } finally {
            MDC.remove("correlationId");
            MDC.remove("traceId");
            MDC.remove("paymentId");
            MDC.remove("eventId");
        }
    }

    private String extractHeader(ConsumerRecord<?, ?> record, String key) {
        return KafkaHeaderUtil.extractHeader(record, key);
    }
}

