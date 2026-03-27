package com.payments.payment_service.service;

import com.payments.payment_service.dto.PaymentEvent;
import com.payments.payment_service.dto.WebhookPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Transactional notification dispatcher for payment.processed / payment.failed events.
 *
 * ============================================================
 * WHY THIS SERVICE EXISTS — THE SELF-INVOCATION TRAP
 * ============================================================
 * Spring @Transactional works via AOP proxies. When a bean calls one of its OWN
 * @Transactional methods directly (this.someMethod()), the call bypasses the proxy
 * and Spring never starts a transaction. This is the classic "self-invocation" problem.
 *
 * PaymentProcessedConsumer previously had a @Transactional dispatchTransactional()
 * called from the same class — meaning @Transactional was silently ignored.
 * IdempotentConsumerService.tryMarkProcessed() uses Propagation.MANDATORY and
 * therefore threw IllegalTransactionStateException on every single message, which
 * was caught and routed every message to the DLQ.
 *
 * FIX: Extract the transactional logic into THIS separate @Service bean. Spring
 * injects it through its proxy, so @Transactional is honoured correctly.
 *
 * ============================================================
 * EXACTLY-ONCE GUARANTEE
 * ============================================================
 * The dispatchNotification() method is the sole transactional boundary for
 * the payment-notification-group consumer. Within a single DB transaction it:
 *   1. Inserts a row into processed_events (PK = eventId).
 *      Duplicate insertion → DataIntegrityViolationException → duplicate detected.
 *   2. If first time: persists WebhookDelivery records via WebhookService (DB write).
 *
 * If the transaction commits → both the dedup record and the webhook schedule land.
 * If the transaction rolls back → neither lands → Kafka redelivery processes cleanly.
 *
 * WebSocket broadcast is intentionally OUTSIDE this method (fire-and-forget).
 * The caller performs it after this method returns so it never holds a DB connection.
 *
 * ============================================================
 * EFFECTIVELY-ONCE DELIVERY (OUTBOX PATTERN)
 * ============================================================
 * The broader system achieves effectively-once delivery by combining:
 *   1. Idempotent Kafka producer (enable.idempotence=true, acks=all, retries=5)
 *      → each Kafka message is delivered at-least-once with no broker-side duplicates.
 *   2. Transactional outbox (events written to DB in same TX as business state)
 *      → no event is lost even if the service crashes between DB write and Kafka send.
 *   3. Idempotent consumers (processed_events table + PK constraint)
 *      → duplicate Kafka redeliveries are detected and skipped atomically.
 *
 * Result: even under pod restarts, broker failures, or consumer group rebalances,
 * each payment event produces exactly one webhook dispatch and one ledger entry.
 */
@Service
public class PaymentNotificationService {

    private static final Logger log = LoggerFactory.getLogger(PaymentNotificationService.class);
    private static final String CONSUMER_GROUP = "payment-notification-group";

    private final IdempotentConsumerService idempotentConsumerService;
    private final WebhookService webhookService;

    public PaymentNotificationService(
        IdempotentConsumerService idempotentConsumerService,
        WebhookService webhookService
    ) {
        this.idempotentConsumerService = idempotentConsumerService;
        this.webhookService = webhookService;
    }

    /**
     * Dispatches a payment notification transactionally.
     *
     * @param topic         source Kafka topic (payment.processed / payment.failed)
     * @param event         deserialized payment event
     * @param eventId       globally unique event ID — dedup key in processed_events
     * @param correlationId distributed tracing correlation ID
     * @return true if this was the first time this event was processed (webhook was dispatched),
     *         false if it was a duplicate (skipped — caller must not broadcast either)
     */
    @Transactional
    public boolean dispatchNotification(
        String topic,
        PaymentEvent event,
        String eventId,
        String correlationId
    ) {
        // Primary dedup: atomic with the webhook DB write in this transaction.
        // On duplicate Kafka redelivery → DataIntegrityViolationException → returns false.
        boolean isFirst = idempotentConsumerService.tryMarkProcessed(eventId, CONSUMER_GROUP, topic);
        if (!isFirst) {
            log.info("Duplicate {} event — skipping notification. eventId={} paymentId={}",
                topic, eventId, event.getPaymentId());
            return false;
        }

        WebhookPayload payload = WebhookPayload.builder()
            .eventId(UUID.randomUUID().toString())
            .eventType(topic)
            .paymentId(event.getPaymentId())
            .status(event.getStatus())
            .correlationId(correlationId)
            .timestamp(LocalDateTime.now())
            .data(event)
            .build();

        webhookService.dispatchEvent(event.getMerchantId(), topic, payload);
        log.debug("Webhook dispatched eventId={} paymentId={} topic={}", eventId, event.getPaymentId(), topic);

        return true;
    }
}
