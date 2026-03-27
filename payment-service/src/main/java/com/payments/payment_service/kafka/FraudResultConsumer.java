package com.payments.payment_service.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payments.payment_service.dto.FraudCheckResponse;
import com.payments.payment_service.dto.FraudResultEvent;
import com.payments.payment_service.service.PaymentService;
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
 * Consumes {@code fraud.result} events published by the Python fraud-service.
 *
 * This is the downstream half of the async fraud pipeline (Upgrade 5).
 * The upstream half is {@link PaymentEventConsumer} which publishes to {@code fraud.request}.
 *
 * Exactly-once guarantee:
 *   {@link PaymentService#processPaymentResult} is {@code @Transactional} and calls
 *   {@code IdempotentConsumerService.tryMarkProcessed(eventId)} as its first operation.
 *   If the same fraud result is redelivered (Kafka consumer group retry), the dedup guard
 *   returns false and the method is a no-op — no double credit/debit.
 */
@Component
public class FraudResultConsumer {

    private static final Logger log = LoggerFactory.getLogger(FraudResultConsumer.class);
    private static final String DLQ_TOPIC = "fraud.result.DLQ";
    private static final String CONSUMER_GROUP = "payment-processor-group";

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public FraudResultConsumer(
        ObjectMapper objectMapper,
        PaymentService paymentService,
        KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.objectMapper = objectMapper;
        this.paymentService = paymentService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
        topics = "fraud.result",
        groupId = CONSUMER_GROUP,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleFraudResult(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            FraudResultEvent fraudResult = objectMapper.readValue(record.value(), FraudResultEvent.class);

            // Extract correlation ID and event ID — prefer headers, then payload fields, then synthetic
            String correlationId = extractHeader(record, "x-correlation-id");
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = fraudResult.getCorrelationId();
            }
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = UUID.randomUUID().toString();
            }

            String eventId = extractHeader(record, "x-event-id");
            if (eventId == null || eventId.isBlank()) {
                eventId = fraudResult.getEventId();
            }
            if (eventId == null || eventId.isBlank()) {
                eventId = record.topic() + ":" + record.partition() + ":" + record.offset();
            }

            // Extract traceId: prefer Kafka header → payload field → fall back to correlationId.
            // x-trace-id is propagated by PaymentEventConsumer when it publishes to fraud.request,
            // and forwarded by the Python fraud-service into fraud.result.
            String traceId = extractHeader(record, "x-trace-id");
            if (traceId == null || traceId.isBlank()) {
                traceId = correlationId; // fallback for events published before traceId support
            }

            MDC.put("traceId",     traceId);
            MDC.put("correlationId", correlationId);
            MDC.put("paymentId",  fraudResult.getPaymentId().toString());
            MDC.put("eventId",    eventId);

            log.info("Received fraud.result eventId={} paymentId={} fraud={} score={} stage={}",
                eventId, fraudResult.getPaymentId(), fraudResult.isFraud(),
                fraudResult.getScore(), fraudResult.getStage());

            // Map fraud.result event → FraudCheckResponse (existing payment service contract)
            FraudCheckResponse fraudCheck = FraudCheckResponse.builder()
                .paymentId(fraudResult.getPaymentId())
                .fraud(fraudResult.isFraud())
                .reason(fraudResult.getReason())
                .score(fraudResult.getScore())
                .fallback(fraudResult.isFallback())
                .build();

            // processPaymentResult is @Transactional — includes idempotency guard inside
            paymentService.processPaymentResult(
                fraudResult.getPaymentId(), fraudCheck, correlationId, eventId
            );

            ack.acknowledge();
            log.info("fraud.result fully processed eventId={} paymentId={}", eventId, fraudResult.getPaymentId());

        } catch (Exception e) {
            log.error("Unrecoverable error processing fraud.result offset={} partition={}: {}",
                record.offset(), record.partition(), e.getMessage(), e);

            kafkaTemplate.send(DLQ_TOPIC, record.key(), record.value())
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        log.error("CRITICAL: Failed to send fraud.result to DLQ! key={}", record.key(), ex);
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

    private String extractHeader(ConsumerRecord<?, ?> record, String key) {
        return KafkaHeaderUtil.extractHeader(record, key);
    }
}
