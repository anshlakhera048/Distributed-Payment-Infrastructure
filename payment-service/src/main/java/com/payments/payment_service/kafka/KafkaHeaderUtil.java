package com.payments.payment_service.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;

/**
 * Centralised Kafka header construction and extraction utility.
 *
 * STANDARD HEADERS (enforced across all producers in this service):
 *
 *   x-event-id       — Unique ID for this Kafka message; used as the deduplication key in
 *                      {@code IdempotentConsumerService.tryMarkProcessed()}. Never changes
 *                      within a single publish attempt. Equals the OutboxEvent UUID.
 *
 *   x-correlation-id — Per-request / per-hop correlation ID. Propagated from the originating
 *                      HTTP request header (X-Correlation-ID) and forwarded through every
 *                      Kafka hop. May differ from traceId if a service generates its own.
 *
 *   x-trace-id       — End-to-end distributed trace ID. Generated ONCE at the HTTP entry
 *                      point (CorrelationIdFilter) and propagated unchanged through all Kafka
 *                      topics and service boundaries. Stored in OutboxEvent.traceId and the
 *                      payments table (trace_id column) so it is always available without
 *                      parsing JSON payloads.
 *
 * USAGE (producers):
 * <pre>
 *   RecordHeaders headers = KafkaHeaderUtil.buildHeaders(eventId, correlationId, traceId);
 *   ProducerRecord&lt;String, String&gt; record =
 *       new ProducerRecord&lt;&gt;(topic, null, partitionKey, payload, headers);
 * </pre>
 *
 * USAGE (consumers):
 * <pre>
 *   String traceId = KafkaHeaderUtil.extractHeader(record, "x-trace-id");
 * </pre>
 *
 * Thread-safety: all methods are stateless and safe for concurrent use.
 */
public final class KafkaHeaderUtil {

    // Standard header names — single source of truth for all producers and consumers.
    public static final String HEADER_EVENT_ID       = "x-event-id";
    public static final String HEADER_CORRELATION_ID = "x-correlation-id";
    public static final String HEADER_TRACE_ID       = "x-trace-id";

    private KafkaHeaderUtil() {
        // Utility class — no instances
    }

    /**
     * Builds a {@link RecordHeaders} containing all three standard trace headers.
     * Null or blank values are replaced with an empty string to avoid NPE in consumers.
     *
     * @param eventId       unique ID for this message (OutboxEvent UUID)
     * @param correlationId per-request correlation ID
     * @param traceId       end-to-end distributed trace ID
     */
    public static RecordHeaders buildHeaders(String eventId, String correlationId, String traceId) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(HEADER_EVENT_ID,       toBytes(eventId));
        headers.add(HEADER_CORRELATION_ID, toBytes(correlationId));
        headers.add(HEADER_TRACE_ID,       toBytes(traceId));
        return headers;
    }

    /**
     * Extracts a header value from a Kafka consumer record.
     * Returns {@code null} if the header is absent or has no value.
     *
     * @param record the incoming Kafka record
     * @param key    the header name (e.g. {@link #HEADER_TRACE_ID})
     */
    public static String extractHeader(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static byte[] toBytes(String value) {
        return (value != null ? value : "").getBytes(StandardCharsets.UTF_8);
    }
}
