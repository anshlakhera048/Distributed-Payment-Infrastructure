package com.payments.payment_service.service;

import com.payments.payment_service.entity.ProcessedEvent;
import com.payments.payment_service.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Exactly-once consumer deduplication guard.
 *
 * Pattern:
 *   1. Consumer receives event with an eventId (from Kafka header or payload field)
 *   2. Call tryMarkProcessed(eventId, group, topic) — INSIDE the outer @Transactional
 *   3. If returns false → already processed → skip and ack
 *   4. If returns true → proceed with business logic in the same transaction
 *   5. Commit → both the business change and the processed_events row land atomically
 *
 * On crash between step 4 and the commit:
 *   → Transaction rolls back → processed_events row is NOT inserted
 *   → Kafka redelivers → consumer processes again correctly
 *
 * Thread-safety:
 *   The PRIMARY KEY constraint on processed_events.event_id makes concurrent
 *   duplicate inserts fail deterministically with DataIntegrityViolationException.
 */
@Service
public class IdempotentConsumerService {

    private static final Logger log = LoggerFactory.getLogger(IdempotentConsumerService.class);
    private static final int RETENTION_DAYS = 7;

    private final ProcessedEventRepository processedEventRepository;

    public IdempotentConsumerService(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    /**
     * Attempts to mark an event as processed.
     *
     * @param eventId       unique event identifier (from Kafka header "x-event-id" or payload)
     * @param consumerGroup the consumer group doing the processing
     * @param topic         source Kafka topic
     * @return true if this is the first time this event is being processed (proceed),
     *         false if it was already processed (skip)
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean tryMarkProcessed(String eventId, String consumerGroup, String topic) {
        try {
            processedEventRepository.save(new ProcessedEvent(eventId, consumerGroup, topic));
            return true;
        } catch (DataIntegrityViolationException e) {
            // Duplicate PK → already processed
            log.info("Duplicate event detected — skipping. eventId={} group={} topic={}",
                eventId, consumerGroup, topic);
            return false;
        }
    }

    /**
     * Weekly retention cleanup — keeps the table bounded.
     * Old events (>7 days) are safe to delete because Kafka retention
     * is also typically 7 days for these topics.
     */
    @Scheduled(cron = "0 0 2 * * SUN")
    @Transactional
    public void cleanupOldEvents() {
        LocalDateTime cutoff = LocalDateTime.now().minusDays(RETENTION_DAYS);
        int deleted = processedEventRepository.deleteOlderThan(cutoff);
        log.info("Cleaned up {} old processed_events entries older than {}", deleted, cutoff);
    }
}
