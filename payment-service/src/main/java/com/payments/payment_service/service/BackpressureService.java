package com.payments.payment_service.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitors Kafka consumer lag and provides system-level backpressure signals.
 *
 * Architecture:
 *   - Polls Kafka admin API every 5 seconds for consumer group offsets
 *   - Calculates lag (end offset - committed offset) per partition
 *   - Exposes total lag via Prometheus gauge {@code kafka_consumer_lag_total}
 *   - Provides {@link #isBackpressureActive()} for rate limiter integration
 *
 * Backpressure thresholds (configurable via application.yml):
 *   - WARNING: lag > 1,000  → log warning, metrics alert
 *   - CRITICAL: lag > 5,000 → adaptive rate limiting kicks in (50% reduction)
 *   - SEVERE: lag > 10,000  → aggressive throttle (90% reduction)
 *
 * This prevents cascading failure: when Kafka consumers can't keep up,
 * the API layer slows down ingestion rather than letting queues grow unbounded.
 */
@Service
public class BackpressureService {

    private static final Logger log = LoggerFactory.getLogger(BackpressureService.class);

    private final String bootstrapServers;
    private final String consumerGroupId;
    private final long warningThreshold;
    private final long criticalThreshold;
    private final long severeThreshold;

    private final AtomicLong totalLag = new AtomicLong(0);
    private final AtomicLong maxPartitionLag = new AtomicLong(0);

    private volatile AdminClient adminClient;

    public BackpressureService(
        @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
        @Value("${spring.kafka.consumer.group-id:payment-processor-group}") String consumerGroupId,
        @Value("${app.backpressure.warning-threshold:1000}") long warningThreshold,
        @Value("${app.backpressure.critical-threshold:5000}") long criticalThreshold,
        @Value("${app.backpressure.severe-threshold:10000}") long severeThreshold,
        MeterRegistry meterRegistry
    ) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        this.warningThreshold = warningThreshold;
        this.criticalThreshold = criticalThreshold;
        this.severeThreshold = severeThreshold;

        Gauge.builder("kafka_consumer_lag_total", totalLag, AtomicLong::doubleValue)
            .description("Total Kafka consumer group lag across all partitions")
            .tag("consumer_group", consumerGroupId)
            .register(meterRegistry);

        Gauge.builder("kafka_consumer_lag_max_partition", maxPartitionLag, AtomicLong::doubleValue)
            .description("Maximum lag on any single partition")
            .tag("consumer_group", consumerGroupId)
            .register(meterRegistry);
    }

    private AdminClient getOrCreateAdminClient() {
        if (adminClient == null) {
            synchronized (this) {
                if (adminClient == null) {
                    Properties props = new Properties();
                    props.put("bootstrap.servers", bootstrapServers);
                    props.put("request.timeout.ms", "5000");
                    props.put("default.api.timeout.ms", "5000");
                    adminClient = AdminClient.create(props);
                }
            }
        }
        return adminClient;
    }

    @PreDestroy
    public void shutdown() {
        if (adminClient != null) {
            try {
                adminClient.close();
            } catch (Exception e) {
                log.debug("Error closing Kafka AdminClient: {}", e.getMessage());
            }
        }
    }

    @Scheduled(fixedDelayString = "${app.backpressure.poll-interval-ms:5000}")
    public void pollConsumerLag() {
        try {
            AdminClient client = getOrCreateAdminClient();
            ListConsumerGroupOffsetsResult offsetsResult =
                client.listConsumerGroupOffsets(consumerGroupId);

            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                offsetsResult.partitionsToOffsetAndMetadata().get();

            Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                getEndOffsets(adminClient, committedOffsets);

            long total = 0;
            long maxLag = 0;

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committedOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                long committed = entry.getValue().offset();
                var endInfo = endOffsets.get(tp);
                if (endInfo != null) {
                    long end = endInfo.offset();
                    long lag = Math.max(0, end - committed);
                    total += lag;
                    maxLag = Math.max(maxLag, lag);
                }
            }

            totalLag.set(total);
            maxPartitionLag.set(maxLag);

            if (total >= severeThreshold) {
                log.error("SEVERE backpressure: Kafka lag={} (threshold={}). Aggressive throttling active.",
                    total, severeThreshold);
            } else if (total >= criticalThreshold) {
                log.warn("CRITICAL backpressure: Kafka lag={} (threshold={}). Rate limiting reduced.",
                    total, criticalThreshold);
            } else if (total >= warningThreshold) {
                log.warn("WARNING: Kafka consumer lag={} approaching critical threshold={}", total, criticalThreshold);
            }

        } catch (Exception e) {
            log.debug("Failed to poll Kafka consumer lag: {}", e.getMessage());
            // Reset the admin client if connection is broken so it can be recreated
            synchronized (this) {
                if (adminClient != null) {
                    try { adminClient.close(); } catch (Exception ignored) {}
                    adminClient = null;
                }
            }
        }
    }

    private Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> getEndOffsets(
        AdminClient adminClient,
        Map<TopicPartition, OffsetAndMetadata> committedOffsets
    ) throws Exception {
        Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> endOffsetRequest = new java.util.HashMap<>();
        for (TopicPartition tp : committedOffsets.keySet()) {
            endOffsetRequest.put(tp, org.apache.kafka.clients.admin.OffsetSpec.latest());
        }
        return adminClient.listOffsets(endOffsetRequest).all().get();
    }

    /**
     * Returns true when backpressure should be applied (lag exceeds critical threshold).
     */
    public boolean isBackpressureActive() {
        return totalLag.get() >= criticalThreshold;
    }

    /**
     * Returns the rate limit multiplier based on current lag.
     *   - Normal: 1.0 (full rate)
     *   - Critical: 0.5 (50% rate)
     *   - Severe: 0.1 (10% rate)
     */
    public double getRateLimitMultiplier() {
        long lag = totalLag.get();
        if (lag >= severeThreshold) return 0.1;
        if (lag >= criticalThreshold) return 0.5;
        return 1.0;
    }

    public long getTotalLag() {
        return totalLag.get();
    }

    public long getMaxPartitionLag() {
        return maxPartitionLag.get();
    }
}
