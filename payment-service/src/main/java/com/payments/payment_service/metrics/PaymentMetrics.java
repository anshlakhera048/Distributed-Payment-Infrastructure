package com.payments.payment_service.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Centralised Prometheus metrics for the payment service.
 * All metrics are exposed at /actuator/prometheus and scraped by Prometheus.
 *
 * Metrics defined:
 *  - payments_created_total        (counter, tags: currency)
 *  - payments_processed_total      (counter, tags: currency, result)
 *  - fraud_detected_total          (counter, tags: reason)
 *  - payment_processing_seconds    (timer)
 *  - outbox_events_published_total (counter)
 *  - outbox_events_failed_total    (counter)
 *  - account_balance_updates_total (counter, tags: type)
 *  - backpressure_activations_total(counter)
 *  - active_payments_pending       (gauge)
 */
@Component
public class PaymentMetrics {

    private final MeterRegistry meterRegistry;
    private final AtomicLong pendingPayments = new AtomicLong(0);

    public PaymentMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        Gauge.builder("active_payments_pending", pendingPayments, AtomicLong::doubleValue)
            .description("Number of payments currently in PENDING status")
            .register(meterRegistry);
    }

    public void recordPaymentCreated(String currency) {
        Counter.builder("payments_created_total")
            .description("Total number of payments created")
            .tag("currency", currency)
            .register(meterRegistry)
            .increment();
        pendingPayments.incrementAndGet();
    }

    public void recordPaymentProcessed(String currency, String result) {
        Counter.builder("payments_processed_total")
            .description("Total number of payments processed")
            .tag("currency", currency)
            .tag("result", result)
            .register(meterRegistry)
            .increment();
        pendingPayments.updateAndGet(current -> Math.max(0, current - 1));
    }

    public void recordFraudDetected(String reason) {
        Counter.builder("fraud_detected_total")
            .description("Total number of fraud events detected")
            .tag("reason", reason)
            .register(meterRegistry)
            .increment();
    }

    public Timer.Sample startPaymentTimer() {
        return Timer.start(meterRegistry);
    }

    public void stopPaymentTimer(Timer.Sample sample, String status) {
        sample.stop(Timer.builder("payment_processing_seconds")
            .description("Time to process a payment end-to-end")
            .tag("status", status)
            .register(meterRegistry));
    }

    public void recordOutboxPublished() {
        Counter.builder("outbox_events_published_total")
            .description("Total outbox events successfully published to Kafka")
            .register(meterRegistry)
            .increment();
    }

    public void recordOutboxFailed() {
        Counter.builder("outbox_events_failed_total")
            .description("Total outbox events that exhausted retries")
            .register(meterRegistry)
            .increment();
    }

    public void recordAccountBalanceUpdate(String type) {
        Counter.builder("account_balance_updates_total")
            .description("Total account balance updates")
            .tag("type", type)
            .register(meterRegistry)
            .increment();
    }

    public void recordBackpressureActivation() {
        Counter.builder("backpressure_activations_total")
            .description("Total times backpressure was activated")
            .register(meterRegistry)
            .increment();
    }
}
