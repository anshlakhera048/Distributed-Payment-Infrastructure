package com.payments.payment_service.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/**
 * Centralised Prometheus metrics for the payment service.
 * All metrics are exposed at /actuator/prometheus and scraped by Prometheus.
 *
 * Metrics defined:
 *  - payments_created_total        (counter, tags: currency)
 *  - payments_processed_total      (counter, tags: currency, result)
 *  - fraud_detected_total          (counter, tags: reason)
 *  - payment_processing_seconds    (timer)
 */
@Component
public class PaymentMetrics {

    private final MeterRegistry meterRegistry;

    public PaymentMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void recordPaymentCreated(String currency) {
        Counter.builder("payments_created_total")
            .description("Total number of payments created")
            .tag("currency", currency)
            .register(meterRegistry)
            .increment();
    }

    public void recordPaymentProcessed(String currency, String result) {
        Counter.builder("payments_processed_total")
            .description("Total number of payments processed")
            .tag("currency", currency)
            .tag("result", result)
            .register(meterRegistry)
            .increment();
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
}
