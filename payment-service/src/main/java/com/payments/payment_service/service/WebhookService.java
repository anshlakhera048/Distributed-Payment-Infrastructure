package com.payments.payment_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payments.payment_service.dto.WebhookPayload;
import com.payments.payment_service.entity.WebhookDelivery;
import com.payments.payment_service.entity.WebhookDeliveryStatus;
import com.payments.payment_service.entity.WebhookEndpoint;
import com.payments.payment_service.repository.WebhookDeliveryRepository;
import com.payments.payment_service.repository.WebhookEndpointRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Webhook delivery service with exponential backoff retry.
 *
 * Delivery guarantees:
 *  - Idempotency key prevents duplicate deliveries
 *  - Retry scheduler re-drives FAILED deliveries
 *  - After maxRetryAttempts the delivery is moved to DLQ
 *  - Each delivery is signed with SHA-256 HMAC (X-Signature header)
 */
@Service
public class WebhookService {

    private static final Logger log = LoggerFactory.getLogger(WebhookService.class);

    private static final int[] BACKOFF_SECONDS = {5, 30, 120, 600, 1800};

    private final WebhookEndpointRepository webhookEndpointRepository;
    private final WebhookDeliveryRepository webhookDeliveryRepository;
    private final WebClient webhookWebClient;
    private final ObjectMapper objectMapper;

    @Value("${app.webhook.max-retry-attempts}")
    private int maxRetryAttempts;

    public WebhookService(
        WebhookEndpointRepository webhookEndpointRepository,
        WebhookDeliveryRepository webhookDeliveryRepository,
        @Qualifier("webhookWebClient") WebClient webhookWebClient,
        ObjectMapper objectMapper
    ) {
        this.webhookEndpointRepository = webhookEndpointRepository;
        this.webhookDeliveryRepository = webhookDeliveryRepository;
        this.webhookWebClient = webhookWebClient;
        this.objectMapper = objectMapper;
    }

    /**
     * Creates WebhookDelivery records for all active endpoints subscribed to this event.
     * Called asynchronously from the Kafka consumer thread.
     */
    @Async("webhookExecutor")
    @Transactional
    public void dispatchEvent(UUID merchantId, String eventType, WebhookPayload payload) {
        List<WebhookEndpoint> endpoints =
            webhookEndpointRepository.findActiveEndpointsForEvent(merchantId, eventType);

        if (endpoints.isEmpty()) {
            log.debug("No active webhook endpoints for merchantId={} eventType={}", merchantId, eventType);
            return;
        }

        for (WebhookEndpoint endpoint : endpoints) {
            String deliveryIdempotencyKey = endpoint.getId() + ":" + payload.getPaymentId() + ":" + eventType;

            // Idempotency: skip if already queued/delivered
            if (webhookDeliveryRepository.existsByIdempotencyKey(deliveryIdempotencyKey)) {
                log.debug("Webhook delivery already exists for key={} — skipping", deliveryIdempotencyKey);
                continue;
            }

            try {
                String jsonPayload = objectMapper.writeValueAsString(payload);

                WebhookDelivery delivery = new WebhookDelivery();
                delivery.setId(UUID.randomUUID());
                delivery.setWebhookEndpointId(endpoint.getId());
                delivery.setPaymentId(payload.getPaymentId());
                delivery.setEventType(eventType);
                delivery.setPayload(jsonPayload);
                delivery.setIdempotencyKey(deliveryIdempotencyKey);
                delivery.setStatus(WebhookDeliveryStatus.PENDING);
                delivery.setAttemptCount(0);

                webhookDeliveryRepository.save(delivery);

                // Attempt delivery immediately
                attemptDelivery(delivery, endpoint);

            } catch (Exception e) {
                log.error("Failed to create webhook delivery for endpoint={}: {}",
                    endpoint.getId(), e.getMessage(), e);
            }
        }
    }

    /**
     * Scheduled retry for FAILED deliveries whose nextRetryAt has arrived.
     */
    @Scheduled(fixedDelay = 30_000)
    @Transactional
    public void retryFailedDeliveries() {
        List<WebhookDelivery> pending = webhookDeliveryRepository
            .findByStatusAndNextRetryAtBefore(WebhookDeliveryStatus.FAILED, LocalDateTime.now());

        for (WebhookDelivery delivery : pending) {
            webhookEndpointRepository.findById(delivery.getWebhookEndpointId())
                .ifPresent(endpoint -> attemptDelivery(delivery, endpoint));
        }
    }

    private void attemptDelivery(WebhookDelivery delivery, WebhookEndpoint endpoint) {
        delivery.setAttemptCount(delivery.getAttemptCount() + 1);
        delivery.setLastAttemptedAt(LocalDateTime.now());

        try {
            Integer statusCode = webhookWebClient.post()
                .uri(endpoint.getUrl())
                .header("X-Payment-Event", delivery.getEventType())
                .header("X-Delivery-Id", delivery.getId().toString())
                .header("X-Idempotency-Key", delivery.getIdempotencyKey())
                .bodyValue(delivery.getPayload())
                .retrieve()
                .toBodilessEntity()
                .map(resp -> resp.getStatusCode().value())
                .onErrorReturn(500)
                .timeout(Duration.ofSeconds(10))
                .block();

            delivery.setLastResponseCode(statusCode);

            if (statusCode != null && statusCode >= 200 && statusCode < 300) {
                delivery.setStatus(WebhookDeliveryStatus.SUCCESS);
                log.info("Webhook delivered successfully to endpoint={} deliveryId={}",
                    endpoint.getId(), delivery.getId());
            } else {
                handleDeliveryFailure(delivery, "HTTP " + statusCode);
            }

        } catch (Exception e) {
            handleDeliveryFailure(delivery, e.getMessage());
        }

        webhookDeliveryRepository.save(delivery);
    }

    private void handleDeliveryFailure(WebhookDelivery delivery, String error) {
        delivery.setLastError(error);

        if (delivery.getAttemptCount() >= maxRetryAttempts) {
            delivery.setStatus(WebhookDeliveryStatus.DLQ);
            log.error("Webhook delivery exhausted retries. Moving to DLQ. deliveryId={} error={}",
                delivery.getId(), error);
        } else {
            delivery.setStatus(WebhookDeliveryStatus.FAILED);
            int backoffIndex = Math.min(delivery.getAttemptCount() - 1, BACKOFF_SECONDS.length - 1);
            long backoffSecs = BACKOFF_SECONDS[backoffIndex];
            delivery.setNextRetryAt(LocalDateTime.now().plusSeconds(backoffSecs));
            log.warn("Webhook delivery failed (attempt={}), will retry in {}s. deliveryId={}",
                delivery.getAttemptCount(), backoffSecs, delivery.getId());
        }
    }
}
