package com.payments.payment_service.service;

import com.payments.payment_service.dto.FraudCheckRequest;
import com.payments.payment_service.dto.FraudCheckResponse;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;

/**
 * HTTP client for the Python fraud-detection microservice.
 *
 * Resilience strategy:
 *  - Circuit breaker: opens after 50% failures in a 10-call window
 *  - Timeout: 5 seconds per request
 *  - Fallback: allow the payment through (open-by-default policy — fail safe)
 */
@Service
public class FraudServiceClient {

    private static final Logger log = LoggerFactory.getLogger(FraudServiceClient.class);

    private final WebClient webClient;

    public FraudServiceClient(@Qualifier("fraudServiceWebClient") WebClient webClient) {
        this.webClient = webClient;
    }

    @CircuitBreaker(name = "fraudService", fallbackMethod = "fallbackCheck")
    public FraudCheckResponse check(FraudCheckRequest request) {
        log.debug("Calling fraud-service for paymentId={}", request.getPaymentId());

        return webClient.post()
            .uri("/score")
            .bodyValue(request)
            .retrieve()
            .bodyToMono(FraudCheckResponse.class)
            .timeout(Duration.ofSeconds(5))
            .doOnError(ex -> log.error("Fraud service call failed for paymentId={}: {}",
                request.getPaymentId(), ex.getMessage()))
            .block();
    }

    /**
     * Fallback method — called when circuit is open or the service returns 5xx/timeout.
     * Policy: allow payment to proceed (fail-open) to avoid blocking legitimate payments.
     * Fraud score is set to 0.5 (neutral) to flag for async review.
     */
    @SuppressWarnings("unused")
    public FraudCheckResponse fallbackCheck(FraudCheckRequest request, Throwable cause) {
        log.warn("Fraud service unavailable (circuit open or timeout) for paymentId={}. " +
                 "Applying fail-open fallback. Cause: {}", request.getPaymentId(), cause.getMessage());

        return FraudCheckResponse.builder()
            .paymentId(request.getPaymentId())
            .fraud(false)
            .reason("fraud_service_unavailable")
            .score(0.5)
            .fallback(true)
            .build();
    }
}
