package com.payments.gateway.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Fallback controller for circuit breaker responses.
 *
 * When the downstream payment-service circuit is open, Spring Cloud Gateway
 * forwards failed requests here instead of returning a raw 502.
 */
@RestController
@RequestMapping("/fallback")
public class FallbackController {

    private static final Logger log = LoggerFactory.getLogger(FallbackController.class);

    @GetMapping("/payment-service")
    @PostMapping("/payment-service")
    public Mono<Map<String, Object>> paymentServiceFallback() {
        log.warn("Circuit breaker open — returning fallback response for payment-service");
        return Mono.error(new ResponseStatusException(
            HttpStatus.SERVICE_UNAVAILABLE,
            "Payment service is temporarily unavailable. Please retry shortly."
        ));
    }
}
