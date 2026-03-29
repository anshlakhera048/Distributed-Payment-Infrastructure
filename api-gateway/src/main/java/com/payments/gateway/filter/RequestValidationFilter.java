package com.payments.gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Global request validation filter for the API Gateway.
 *
 * Validates:
 *   1. POST requests have Content-Type: application/json
 *   2. POST /payments requests include Idempotency-Key header
 *   3. Request body size does not exceed 1MB
 *
 * Runs before routing (order = -2, before RequestEnrichmentFilter at -1).
 * Rejects invalid requests early at the gateway level before they reach
 * downstream services.
 */
@Component
public class RequestValidationFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(RequestValidationFilter.class);
    private static final String IDEMPOTENCY_HEADER = "Idempotency-Key";
    private static final long MAX_CONTENT_LENGTH = 1_048_576; // 1MB

    @Override
    public int getOrder() {
        return -2;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        HttpMethod method = exchange.getRequest().getMethod();

        // Validate Content-Type for POST requests
        if (method == HttpMethod.POST) {
            MediaType contentType = exchange.getRequest().getHeaders().getContentType();
            if (contentType == null || !contentType.isCompatibleWith(MediaType.APPLICATION_JSON)) {
                log.warn("Rejected: POST request without application/json Content-Type on path={}", path);
                exchange.getResponse().setStatusCode(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
                return exchange.getResponse().setComplete();
            }
        }

        // Validate Idempotency-Key for payment creation
        // Match exact payment creation paths: /payments, /api/payments, /v1/payments
        if (method == HttpMethod.POST && path.matches("^(/api|/v1)?/payments/?$")) {
            String idempotencyKey = exchange.getRequest().getHeaders().getFirst(IDEMPOTENCY_HEADER);
            if (idempotencyKey == null || idempotencyKey.isBlank()) {
                log.warn("Rejected: POST /payments without Idempotency-Key header");
                exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
                return exchange.getResponse().setComplete();
            }
        }

        // Validate content length
        long contentLength = exchange.getRequest().getHeaders().getContentLength();
        if (contentLength > MAX_CONTENT_LENGTH) {
            log.warn("Rejected: Request body too large ({}B > {}B)", contentLength, MAX_CONTENT_LENGTH);
            exchange.getResponse().setStatusCode(HttpStatus.PAYLOAD_TOO_LARGE);
            return exchange.getResponse().setComplete();
        }

        return chain.filter(exchange);
    }
}
