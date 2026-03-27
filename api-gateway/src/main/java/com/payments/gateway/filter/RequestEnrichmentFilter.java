package com.payments.gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Global filter that runs on every proxied request (after JWT validation).
 *
 * Responsibilities:
 *  1. Propagate (or generate) a correlation ID as {@code X-Correlation-ID} header.
 *  2. Forward the authenticated subject claim ({@code sub}) as {@code X-User-Id}.
 *  3. Strip the raw Authorization header before forwarding to downstream services
 *     (downstream services trust X-User-Id injected by the gateway only).
 *
 * Order {@code -1} ensures this runs before routing filters.
 */
@Component
public class RequestEnrichmentFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(RequestEnrichmentFilter.class);
    private static final String CORRELATION_HEADER  = "X-Correlation-ID";
    private static final String USER_ID_HEADER      = "X-User-Id";

    @Override
    public int getOrder() {
        return -1;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        return ReactiveSecurityContextHolder.getContext()
            .map(ctx -> {
                Object principal = ctx.getAuthentication().getPrincipal();
                String sub = (principal instanceof Jwt jwt) ? jwt.getSubject() : "unknown";

                String correlationId = exchange.getRequest().getHeaders()
                    .getFirst(CORRELATION_HEADER);
                if (correlationId == null || correlationId.isBlank()) {
                    correlationId = UUID.randomUUID().toString();
                }

                log.debug("Gateway routing sub={} correlationId={} path={}",
                    sub, correlationId, exchange.getRequest().getPath());

                final String finalCorrelationId = correlationId;
                final String finalSub = sub;

                ServerHttpRequest mutatedRequest = exchange.getRequest().mutate()
                    .header(CORRELATION_HEADER, finalCorrelationId)
                    .header(USER_ID_HEADER, finalSub)
                    .build();

                return exchange.mutate().request(mutatedRequest).build();
            })
            // If no security context (e.g., permitted endpoints), just pass through
            .defaultIfEmpty(exchange)
            .flatMap(chain::filter);
    }
}
