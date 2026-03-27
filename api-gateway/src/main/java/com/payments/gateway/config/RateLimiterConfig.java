package com.payments.gateway.config;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Rate limiter key resolver configuration.
 *
 * The {@code remoteAddrKeyResolver} bean is referenced in application.yml
 * by the RequestRateLimiter filter: {@code key-resolver: "#{@remoteAddrKeyResolver}"}.
 *
 * Uses the client's remote IP address as the rate limiting key.
 * If the gateway is behind a load balancer, extract from X-Forwarded-For instead.
 */
@Configuration
public class RateLimiterConfig {

    @Bean
    public KeyResolver remoteAddrKeyResolver() {
        return (ServerWebExchange exchange) -> {
            // Prefer X-Forwarded-For when behind a load balancer
            String xff = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
            if (xff != null && !xff.isBlank()) {
                // Take the first IP (original client) from the comma-delimited list
                return Mono.just(xff.split(",")[0].trim());
            }
            InetSocketAddress remoteAddr = exchange.getRequest().getRemoteAddress();
            String ip = remoteAddr != null ? remoteAddr.getAddress().getHostAddress() : "unknown";
            return Mono.just(ip);
        };
    }
}
