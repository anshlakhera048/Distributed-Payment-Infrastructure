package com.payments.gateway.config;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

/**
 * Rate limiter key resolver configuration.
 *
 * Uses the client's remote IP address as the rate limiting key.
 * X-Forwarded-For is intentionally NOT trusted to prevent client IP spoofing.
 * If deployed behind a trusted reverse proxy, configure the proxy to set
 * a verified header and update this resolver accordingly.
 */
@Configuration
public class RateLimiterConfig {

    @Bean
    public KeyResolver remoteAddrKeyResolver() {
        return exchange -> {
            InetSocketAddress remoteAddr = exchange.getRequest().getRemoteAddress();
            String ip = remoteAddr != null ? remoteAddr.getAddress().getHostAddress() : "unknown";
            return Mono.just(ip);
        };
    }
}
