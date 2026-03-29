package com.payments.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.jwt.NimbusReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.web.server.SecurityWebFilterChain;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

/**
 * Security configuration for the API Gateway.
 *
 * All routes except /actuator/health require a valid JWT bearer token.
 * Tokens are validated locally using an HMAC-SHA256 (HS256) symmetric key —
 * no external auth server roundtrip on every request.
 *
 * Production note: rotate the JWT secret via environment variable and
 * redeploy both the token issuer and this gateway. For asymmetric key (RS256)
 * set {@code app.jwt.jwk-set-uri} and use NimbusReactiveJwtDecoder.withJwkSetUri().
 */
@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    /**
     * HS256 secret — minimum 32 characters (256 bits).
     * Set via environment variable GATEWAY_JWT_SECRET in production.
     */
    @Value("${app.jwt.secret}")
    private String jwtSecret;

    @Bean
    public SecurityWebFilterChain springSecurityFilterChain(ServerHttpSecurity http) {
        return http
            .csrf(ServerHttpSecurity.CsrfSpec::disable)
            .authorizeExchange(exchanges -> exchanges
                // Actuator probes — accessible without authentication
                .pathMatchers("/actuator/health", "/actuator/info", "/actuator/prometheus").permitAll()
                // All other paths require a valid JWT
                .anyExchange().authenticated()
            )
            .oauth2ResourceServer(oauth2 -> oauth2
                .jwt(jwt -> jwt.jwtDecoder(jwtDecoder()))
            )
            .build();
    }

    @Bean
    public ReactiveJwtDecoder jwtDecoder() {
        byte[] keyBytes = jwtSecret.getBytes(StandardCharsets.UTF_8);
        SecretKey key = new SecretKeySpec(keyBytes, "HmacSHA256");
        return NimbusReactiveJwtDecoder.withSecretKey(key).build();
    }
}
