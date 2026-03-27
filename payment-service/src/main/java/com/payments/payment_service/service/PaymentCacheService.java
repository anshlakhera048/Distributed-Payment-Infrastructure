package com.payments.payment_service.service;

import com.payments.payment_service.dto.PaymentResponse;
import com.payments.payment_service.entity.Payment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Redis-backed cache for idempotency key lookups and payment responses.
 * Separates caching concerns from business logic.
 */
@Service
public class PaymentCacheService {

    private static final Logger log = LoggerFactory.getLogger(PaymentCacheService.class);

    private static final String IDEMPOTENCY_KEY_PREFIX = "idem:";
    private static final String PAYMENT_CACHE_PREFIX = "payment:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);
    private static final Duration PAYMENT_CACHE_TTL = Duration.ofMinutes(30);

    private final RedisTemplate<String, Object> redisTemplate;

    public PaymentCacheService(@Qualifier("redisTemplate") RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void cacheIdempotencyKey(String idempotencyKey, UUID paymentId) {
        String redisKey = IDEMPOTENCY_KEY_PREFIX + idempotencyKey;
        redisTemplate.opsForValue().set(redisKey, paymentId.toString(), IDEMPOTENCY_TTL);
        log.debug("Cached idempotency key={} → paymentId={}", idempotencyKey, paymentId);
    }

    public Optional<UUID> getPaymentIdForIdempotencyKey(String idempotencyKey) {
        String redisKey = IDEMPOTENCY_KEY_PREFIX + idempotencyKey;
        Object value = redisTemplate.opsForValue().get(redisKey);
        if (value == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(UUID.fromString(value.toString()));
        } catch (IllegalArgumentException e) {
            log.warn("Invalid UUID in Redis for key={}: {}", redisKey, value);
            return Optional.empty();
        }
    }

    public void cachePayment(Payment payment) {
        String redisKey = PAYMENT_CACHE_PREFIX + payment.getId();
        redisTemplate.opsForValue().set(redisKey, toPaymentResponse(payment), PAYMENT_CACHE_TTL);
    }

    public Optional<PaymentResponse> getCachedPayment(UUID paymentId) {
        String redisKey = PAYMENT_CACHE_PREFIX + paymentId;
        Object value = redisTemplate.opsForValue().get(redisKey);
        if (value instanceof PaymentResponse) {
            return Optional.of((PaymentResponse) value);
        }
        return Optional.empty();
    }

    public void evictPayment(UUID paymentId) {
        redisTemplate.delete(PAYMENT_CACHE_PREFIX + paymentId);
    }

    private PaymentResponse toPaymentResponse(Payment payment) {
        return PaymentResponse.builder()
            .paymentId(payment.getId())
            .userId(payment.getUserId())
            .merchantId(payment.getMerchantId())
            .amount(payment.getAmount())
            .currency(payment.getCurrency())
            .status(payment.getStatus().name())
            .idempotencyKey(payment.getIdempotencyKey())
            .description(payment.getDescription())
            .createdAt(payment.getCreatedAt())
            .build();
    }
}
