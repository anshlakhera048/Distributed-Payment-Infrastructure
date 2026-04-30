package com.payments.payment_service.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.payments.payment_service.exception.RateLimitExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Fixed-window rate limiter using Redis + Lua script.
 * Each key is allowed N requests per 60-second tumbling window.
 * Lua script ensures atomicity of the check-and-increment operation.
 *
 * DEFENSIVE FALLBACK:
 * If Redis is unavailable ({@link RedisConnectionFailureException} or any
 * {@link RuntimeException} during the script execution), the limiter falls back
 * to a per-JVM in-memory sliding window backed by a bounded Caffeine cache.
 *
 * <p>MEMORY SAFETY: the fallback cache is capped at {@value #FALLBACK_MAX_SIZE} entries
 * and each entry expires after {@value #FALLBACK_ENTRY_TTL_MINUTES} minutes (2× the
 * window). Even under aggressive cardinality attacks (millions of distinct userIds),
 * the cache evicts by LRU once the cap is reached — preventing unbounded heap growth.
 *
 * <p>Trade-off: in-memory state is not shared across instances. During Redis
 * unavailability, each pod enforces its own limit, so the effective cluster-wide
 * limit = requestsPerMinute × podCount. This is acceptable for a degraded-mode
 * fallback — the alternative (fail open) is worse for fraud exposure.
 */
@Service
public class RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);

    private static final String RATE_LIMIT_PREFIX = "rate_limit:";
    private static final long   WINDOW_MS              = 60_000L; // 60-second window
    private static final int    FALLBACK_MAX_SIZE       = 50_000;  // max unique userIds in cache
    private static final int    FALLBACK_ENTRY_TTL_MINUTES = 2;   // 2 × window = safe expiry

    /**
     * Atomic Lua script for fixed-window counter increment.
     * Returns 1 if under limit (request allowed), 0 if over limit.
     */
    private static final String RATE_LIMIT_SCRIPT =
        "local key = KEYS[1] " +
        "local limit = tonumber(ARGV[1]) " +
        "local window = tonumber(ARGV[2]) " +
        "local current = redis.call('INCR', key) " +
        "if current == 1 then redis.call('EXPIRE', key, window) end " +
        "if current > limit then return 0 else return 1 end";

    /**
     * In-memory fallback cache: userId → [count, windowStartEpochMs].
     *
     * Caffeine provides:
     *   - {@code maximumSize} cap — LRU eviction prevents heap exhaustion under
     *     high-cardinality attacks (millions of unique userIds).
     *   - {@code expireAfterWrite} — entries are automatically removed after 2 minutes
     *     so memory is reclaimed once Redis recovers and the fallback is no longer needed.
     *
     * This replaces the previous raw ConcurrentHashMap which had no eviction mechanism
     * and could grow without bound during sustained Redis outages.
     */
    private final Cache<String, long[]> inMemoryCounters = Caffeine.newBuilder()
        .maximumSize(FALLBACK_MAX_SIZE)
        .expireAfterWrite(Duration.ofMinutes(FALLBACK_ENTRY_TTL_MINUTES))
        .build();

    private final RedisTemplate<String, Object> redisTemplate;
    private final boolean rateLimitingEnabled;
    private final int requestsPerMinute;
    private final BackpressureService backpressureService;

    public RateLimiterService(
        @Qualifier("redisTemplate") RedisTemplate<String, Object> redisTemplate,
        @Value("${app.rate-limiting.enabled}") boolean rateLimitingEnabled,
        @Value("${app.rate-limiting.requests-per-minute}") int requestsPerMinute,
        BackpressureService backpressureService
    ) {
        this.redisTemplate = redisTemplate;
        this.rateLimitingEnabled = rateLimitingEnabled;
        this.requestsPerMinute = requestsPerMinute;
        this.backpressureService = backpressureService;
    }

    /**
     * Checks the rate limit for the given userId.
     * Applies adaptive throttling based on Kafka consumer lag:
     *   - Normal: full requestsPerMinute
     *   - Critical lag: 50% of requestsPerMinute
     *   - Severe lag: 10% of requestsPerMinute
     * Tries Redis first; falls back to in-memory on any Redis failure.
     * Throws {@link RateLimitExceededException} if the limit is exceeded.
     */
    public void checkRateLimit(UUID userId) {
        if (!rateLimitingEnabled) {
            return;
        }

        try {
            checkRedisRateLimit(userId);
        } catch (RedisConnectionFailureException e) {
            log.warn("Redis unavailable for rate limiting ({}), using in-memory fallback for userId={}",
                e.getMessage(), userId);
            checkInMemoryRateLimit(userId);
        } catch (RuntimeException e) {
            log.warn("Redis rate limit check failed ({}), using in-memory fallback for userId={}",
                e.getClass().getSimpleName(), userId);
            checkInMemoryRateLimit(userId);
        }
    }

    /**
     * Returns the effective rate limit after applying backpressure multiplier.
     */
    private int getEffectiveRateLimit() {
        double multiplier = backpressureService.getRateLimitMultiplier();
        int effective = Math.max(1, (int) (requestsPerMinute * multiplier));
        if (multiplier < 1.0) {
            log.debug("Adaptive rate limit active: base={} multiplier={} effective={}",
                requestsPerMinute, multiplier, effective);
        }
        return effective;
    }

    /**
     * Redis-backed sliding window check. Calls atomic Lua script.
     */
    private void checkRedisRateLimit(UUID userId) {
        String key = RATE_LIMIT_PREFIX + userId.toString();
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(RATE_LIMIT_SCRIPT, Long.class);
        List<String> keys = Collections.singletonList(key);
        int effectiveLimit = getEffectiveRateLimit();

        Long result = redisTemplate.execute(script, keys,
            String.valueOf(effectiveLimit),
            "60"
        );

        if (result == null || result == 0L) {
            log.warn("Rate limit exceeded (Redis) for userId={} effectiveLimit={}", userId, effectiveLimit);
            throw new RateLimitExceededException(
                "Rate limit exceeded. Max " + effectiveLimit + " requests per minute."
            );
        }
    }

    /**
     * In-memory sliding window fallback (single-JVM, not shared across pods).
     * Uses a fixed 60-second tumbling window reset per userId.
     *
     * Thread-safety: Caffeine's {@code get(key, mappingFunction)} is atomic per key
     * (backed by ConcurrentHashMap internally). The compute block runs at most once
     * concurrently per key, eliminating the race condition that could occur between
     * reading and updating a raw ConcurrentHashMap entry.
     */
    private void checkInMemoryRateLimit(UUID userId) {
        String key = userId.toString();
        long now = System.currentTimeMillis();
        int effectiveLimit = getEffectiveRateLimit();

        long[] state = inMemoryCounters.asMap().compute(key, (k, existing) -> {
            if (existing == null || now - existing[1] >= WINDOW_MS) {
                return new long[]{1L, now};
            }
            existing[0]++;
            return existing;
        });

        if (state[0] > effectiveLimit) {
            log.warn("Rate limit exceeded (in-memory fallback) for userId={} count={} effectiveLimit={}",
                userId, state[0], effectiveLimit);
            throw new RateLimitExceededException(
                "Rate limit exceeded. Max " + effectiveLimit + " requests per minute."
            );
        }
    }
}
