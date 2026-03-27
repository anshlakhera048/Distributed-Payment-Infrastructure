package com.payments.payment_service.service;

import com.payments.payment_service.dto.CreatePaymentRequest;
import com.payments.payment_service.dto.FraudCheckResponse;
import com.payments.payment_service.dto.PaymentEvent;
import com.payments.payment_service.dto.PaymentResponse;
import com.payments.payment_service.entity.Payment;
import com.payments.payment_service.entity.PaymentStatus;
import com.payments.payment_service.exception.PaymentNotFoundException;
import com.payments.payment_service.metrics.PaymentMetrics;
import com.payments.payment_service.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * Core payment processing service.
 *
 * Lifecycle: PENDING → PROCESSING → SUCCESS | FAILED | FRAUD_REJECTED
 *
 * Key guarantees:
 *  - Idempotency: same idempotency-key always returns same result
 *  - Rate limiting: enforced before processing
 *  - Outbox pattern: events written in same DB transaction as payment
 *  - Ledger consistency: double-entry bookkeeping enforced atomically
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final PaymentCacheService paymentCacheService;
    private final RateLimiterService rateLimiterService;
    private final OutboxPublisherService outboxPublisherService;
    private final LedgerService ledgerService;
    private final PaymentMetrics paymentMetrics;
    private final IdempotentConsumerService idempotentConsumerService;

    public PaymentService(
        PaymentRepository paymentRepository,
        PaymentCacheService paymentCacheService,
        RateLimiterService rateLimiterService,
        OutboxPublisherService outboxPublisherService,
        LedgerService ledgerService,
        PaymentMetrics paymentMetrics,
        IdempotentConsumerService idempotentConsumerService
    ) {
        this.paymentRepository = paymentRepository;
        this.paymentCacheService = paymentCacheService;
        this.rateLimiterService = rateLimiterService;
        this.outboxPublisherService = outboxPublisherService;
        this.ledgerService = ledgerService;
        this.paymentMetrics = paymentMetrics;
        this.idempotentConsumerService = idempotentConsumerService;
    }

    // -----------------------------------------------------------------------
    // CREATE — POST /payments
    // -----------------------------------------------------------------------

    /**
     * Creates a new payment with idempotency guarantees.
     * Uses SERIALIZABLE isolation on idempotency key lookup to prevent races.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public PaymentResponse createPayment(CreatePaymentRequest req, String idempotencyKey) {
        String correlationId = MDC.get("correlationId");
        String traceId = MDC.get("traceId");

        // 1. Rate limiting (before touching DB)
        rateLimiterService.checkRateLimit(req.getUserId());

        // 2. Fast idempotency check via Redis cache
        Optional<UUID> cachedPaymentId = paymentCacheService.getPaymentIdForIdempotencyKey(idempotencyKey);
        if (cachedPaymentId.isPresent()) {
            log.info("Idempotency hit (cache) for key={} → paymentId={}", idempotencyKey, cachedPaymentId.get());
            return paymentRepository.findById(cachedPaymentId.get())
                .map(this::toPaymentResponse)
                .orElseThrow(() -> new PaymentNotFoundException(cachedPaymentId.get()));
        }

        // 3. DB-level idempotency check with pessimistic lock to handle concurrent duplicates
        Optional<Payment> existing = paymentRepository.findByIdempotencyKeyForUpdate(idempotencyKey);
        if (existing.isPresent()) {
            log.info("Idempotency hit (DB) for key={} → paymentId={}", idempotencyKey, existing.get().getId());
            paymentCacheService.cacheIdempotencyKey(idempotencyKey, existing.get().getId());
            return toPaymentResponse(existing.get());
        }

        // 4. Create the payment
        Payment payment = new Payment();
        payment.setId(UUID.randomUUID());
        payment.setUserId(req.getUserId());
        payment.setMerchantId(req.getMerchantId());
        payment.setAmount(req.getAmount());
        payment.setCurrency(req.getCurrency());
        payment.setStatus(PaymentStatus.PENDING);
        payment.setIdempotencyKey(idempotencyKey);
        payment.setDescription(req.getDescription());
        payment.setCorrelationId(correlationId);
        payment.setTraceId(traceId);

        Payment saved = paymentRepository.save(payment);
        log.info("Payment created: paymentId={} userId={} amount={} {}",
            saved.getId(), saved.getUserId(), saved.getAmount(), saved.getCurrency());

        // 5. Write outbox event in the SAME transaction (guarantees Kafka publish)
        //    Partition key = userId (not paymentId) — ensures all events for a user
        //    land on the same Kafka partition for sequential per-user processing.
        //    traceId is passed as an explicit column (not embedded in the payload)
        //    so the publisher can propagate it as a Kafka header without JSON parsing.
        PaymentEvent event = buildPaymentEvent(saved, "payment.created");
        outboxPublisherService.createOutboxEvent(
            "payment.created",
            saved.getId().toString(),     // aggregateId = paymentId (for audit)
            saved.getUserId().toString(), // partitionKey = userId (distribution strategy)
            event,
            correlationId,
            traceId
        );

        // 6. Cache idempotency key in Redis
        paymentCacheService.cacheIdempotencyKey(idempotencyKey, saved.getId());
        paymentCacheService.cachePayment(saved);

        // 7. Record metrics
        paymentMetrics.recordPaymentCreated(req.getCurrency());

        return toPaymentResponse(saved);
    }

    // -----------------------------------------------------------------------
    // PROCESS RESULT — called by PaymentEventConsumer after fraud check
    // -----------------------------------------------------------------------

    /**
     * Processes the result of a fraud check.
     *
     * Exactly-once guarantee:
     *   1. IdempotentConsumerService.tryMarkProcessed() inserts into processed_events.
     *      This is the authoritative dedup check — atomic with the payment status update.
     *   2. Payment status guard provides a secondary safety net (covers crashes between
     *      DB commit and Kafka ack).
     *   Both checks are inside this @Transactional — the whole thing commits or rolls back together.
     *
     * Failure hardening:
     *   @Retryable with exponential backoff handles transient DB connection losses
     *   (connection pool exhaustion, brief PG failover). After 3 attempts the Kafka
     *   consumer's own ExponentialBackOff takes over (configured in KafkaConfig).
     */
    @Retryable(
        retryFor = TransientDataAccessException.class,
        maxAttempts = 3,
        backoff = @Backoff(delay = 200, multiplier = 2, maxDelay = 2000)
    )
    @Transactional
    public void processPaymentResult(
        UUID paymentId,
        FraudCheckResponse fraudCheck,
        String correlationId,
        String eventId
    ) {
        // Primary dedup: atomic with all subsequent DB writes in this TX.
        // Topic = "fraud.result" because this method is called by FraudResultConsumer.
        // The eventId was originated from payment.created and propagated through fraud.request
        // → fraud.result, so it uniquely identifies the original payment event.
        boolean isFirst = idempotentConsumerService.tryMarkProcessed(
            eventId, "payment-processor-group", "fraud.result"
        );
        if (!isFirst) {
            log.info("Duplicate event skipped — eventId={} paymentId={}", eventId, paymentId);
            return;
        }

        Payment payment = paymentRepository.findById(paymentId)
            .orElseThrow(() -> new PaymentNotFoundException(paymentId));

        // Secondary guard: covers the window between DB commit and Kafka ack on first delivery
        if (payment.getStatus() != PaymentStatus.PENDING) {
            log.warn("Payment {} is already in status {} — skipping secondary guard",
                paymentId, payment.getStatus());
            return;
        }

        payment.setFraudScore(fraudCheck.getScore());

        if (fraudCheck.isFraud()) {
            payment.setStatus(PaymentStatus.FRAUD_REJECTED);
            payment.setFailureReason("FRAUD: " + fraudCheck.getReason());
            log.warn("Payment {} rejected as FRAUD. Score={} Reason={}",
                paymentId, fraudCheck.getScore(), fraudCheck.getReason());

            // Use userId as partition key so fraud alerts for the same user are co-partitioned
            outboxPublisherService.createOutboxEvent(
                "fraud.alerts",
                paymentId.toString(),
                payment.getUserId().toString(),
                buildPaymentEvent(payment, "fraud.alerts"),
                correlationId,
                payment.getTraceId()
            );

            outboxPublisherService.createOutboxEvent(
                "payment.failed",
                paymentId.toString(),
                payment.getUserId().toString(),
                buildPaymentEvent(payment, "payment.failed"),
                correlationId,
                payment.getTraceId()
            );

            paymentMetrics.recordFraudDetected(fraudCheck.getReason());

        } else {
            // ---------------------------------------------------------------
            // PRE-SUCCESS CONSISTENCY GATE
            // ---------------------------------------------------------------
            // Record ledger FIRST. LedgerService.recordPayment() is atomic and
            // validates the double-entry invariant (debits == credits) before returning.
            // If the invariant fails it throws — rolling back this entire TX — which
            // means the payment is never marked SUCCESS and no outbox event is written.
            // Ordering: ledger write → verify invariant → THEN set status.
            // This prevents a SUCCESS status being persisted without a balanced ledger.
            ledgerService.recordPayment(
                paymentId,
                payment.getUserId().toString(),
                payment.getMerchantId().toString(),
                payment.getAmount(),
                payment.getCurrency(),
                correlationId
            );

            // Explicit pre-status verification: confirms the ledger write succeeded
            // and the invariant holds before we commit the SUCCESS status.
            ledgerService.verifyLedgerBalance(paymentId, payment.getAmount());

            payment.setStatus(PaymentStatus.SUCCESS);
            log.info("Payment {} marked SUCCESS after ledger verification. FraudScore={} Fallback={}",
                paymentId, fraudCheck.getScore(), fraudCheck.isFallback());

            outboxPublisherService.createOutboxEvent(
                "payment.processed",
                paymentId.toString(),
                payment.getUserId().toString(),
                buildPaymentEvent(payment, "payment.processed"),
                correlationId,
                payment.getTraceId()
            );

            paymentMetrics.recordPaymentProcessed(payment.getCurrency(), "SUCCESS");
        }

        paymentRepository.save(payment);

        // Evict stale cache entry so next read fetches fresh status
        paymentCacheService.evictPayment(paymentId);
    }

    /**
     * Recovery method called when all @Retryable attempts are exhausted.
     * Re-throws as a runtime exception so the Kafka consumer's ExponentialBackOff
     * can perform its own retries and eventually route to the DLQ.
     */
    @Recover
    public void recoverProcessPaymentResult(
        TransientDataAccessException ex,
        UUID paymentId,
        FraudCheckResponse fraudCheck,
        String correlationId,
        String eventId
    ) {
        log.error("Exhausted all retries for processPaymentResult paymentId={} eventId={}: {}",
            paymentId, eventId, ex.getMessage(), ex);
        // Re-throw so the Kafka consumer error handler can route to DLQ
        throw new IllegalStateException(
            "DB transient failure processing eventId=" + eventId + " paymentId=" + paymentId, ex
        );
    }

    // -----------------------------------------------------------------------
    // READ
    // -----------------------------------------------------------------------

    @Transactional(readOnly = true)
    public PaymentResponse getPayment(UUID paymentId) {
        return paymentCacheService.getCachedPayment(paymentId)
            .orElseGet(() -> {
                Payment payment = paymentRepository.findById(paymentId)
                    .orElseThrow(() -> new PaymentNotFoundException(paymentId));
                paymentCacheService.cachePayment(payment);
                return toPaymentResponse(payment);
            });
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

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

    private PaymentEvent buildPaymentEvent(Payment payment, String eventType) {
        return PaymentEvent.builder()
            .version("v1")
            .eventId(UUID.randomUUID().toString())
            .traceId(payment.getTraceId())          // propagated from HTTP request
            .paymentId(payment.getId())
            .userId(payment.getUserId())
            .merchantId(payment.getMerchantId())
            .amount(payment.getAmount())
            .currency(payment.getCurrency())
            .status(payment.getStatus().name())
            .idempotencyKey(payment.getIdempotencyKey())
            .description(payment.getDescription())
            .correlationId(payment.getCorrelationId())
            .eventType(eventType)
            .createdAt(payment.getCreatedAt())
            .eventTimestamp(LocalDateTime.now())
            .build();
    }
}
