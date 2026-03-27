package com.payments.payment_service.service;

import com.payments.payment_service.entity.PaymentStatus;
import com.payments.payment_service.repository.LedgerEntryRepository;
import com.payments.payment_service.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Periodic reconciliation service.
 *
 * Checks for:
 *  1. Payments stuck in PENDING beyond the threshold (stale payments)
 *  2. SUCCESS payments with no ledger entries (ghost payments)
 *  3. Duplicate ledger entries — more than 2 entries per payment (DEBIT + CREDIT = 2 expected)
 *  4. Ledger debit/credit imbalance — double-entry invariant violation
 *  5. Orphan ledger entries — ledger rows with no matching Payment record
 *
 * Emits reconciliation alerts to Kafka so they can be monitored/acted upon.
 */
@Service
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

    private final PaymentRepository paymentRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.reconciliation.stale-payment-threshold-minutes}")
    private int stalePaymentThresholdMinutes;

    public ReconciliationService(
        PaymentRepository paymentRepository,
        LedgerEntryRepository ledgerEntryRepository,
        KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.paymentRepository = paymentRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(cron = "${app.reconciliation.cron}")
    @Transactional(readOnly = true)
    public void reconcile() {
        String jobId = UUID.randomUUID().toString();
        MDC.put("reconciliationJobId", jobId);

        log.info("Reconciliation job started. jobId={}", jobId);

        AtomicInteger staleCount        = new AtomicInteger();
        AtomicInteger ghostCount        = new AtomicInteger();
        AtomicInteger duplicateCount    = new AtomicInteger();
        AtomicInteger imbalanceCount    = new AtomicInteger();
        AtomicInteger orphanCount       = new AtomicInteger();

        try {
            // ----------------------------------------------------------------
            // 1. Stale PENDING payments
            // ----------------------------------------------------------------
            LocalDateTime staleThreshold = LocalDateTime.now().minusMinutes(stalePaymentThresholdMinutes);
            paymentRepository
                .findByStatusAndCreatedAtBefore(PaymentStatus.PENDING, staleThreshold)
                .forEach(payment -> {
                    staleCount.incrementAndGet();
                    log.warn("RECONCILIATION[STALE_PAYMENT]: paymentId={} createdAt={}",
                        payment.getId(), payment.getCreatedAt());
                    emitAlert("STALE_PAYMENT", payment.getId().toString(),
                        "Payment stuck in PENDING for over " + stalePaymentThresholdMinutes + " minutes");
                });

            // ----------------------------------------------------------------
            // 2. Ghost payments: SUCCESS with no ledger entries
            // ----------------------------------------------------------------
            paymentRepository
                .findByStatusAndCreatedAtBefore(PaymentStatus.SUCCESS, LocalDateTime.now())
                .forEach(payment -> {
                    if (!ledgerEntryRepository.existsByPaymentId(payment.getId())) {
                        ghostCount.incrementAndGet();
                        log.error("RECONCILIATION[GHOST_PAYMENT]: paymentId={} — SUCCESS but no ledger entries",
                            payment.getId());
                        emitAlert("GHOST_PAYMENT", payment.getId().toString(),
                            "Payment is SUCCESS but has no ledger entries — potential data inconsistency");
                    }
                });

            // ----------------------------------------------------------------
            // 3. Duplicate ledger entries
            //    Expected: exactly 1 DEBIT + 1 CREDIT = 2 entries per payment.
            //    Any payment with > 2 entries = duplicate write detected.
            // ----------------------------------------------------------------
            List<UUID> duplicates = ledgerEntryRepository.findPaymentIdsWithDuplicateLedgerEntries();
            for (UUID paymentId : duplicates) {
                duplicateCount.incrementAndGet();
                log.error("RECONCILIATION[DUPLICATE_LEDGER]: paymentId={} has more than 2 ledger entries",
                    paymentId);
                emitAlert("DUPLICATE_LEDGER", paymentId.toString(),
                    "Payment has more than 2 ledger entries — double-write suspected");
            }

            // ----------------------------------------------------------------
            // 4. Ledger debit/credit imbalance
            //    Double-entry invariant: SUM(DEBIT) == SUM(CREDIT) per payment.
            // ----------------------------------------------------------------
            List<UUID> imbalanced = ledgerEntryRepository.findPaymentIdsWithLedgerImbalance();
            for (UUID paymentId : imbalanced) {
                imbalanceCount.incrementAndGet();
                log.error("RECONCILIATION[LEDGER_IMBALANCE]: paymentId={} — DEBIT != CREDIT amounts",
                    paymentId);
                emitAlert("LEDGER_IMBALANCE", paymentId.toString(),
                    "Ledger debit/credit mismatch — double-entry constraint violated");
            }

            // ----------------------------------------------------------------
            // 5. Orphan ledger entries (no matching Payment record)
            // ----------------------------------------------------------------
            List<UUID> orphans = ledgerEntryRepository.findOrphanedLedgerPaymentIds();
            for (UUID paymentId : orphans) {
                orphanCount.incrementAndGet();
                log.error("RECONCILIATION[ORPHAN_LEDGER]: ledger has entries for missing paymentId={}",
                    paymentId);
                emitAlert("ORPHAN_LEDGER", paymentId.toString(),
                    "Ledger entries exist for a payment that no longer exists in the payments table");
            }

            log.info("Reconciliation complete. jobId={} stale={} ghost={} duplicate={} imbalance={} orphan={}",
                jobId, staleCount.get(), ghostCount.get(), duplicateCount.get(),
                imbalanceCount.get(), orphanCount.get());

        } finally {
            MDC.remove("reconciliationJobId");
        }
    }

    private void emitAlert(String alertType, String paymentId, String reason) {
        String payload = String.format(
            "{\"alertType\":\"%s\",\"paymentId\":\"%s\",\"reason\":\"%s\",\"timestamp\":\"%s\"}",
            alertType, paymentId, reason, LocalDateTime.now());

        kafkaTemplate.send("reconciliation.alerts", paymentId, payload)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to emit reconciliation alert type={} paymentId={}: {}",
                        alertType, paymentId, ex.getMessage());
                }
            });
    }
}

