package com.payments.payment_service.repository;

import com.payments.payment_service.entity.LedgerEntry;
import com.payments.payment_service.entity.LedgerEntryType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, UUID> {

    List<LedgerEntry> findByPaymentId(UUID paymentId);

    boolean existsByPaymentId(UUID paymentId);

    /**
     * Used to validate the double-entry invariant: sum(DEBIT) == sum(CREDIT) per payment.
     */
    @Query("SELECT COALESCE(SUM(e.amount), 0) FROM LedgerEntry e " +
           "WHERE e.paymentId = :paymentId AND e.entryType = :type")
    BigDecimal sumAmountByPaymentIdAndType(
        @Param("paymentId") UUID paymentId,
        @Param("type") LedgerEntryType type
    );

    /**
     * Finds payments that have more than 2 ledger entries (one DEBIT + one CREDIT = 2 normal).
     * More than 2 entries indicates duplicate ledger writes — a data integrity violation.
     */
    @Query("SELECT e.paymentId FROM LedgerEntry e GROUP BY e.paymentId HAVING COUNT(e) > 2")
    List<UUID> findPaymentIdsWithDuplicateLedgerEntries();

    /**
     * Finds payment IDs in the ledger that have no corresponding Payment record.
     * This indicates orphan ledger entries created without an associated payment (data leak).
     */
    @Query("SELECT DISTINCT e.paymentId FROM LedgerEntry e " +
           "WHERE NOT EXISTS (SELECT p FROM Payment p WHERE p.id = e.paymentId)")
    List<UUID> findOrphanedLedgerPaymentIds();

    /**
     * Finds payment IDs where the sum of DEBIT entries does not equal the sum of CREDIT entries.
     * A mismatch violates the double-entry bookkeeping invariant.
     */
    @Query("SELECT e.paymentId FROM LedgerEntry e GROUP BY e.paymentId " +
           "HAVING SUM(CASE WHEN e.entryType = com.payments.payment_service.entity.LedgerEntryType.DEBIT " +
           "                THEN e.amount ELSE 0 END) " +
           "    <> SUM(CASE WHEN e.entryType = com.payments.payment_service.entity.LedgerEntryType.CREDIT " +
           "                THEN e.amount ELSE 0 END)")
    List<UUID> findPaymentIdsWithLedgerImbalance();
}

