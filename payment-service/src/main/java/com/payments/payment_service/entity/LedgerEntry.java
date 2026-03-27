package com.payments.payment_service.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Represents a single leg of a double-entry bookkeeping entry.
 * Every payment creates exactly two entries: one DEBIT and one CREDIT.
 * Invariant: sum(DEBIT) == sum(CREDIT) for any given paymentId.
 */
@Entity
@Table(name = "ledger_entries", indexes = {
    @Index(name = "idx_ledger_payment_id", columnList = "payment_id"),
    @Index(name = "idx_ledger_account_id", columnList = "account_id"),
    @Index(name = "idx_ledger_created_at", columnList = "created_at")
})
@Getter
@Setter
@NoArgsConstructor
public class LedgerEntry {

    @Id
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Enumerated(EnumType.STRING)
    @Column(name = "entry_type", nullable = false)
    private LedgerEntryType entryType;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    /**
     * The account being affected (userId for DEBIT, merchantId for CREDIT).
     */
    @Column(name = "account_id", nullable = false)
    private String accountId;

    /**
     * The counter-party account.
     */
    @Column(name = "counter_account_id", nullable = false)
    private String counterAccountId;

    @Column(name = "correlation_id")
    private String correlationId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }
}
