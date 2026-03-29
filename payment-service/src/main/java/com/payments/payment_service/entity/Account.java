package com.payments.payment_service.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Represents a financial account with balance tracking and optimistic locking.
 *
 * Each user/merchant has one Account per currency. The balance is updated atomically
 * during payment processing via {@code @Version}-based optimistic locking.
 *
 * Optimistic locking strategy:
 *   - JPA {@code @Version} field increments on every UPDATE
 *   - Concurrent updates to the same account cause {@link jakarta.persistence.OptimisticLockException}
 *   - The caller retries the entire transaction on conflict (see AccountService)
 *
 * Balance constraints:
 *   - Balance can go negative only for merchant accounts (credit side)
 *   - User accounts enforce non-negative balance (debit side)
 */
@Entity
@Table(name = "accounts", uniqueConstraints = {
    @UniqueConstraint(name = "uk_account_owner_currency", columnNames = {"owner_id", "currency"})
}, indexes = {
    @Index(name = "idx_account_owner_id", columnList = "owner_id"),
    @Index(name = "idx_account_type", columnList = "account_type")
})
@Getter
@Setter
@NoArgsConstructor
public class Account {

    @Id
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "owner_id", nullable = false)
    private String ownerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "account_type", nullable = false)
    private AccountType accountType;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal balance;

    @Column(nullable = false, length = 3)
    private String currency;

    @Version
    @Column(nullable = false)
    private Long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (balance == null) {
            balance = BigDecimal.ZERO;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
