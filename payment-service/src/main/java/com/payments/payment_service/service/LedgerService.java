package com.payments.payment_service.service;

import com.payments.payment_service.entity.LedgerEntry;
import com.payments.payment_service.entity.LedgerEntryType;
import com.payments.payment_service.repository.LedgerEntryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import com.payments.payment_service.exception.InsufficientBalanceException;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Implements double-entry bookkeeping with account balance updates.
 *
 * Invariant (enforced after every recording):
 *   SUM(DEBIT entries for paymentId) == SUM(CREDIT entries for paymentId)
 *
 * Account balance updates:
 *   - DEBIT: subtracts from payer's account (via AccountService with optimistic locking)
 *   - CREDIT: adds to payee's account (via AccountService with optimistic locking)
 *   - Both operations happen atomically within the same @Transactional boundary
 *   - OptimisticLockException on concurrent updates triggers retry at the caller
 *
 * All writes happen inside the caller's @Transactional boundary.
 * Any invariant violation triggers an exception which rolls back the entire
 * outer transaction — guaranteeing strong ledger consistency.
 */
@Service
public class LedgerService {

    private static final Logger log = LoggerFactory.getLogger(LedgerService.class);

    private final LedgerEntryRepository ledgerEntryRepository;
    private final AccountService accountService;

    public LedgerService(LedgerEntryRepository ledgerEntryRepository, AccountService accountService) {
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.accountService = accountService;
    }

    /**
     * Records a payment using double-entry bookkeeping.
     * Creates one DEBIT (funds leaving payerId) and one CREDIT (funds arriving at payeeId).
     * Must be called within an active @Transactional context.
     *
     * @param paymentId     the payment being recorded
     * @param payerId       account being debited (typically userId)
     * @param payeeId       account being credited (typically merchantId)
     * @param amount        payment amount
     * @param currency      ISO 4217 currency code
     * @param correlationId correlation ID for tracing
     */
    @Transactional(noRollbackFor = InsufficientBalanceException.class)
    public void recordPayment(
        UUID paymentId,
        String payerId,
        String payeeId,
        BigDecimal amount,
        String currency,
        String correlationId
    ) {
        if (ledgerEntryRepository.existsByPaymentId(paymentId)) {
            log.warn("Ledger entries already exist for paymentId={} — skipping (idempotent)", paymentId);
            return;
        }

        LedgerEntry debit = buildEntry(paymentId, LedgerEntryType.DEBIT, amount, currency,
            payerId, payeeId, correlationId);

        LedgerEntry credit = buildEntry(paymentId, LedgerEntryType.CREDIT, amount, currency,
            payeeId, payerId, correlationId);

        ledgerEntryRepository.save(debit);
        ledgerEntryRepository.save(credit);

        validateInvariant(paymentId, amount);

        // Update account balances atomically (optimistic locking via @Version)
        accountService.debit(payerId, currency, amount);
        accountService.credit(payeeId, currency, amount);

        log.info("Recorded double-entry for paymentId={} amount={} {} — account balances updated",
            paymentId, amount, currency);
    }

    /**
     * Validates the double-entry invariant:
     *   total debits == total credits for the given paymentId.
     *
     * Throws IllegalStateException if violated — rolls back the transaction.
     */
    private void validateInvariant(UUID paymentId, BigDecimal expected) {
        BigDecimal totalDebits = ledgerEntryRepository
            .sumAmountByPaymentIdAndType(paymentId, LedgerEntryType.DEBIT);
        BigDecimal totalCredits = ledgerEntryRepository
            .sumAmountByPaymentIdAndType(paymentId, LedgerEntryType.CREDIT);

        if (totalDebits.compareTo(totalCredits) != 0) {
            throw new IllegalStateException(
                String.format("LEDGER INVARIANT VIOLATION for paymentId=%s: debits=%s credits=%s",
                    paymentId, totalDebits, totalCredits)
            );
        }

        if (totalDebits.compareTo(expected) != 0) {
            throw new IllegalStateException(
                String.format("LEDGER INVARIANT VIOLATION for paymentId=%s: " +
                    "expected=%s actual=%s", paymentId, expected, totalDebits)
            );
        }
    }

    /**
     * Public consistency gate: verifies the ledger is balanced for a payment
     * BEFORE the caller marks the payment as SUCCESS.
     *
     * <p>Called by {@code PaymentService.processPaymentResult()} immediately after
     * {@link #recordPayment} returns. Although {@code recordPayment} already validates
     * the invariant internally, this explicit call provides an unambiguous pre-condition
     * guard at the call site — making the ordering constraint visible:
     * <ol>
     *   <li>Write ledger entries ({@code recordPayment})</li>
     *   <li>Verify invariant holds ({@code verifyLedgerBalance}) ← you are here</li>
     *   <li>Set PaymentStatus.SUCCESS</li>
     * </ol>
     *
     * <p>If this method throws, the outer {@code @Transactional} rolls back, ensuring
     * no SUCCESS status is ever persisted without a balanced ledger.
     *
     * @param paymentId      the payment to verify
     * @param expectedAmount the amount that must equal both total debits and total credits
     * @throws IllegalStateException if the ledger is unbalanced or entries are missing
     */
    public void verifyLedgerBalance(UUID paymentId, BigDecimal expectedAmount) {
        if (!ledgerEntryRepository.existsByPaymentId(paymentId)) {
            throw new IllegalStateException(
                "Ledger balance verification failed: no entries found for paymentId=" + paymentId +
                ". Cannot mark payment SUCCESS without a balanced ledger."
            );
        }
        validateInvariant(paymentId, expectedAmount);
        log.debug("Ledger balance verified for paymentId={} amount={}", paymentId, expectedAmount);
    }

    private LedgerEntry buildEntry(
        UUID paymentId,
        LedgerEntryType type,
        BigDecimal amount,
        String currency,
        String accountId,
        String counterAccountId,
        String correlationId
    ) {
        LedgerEntry entry = new LedgerEntry();
        entry.setId(UUID.randomUUID());
        entry.setPaymentId(paymentId);
        entry.setEntryType(type);
        entry.setAmount(amount);
        entry.setCurrency(currency);
        entry.setAccountId(accountId);
        entry.setCounterAccountId(counterAccountId);
        entry.setCorrelationId(correlationId);
        return entry;
    }
}
