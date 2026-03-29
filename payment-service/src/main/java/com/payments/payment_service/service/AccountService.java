package com.payments.payment_service.service;

import com.payments.payment_service.entity.Account;
import com.payments.payment_service.entity.AccountType;
import com.payments.payment_service.exception.InsufficientBalanceException;
import com.payments.payment_service.repository.AccountRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

/**
 * Manages account balances with optimistic locking.
 *
 * Account creation is lazy: accounts are auto-provisioned on first use
 * with a configurable initial balance. In production, account provisioning
 * would be a separate onboarding flow.
 *
 * Optimistic locking via JPA @Version ensures concurrent updates to the same
 * account throw OptimisticLockException. The caller (LedgerService) retries
 * via Spring Retry.
 */
@Service
public class AccountService {

    private static final Logger log = LoggerFactory.getLogger(AccountService.class);
    private static final BigDecimal DEFAULT_USER_BALANCE = new BigDecimal("100000.0000");
    private static final BigDecimal DEFAULT_MERCHANT_BALANCE = BigDecimal.ZERO;

    private final AccountRepository accountRepository;

    public AccountService(AccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }

    /**
     * Gets or lazily creates an account for the given owner and currency.
     * User accounts start with a default balance; merchant accounts start at zero.
     */
    @Transactional
    public Account getOrCreateAccount(String ownerId, String currency, AccountType type) {
        return accountRepository.findByOwnerIdAndCurrency(ownerId, currency)
            .orElseGet(() -> {
                Account account = new Account();
                account.setId(UUID.randomUUID());
                account.setOwnerId(ownerId);
                account.setCurrency(currency);
                account.setAccountType(type);
                account.setBalance(type == AccountType.USER ? DEFAULT_USER_BALANCE : DEFAULT_MERCHANT_BALANCE);
                Account saved = accountRepository.save(account);
                log.info("Auto-provisioned {} account for ownerId={} currency={} initialBalance={}",
                    type, ownerId, currency, saved.getBalance());
                return saved;
            });
    }

    /**
     * Debits (subtracts) an amount from a user account.
     * Enforces non-negative balance constraint for user accounts.
     *
     * @throws InsufficientBalanceException if the debit would result in negative balance
     */
    @Transactional(noRollbackFor = InsufficientBalanceException.class)
    public void debit(String ownerId, String currency, BigDecimal amount) {
        Account account = getOrCreateAccount(ownerId, currency, AccountType.USER);

        if (account.getBalance().compareTo(amount) < 0) {
            log.warn("Insufficient balance for debit: ownerId={} currency={} balance={} requested={}",
                ownerId, currency, account.getBalance(), amount);
            throw new InsufficientBalanceException(ownerId, currency);
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.debug("Debited {} {} from account ownerId={} newBalance={}",
            amount, currency, ownerId, account.getBalance());
    }

    /**
     * Credits (adds) an amount to a merchant account.
     */
    @Transactional
    public void credit(String ownerId, String currency, BigDecimal amount) {
        Account account = getOrCreateAccount(ownerId, currency, AccountType.MERCHANT);
        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.debug("Credited {} {} to account ownerId={} newBalance={}",
            amount, currency, ownerId, account.getBalance());
    }

    /**
     * Returns the current balance for an account, or empty if the account doesn't exist.
     */
    @Transactional(readOnly = true)
    public Optional<BigDecimal> getBalance(String ownerId, String currency) {
        return accountRepository.findByOwnerIdAndCurrency(ownerId, currency)
            .map(Account::getBalance);
    }
}
