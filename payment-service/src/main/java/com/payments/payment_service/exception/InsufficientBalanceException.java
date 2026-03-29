package com.payments.payment_service.exception;

public class InsufficientBalanceException extends RuntimeException {

    private final String accountOwnerId;
    private final String currency;

    public InsufficientBalanceException(String accountOwnerId, String currency) {
        super("Insufficient balance for account owner=" + accountOwnerId + " currency=" + currency);
        this.accountOwnerId = accountOwnerId;
        this.currency = currency;
    }

    public String getAccountOwnerId() {
        return accountOwnerId;
    }

    public String getCurrency() {
        return currency;
    }
}
