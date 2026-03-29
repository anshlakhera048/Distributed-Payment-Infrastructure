package com.payments.payment_service.repository;

import com.payments.payment_service.entity.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface AccountRepository extends JpaRepository<Account, UUID> {

    Optional<Account> findByOwnerIdAndCurrency(String ownerId, String currency);

    @Lock(LockModeType.OPTIMISTIC)
    @Query("SELECT a FROM Account a WHERE a.ownerId = :ownerId AND a.currency = :currency")
    Optional<Account> findByOwnerIdAndCurrencyWithLock(
        @Param("ownerId") String ownerId,
        @Param("currency") String currency
    );
}
