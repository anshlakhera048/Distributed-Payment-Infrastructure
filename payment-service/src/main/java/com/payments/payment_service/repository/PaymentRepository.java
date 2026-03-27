package com.payments.payment_service.repository;

import com.payments.payment_service.entity.Payment;
import com.payments.payment_service.entity.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import jakarta.persistence.LockModeType;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    /**
     * Pessimistic write lock for idempotency key — prevents races on
     * concurrent duplicate requests.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM Payment p WHERE p.idempotencyKey = :key")
    Optional<Payment> findByIdempotencyKeyForUpdate(@Param("key") String idempotencyKey);

    List<Payment> findByStatusAndCreatedAtBefore(PaymentStatus status, LocalDateTime before);

    @Query("SELECT COUNT(p) FROM Payment p WHERE p.userId = :userId AND p.createdAt >= :since")
    long countByUserIdAndCreatedAtAfter(@Param("userId") UUID userId, @Param("since") LocalDateTime since);
}
