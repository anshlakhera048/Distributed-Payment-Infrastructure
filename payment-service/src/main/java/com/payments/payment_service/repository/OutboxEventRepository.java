package com.payments.payment_service.repository;

import com.payments.payment_service.entity.OutboxEvent;
import com.payments.payment_service.entity.OutboxEventStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    List<OutboxEvent> findTop50ByStatusOrderByCreatedAtAsc(OutboxEventStatus status);

    @Modifying
    @Query("DELETE FROM OutboxEvent o WHERE o.status = :status AND o.retryCount >= :maxRetries")
    void deleteExhaustedEvents(
        @Param("status") OutboxEventStatus status,
        @Param("maxRetries") int maxRetries
    );
}
