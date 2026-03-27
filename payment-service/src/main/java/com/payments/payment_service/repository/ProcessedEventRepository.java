package com.payments.payment_service.repository;

import com.payments.payment_service.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {

    /**
     * Retention cleanup: remove events older than the given cutoff.
     * Prevents the table from growing unboundedly.
     * Run periodically (e.g., daily) via a @Scheduled job.
     */
    @Modifying
    @Query("DELETE FROM ProcessedEvent pe WHERE pe.processedAt < :cutoff")
    int deleteOlderThan(@Param("cutoff") LocalDateTime cutoff);
}
