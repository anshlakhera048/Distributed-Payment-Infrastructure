package com.payments.payment_service.repository;

import com.payments.payment_service.entity.WebhookEndpoint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface WebhookEndpointRepository extends JpaRepository<WebhookEndpoint, UUID> {

    List<WebhookEndpoint> findByMerchantIdAndActiveTrue(UUID merchantId);

    /**
     * Find all active endpoints that are subscribed to a specific event type.
     */
    @Query("SELECT w FROM WebhookEndpoint w WHERE w.active = true " +
           "AND w.merchantId = :merchantId " +
           "AND (w.events = 'ALL' OR w.events LIKE %:eventType%)")
    List<WebhookEndpoint> findActiveEndpointsForEvent(
        @Param("merchantId") UUID merchantId,
        @Param("eventType") String eventType
    );
}
