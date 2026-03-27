package com.payments.payment_service.repository;

import com.payments.payment_service.entity.WebhookDelivery;
import com.payments.payment_service.entity.WebhookDeliveryStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface WebhookDeliveryRepository extends JpaRepository<WebhookDelivery, UUID> {

    boolean existsByIdempotencyKey(String idempotencyKey);

    List<WebhookDelivery> findByStatusAndNextRetryAtBefore(
        WebhookDeliveryStatus status,
        LocalDateTime now
    );

    List<WebhookDelivery> findByPaymentId(UUID paymentId);
}
