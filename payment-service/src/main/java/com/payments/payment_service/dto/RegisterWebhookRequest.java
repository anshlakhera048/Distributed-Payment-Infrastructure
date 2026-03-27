package com.payments.payment_service.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.hibernate.validator.constraints.URL;

import java.util.UUID;

@Data
public class RegisterWebhookRequest {

    @NotNull(message = "merchantId is required")
    private UUID merchantId;

    @NotBlank(message = "url is required")
    @URL(message = "url must be a valid HTTP/HTTPS URL")
    private String url;

    /**
     * Comma-separated list of events to subscribe to.
     * Accepted values: payment.processed, payment.failed, fraud.alerts
     * Use "ALL" to subscribe to all events.
     */
    @NotBlank(message = "events is required")
    private String events;
}
