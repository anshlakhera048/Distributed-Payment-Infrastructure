package com.payments.payment_service.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

import java.util.UUID;

@Data
public class RegisterWebhookRequest {

    @NotNull(message = "merchantId is required")
    private UUID merchantId;

    @NotBlank(message = "url is required")
    @Pattern(
        regexp = "^https://[a-zA-Z0-9]([a-zA-Z0-9\\-]*[a-zA-Z0-9])?(\\.[a-zA-Z0-9]([a-zA-Z0-9\\-]*[a-zA-Z0-9])?)+(/.*)?$",
        message = "url must be a valid HTTPS URL with a public domain"
    )
    private String url;

    /**
     * Comma-separated list of events to subscribe to.
     * Accepted values: payment.processed, payment.failed, fraud.alerts
     * Use "ALL" to subscribe to all events.
     */
    @NotBlank(message = "events is required")
    private String events;
}
