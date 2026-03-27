package com.payments.payment_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WebhookPayload {

    private String eventId;
    private String eventType;
    private UUID paymentId;
    private String status;
    private String correlationId;
    private LocalDateTime timestamp;
    private Object data;
}
