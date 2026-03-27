package com.payments.payment_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentResponse {

    private UUID paymentId;
    private UUID userId;
    private UUID merchantId;
    private BigDecimal amount;
    private String currency;
    private String status;
    private String idempotencyKey;
    private String description;
    private LocalDateTime createdAt;
}
