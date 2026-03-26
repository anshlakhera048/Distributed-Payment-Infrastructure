package com.payments.payment_service.dto;

import lombok.Data;
import java.util.UUID;

@Data
public class PaymentResponse {
    private UUID paymentId;
    private String status;
}