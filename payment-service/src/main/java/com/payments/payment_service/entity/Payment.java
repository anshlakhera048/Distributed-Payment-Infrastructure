package com.payments.payment_service.entity;

import jakarta.persistence.*;
import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "payments", uniqueConstraints = {
    @UniqueConstraint(columnNames = "idempotency_key")
})
@Data
public class Payment {

    @Id
    private UUID id;

    private UUID userId;
    private BigDecimal amount;
    private String currency;
    private String status;
    private String idempotencyKey;
    private LocalDateTime createdAt;
}