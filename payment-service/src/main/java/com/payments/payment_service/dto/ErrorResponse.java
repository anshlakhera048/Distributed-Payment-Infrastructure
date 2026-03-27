package com.payments.payment_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponse {

    private String type;
    private String message;
    private int status;
    private String correlationId;
    private String path;
    private LocalDateTime timestamp;
}
