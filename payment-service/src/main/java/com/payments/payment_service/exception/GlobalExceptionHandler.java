package com.payments.payment_service.exception;

import com.payments.payment_service.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.stream.Collectors;

/**
 * Global exception handler — maps all exceptions to a standard error response format.
 * Every error includes the correlation ID from MDC for distributed tracing.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(PaymentNotFoundException.class)
    public ResponseEntity<ErrorResponse> handlePaymentNotFound(
        PaymentNotFoundException ex, HttpServletRequest request
    ) {
        log.warn("Payment not found: paymentId={}", ex.getPaymentId());
        return build(HttpStatus.NOT_FOUND, "PAYMENT_NOT_FOUND", ex.getMessage(), request);
    }

    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<ErrorResponse> handleRateLimit(
        RateLimitExceededException ex, HttpServletRequest request
    ) {
        log.warn("Rate limit exceeded: {}", ex.getMessage());
        return build(HttpStatus.TOO_MANY_REQUESTS, "RATE_LIMIT_EXCEEDED", ex.getMessage(), request);
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ErrorResponse> handleDataIntegrity(
        DataIntegrityViolationException ex, HttpServletRequest request
    ) {
        // Idempotency key collision — duplicate request
        if (ex.getMessage() != null && ex.getMessage().contains("idempotency_key")) {
            log.warn("Duplicate idempotency key violation");
            return build(HttpStatus.CONFLICT, "DUPLICATE_IDEMPOTENCY_KEY",
                "A payment with this Idempotency-Key already exists", request);
        }
        log.error("Data integrity violation: {}", ex.getMessage());
        return build(HttpStatus.CONFLICT, "DATA_CONFLICT", "Data integrity constraint violated", request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(
        MethodArgumentNotValidException ex, HttpServletRequest request
    ) {
        String details = ex.getBindingResult().getFieldErrors().stream()
            .map(e -> e.getField() + ": " + e.getDefaultMessage())
            .collect(Collectors.joining(", "));

        log.warn("Validation failed: {}", details);
        return build(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", details, request);
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<ErrorResponse> handleMissingHeader(
        MissingRequestHeaderException ex, HttpServletRequest request
    ) {
        log.warn("Missing required header: {}", ex.getHeaderName());
        return build(HttpStatus.BAD_REQUEST, "MISSING_HEADER",
            "Required header missing: " + ex.getHeaderName(), request);
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ErrorResponse> handleIllegalState(
        IllegalStateException ex, HttpServletRequest request
    ) {
        log.error("Illegal state: {}", ex.getMessage(), ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", ex.getMessage(), request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception ex, HttpServletRequest request) {
        log.error("Unhandled exception: {}", ex.getMessage(), ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
            "An unexpected error occurred", request);
    }

    private ResponseEntity<ErrorResponse> build(
        HttpStatus status, String type, String message, HttpServletRequest request
    ) {
        return ResponseEntity.status(status).body(
            ErrorResponse.builder()
                .type(type)
                .message(message)
                .status(status.value())
                .correlationId(MDC.get("correlationId"))
                .path(request.getRequestURI())
                .timestamp(LocalDateTime.now())
                .build()
        );
    }
}
