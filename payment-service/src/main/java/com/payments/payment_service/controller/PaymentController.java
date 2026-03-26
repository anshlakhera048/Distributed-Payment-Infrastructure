package com.payments.payment_service.controller;

import com.payments.payment_service.dto.CreatePaymentRequest;
import com.payments.payment_service.dto.PaymentResponse;
import com.payments.payment_service.entity.Payment;
import com.payments.payment_service.service.PaymentService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    @Autowired
    private PaymentService paymentService;

    @PostMapping
    public ResponseEntity<PaymentResponse> createPayment(
            @Valid @RequestBody CreatePaymentRequest request,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {

        Payment payment = paymentService.createPayment(request, idempotencyKey);

        PaymentResponse response = new PaymentResponse();
        response.setPaymentId(payment.getId());
        response.setStatus(payment.getStatus());

        return ResponseEntity.ok(response);
    }
}