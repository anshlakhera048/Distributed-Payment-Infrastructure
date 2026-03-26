package com.payments.payment_service.service;

import com.payments.payment_service.dto.CreatePaymentRequest;
import com.payments.payment_service.entity.Payment;
import com.payments.payment_service.kafka.KafkaProducerService;
import com.payments.payment_service.repository.PaymentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Service
public class PaymentService {

    @Autowired
    private PaymentRepository repo;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Transactional
    public Payment createPayment(CreatePaymentRequest req, String idemKey) {

        // Check if this request was already processed
        Optional<Payment> existing = repo.findByIdempotencyKey(idemKey);
        if (existing.isPresent()) {
            return existing.get(); // Return existing — idempotent!
        }

        Payment payment = new Payment();
        payment.setId(UUID.randomUUID());
        payment.setUserId(req.getUserId());
        payment.setAmount(req.getAmount());
        payment.setCurrency(req.getCurrency());
        payment.setStatus("PENDING");
        payment.setIdempotencyKey(idemKey);
        payment.setCreatedAt(LocalDateTime.now());

        Payment saved = repo.save(payment);
        kafkaProducerService.publishPaymentCreated(saved);
        return saved;
    }
}