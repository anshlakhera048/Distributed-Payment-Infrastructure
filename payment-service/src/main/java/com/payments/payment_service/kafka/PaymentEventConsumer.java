package com.payments.payment_service.kafka;

import com.payments.payment_service.entity.Payment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final RestTemplate restTemplate = new RestTemplate();

    @KafkaListener(
        topics = "payment.created",
        groupId = "payment-group"
    )
    public void handlePaymentCreated(
            ConsumerRecord<String, Payment> record,
            Acknowledgment ack) {

        Payment payment = record.value();
        log.info("Received payment event: {}", payment.getId());

        Map<String, Object> fraudRequest = new HashMap<>();
        fraudRequest.put("payment_id", payment.getId().toString());
        fraudRequest.put("user_id", payment.getUserId().toString());
        fraudRequest.put("amount", payment.getAmount());
        fraudRequest.put("currency", payment.getCurrency());

        try {
            ResponseEntity<Map<String, Object>> fraudResponse = restTemplate.postForEntity(
                "http://localhost:8000/score",
                fraudRequest,
                (Class<Map<String, Object>>) (Class<?>) Map.class
            );

            Map<String, Object> body = fraudResponse.getBody();
            if (body == null) {
                log.error("Empty response from fraud-service for paymentId={}", payment.getId());
                // Acknowledge to avoid infinite redelivery of a structurally valid message
                ack.acknowledge();
                return;
            }

            boolean isFraud = Boolean.TRUE.equals(body.get("fraud"));
            String reason = (String) body.get("reason");
            log.info("Fraud check for payment {} → fraud={}, reason={}", payment.getId(), isFraud, reason);

            // TODO: update payment status in DB based on fraud result (next phase)

        } catch (RestClientException ex) {
            log.error("Failed to call fraud-service for paymentId={}", payment.getId(), ex);
            // Do NOT acknowledge — message will be redelivered
            return;
        }

        ack.acknowledge();
    }
}