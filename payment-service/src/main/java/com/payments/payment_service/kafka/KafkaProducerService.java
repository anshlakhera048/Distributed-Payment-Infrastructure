package com.payments.payment_service.kafka;

import com.payments.payment_service.entity.Payment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    @Autowired
    private KafkaTemplate<String, Payment> kafkaTemplate;

    public void publishPaymentCreated(Payment payment) {
        kafkaTemplate.send("payment.created", payment.getId().toString(), payment)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish payment.created event for paymentId={}", payment.getId(), ex);
                } else {
                    log.debug("Published payment.created event for paymentId={}", payment.getId());
                }
            });
    }
}