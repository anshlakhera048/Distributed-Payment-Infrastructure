package com.payments.payment_service;

import com.payments.payment_service.kafka.KafkaProducerService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class PaymentServiceApplicationTests {

	@MockitoBean
	KafkaProducerService kafkaProducerService;

	@Test
	void contextLoads() {
	}

}
