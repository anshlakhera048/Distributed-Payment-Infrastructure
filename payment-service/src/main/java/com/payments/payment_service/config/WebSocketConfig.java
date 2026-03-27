package com.payments.payment_service.config;

import com.payments.payment_service.websocket.PaymentWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final PaymentWebSocketHandler paymentWebSocketHandler;

    public WebSocketConfig(PaymentWebSocketHandler paymentWebSocketHandler) {
        this.paymentWebSocketHandler = paymentWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry
            .addHandler(paymentWebSocketHandler, "/ws/payments")
            .setAllowedOriginPatterns("*");
    }
}
