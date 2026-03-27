package com.payments.payment_service.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * WebSocket handler for real-time payment event streaming.
 *
 * Clients connect to: ws://host:8080/ws/payments
 *
 * Receives:
 *  - payment.processed events
 *  - payment.failed events
 *  - fraud.alerts (future)
 *
 * Each message is a JSON-serialized PaymentEvent.
 */
@Component
public class PaymentWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentWebSocketHandler.class);

    private final WebSocketSessionManager sessionManager;

    public PaymentWebSocketHandler(WebSocketSessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessionManager.register(session);
        try {
            session.sendMessage(new TextMessage("{\"type\":\"CONNECTED\",\"message\":\"Subscribed to payment events\"}"));
        } catch (Exception e) {
            log.warn("Could not send welcome message to sessionId={}: {}", session.getId(), e.getMessage());
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessionManager.remove(session);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.error("WebSocket transport error for sessionId={}: {}", session.getId(), exception.getMessage());
        sessionManager.remove(session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        // Clients can send pings; echo back a pong for liveness checks
        if ("ping".equalsIgnoreCase(message.getPayload().trim())) {
            try {
                session.sendMessage(new TextMessage("{\"type\":\"PONG\"}"));
            } catch (Exception e) {
                log.warn("Failed to send pong to sessionId={}", session.getId());
            }
        }
    }
}
