package com.payments.payment_service.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Redis pub/sub subscriber that forwards channel messages to local WebSocket sessions.
 *
 * Registered as a {@link MessageListener} in {@link RedisWebSocketConfig} against the
 * "ws:payments" channel. When Redis delivers a message (published by any pod), this
 * listener calls {@link WebSocketSessionManager#broadcast} to push the JSON payload to
 * all WebSocket clients connected to THIS pod.
 *
 * Activated together with {@link RedisWebSocketBroadcaster} when {@code app.websocket.mode=redis}.
 */
@Component
@ConditionalOnProperty(name = "app.websocket.mode", havingValue = "redis")
public class RedisWebSocketSubscriber implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(RedisWebSocketSubscriber.class);

    /**
     * Use WebSocketSessionManager directly (not the WebSocketBroadcaster interface)
     * to avoid circular dispatch back through Redis when running in redis-ws mode.
     */
    private final WebSocketSessionManager sessionManager;

    public RedisWebSocketSubscriber(WebSocketSessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            String jsonBody = new String(message.getBody(), StandardCharsets.UTF_8);
            log.debug("Redis WS subscriber received message ({}B) — forwarding to local sessions", jsonBody.length());
            sessionManager.broadcast(jsonBody);
        } catch (Exception e) {
            log.error("Error forwarding Redis WS message to local sessions: {}", e.getMessage(), e);
        }
    }
}
