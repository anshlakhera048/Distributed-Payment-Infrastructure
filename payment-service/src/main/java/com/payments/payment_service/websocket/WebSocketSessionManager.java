package com.payments.payment_service.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe store for active WebSocket sessions.
 * Broadcasts messages to all locally-connected clients.
 *
 * Implements {@link WebSocketBroadcaster} for single-node deployments.
 * In multi-node deployments, {@link RedisWebSocketBroadcaster} wraps this
 * class: Redis pub/sub fans out the message to every pod, and each pod
 * calls {@link #broadcast} on its own sessions from the Redis subscriber.
 *
 * Spring wires the active profile's {@link WebSocketBroadcaster} bean:
 *   - default / "local" profile → this bean (no Redis pub/sub)
 *   - "redis-ws" profile       → {@link RedisWebSocketBroadcaster}
 */
@Primary
@Component
public class WebSocketSessionManager implements WebSocketBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(WebSocketSessionManager.class);

    private final Set<WebSocketSession> sessions = ConcurrentHashMap.newKeySet();

    public void register(WebSocketSession session) {
        sessions.add(session);
        log.info("WebSocket client connected: sessionId={} total={}", session.getId(), sessions.size());
    }

    public void remove(WebSocketSession session) {
        sessions.remove(session);
        log.info("WebSocket client disconnected: sessionId={} total={}", session.getId(), sessions.size());
    }

    /**
     * Broadcasts a JSON message to all connected sessions.
     * Removes sessions that are no longer open to prevent stale state accumulation.
     */
    public void broadcast(String jsonMessage) {
        log.debug("Broadcasting to {} WebSocket clients", sessions.size());

        TextMessage message = new TextMessage(jsonMessage);
        sessions.removeIf(session -> {
            if (!session.isOpen()) {
                return true;
            }
            try {
                synchronized (session) {
                    session.sendMessage(message);
                }
                return false;
            } catch (IOException e) {
                log.warn("Failed to send to WebSocket session={}: {}", session.getId(), e.getMessage());
                return true;
            }
        });
    }

    public int getConnectedCount() {
        return sessions.size();
    }
}
