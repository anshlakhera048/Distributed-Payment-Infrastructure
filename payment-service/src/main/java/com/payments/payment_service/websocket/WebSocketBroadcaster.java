package com.payments.payment_service.websocket;

/**
 * Abstraction over WebSocket message broadcast.
 *
 * Single-node: {@link WebSocketSessionManager} broadcasts directly to in-memory sessions.
 * Multi-node:  {@link RedisWebSocketBroadcaster} publishes to a Redis pub/sub channel so
 *              every pod picks up the message and forwards it to its local sessions.
 *
 * Implementations must be non-blocking or use a dedicated thread pool — the caller
 * may be inside a Kafka listener thread.
 */
public interface WebSocketBroadcaster {

    /**
     * Sends {@code jsonMessage} to all WebSocket clients connected to this
     * (or any other) instance in the cluster.
     *
     * @param jsonMessage raw JSON payload — must be a valid JSON string
     */
    void broadcast(String jsonMessage);
}
