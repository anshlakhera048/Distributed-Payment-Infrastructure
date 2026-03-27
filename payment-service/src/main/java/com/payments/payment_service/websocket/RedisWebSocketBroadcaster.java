package com.payments.payment_service.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Multi-node WebSocket broadcaster using Redis pub/sub.
 *
 * Activated when {@code app.websocket.mode=redis} is set (e.g. in K8s).
 * Falls back to {@link WebSocketSessionManager} (single-node) by default.
 *
 * How it works:
 *   1. Any pod calls {@link #broadcast} → publishes JSON to Redis channel "ws:payments".
 *   2. Every pod's {@link RedisWebSocketSubscriber} receives the channel message
 *      and calls {@link WebSocketSessionManager#broadcast} on its own local sessions.
 *
 * This ensures all connected WebSocket clients across the cluster receive the event,
 * regardless of which pod processed the Kafka record.
 */
@Component
@ConditionalOnProperty(name = "app.websocket.mode", havingValue = "redis")
public class RedisWebSocketBroadcaster implements WebSocketBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(RedisWebSocketBroadcaster.class);
    static final String WS_CHANNEL = "ws:payments";

    private final StringRedisTemplate stringRedisTemplate;

    public RedisWebSocketBroadcaster(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * Publishes {@code jsonMessage} to the Redis pub/sub channel.
     * Every pod subscribed to this channel will receive the message
     * and forward it to its local WebSocket sessions via {@link RedisWebSocketSubscriber}.
     */
    @Override
    public void broadcast(String jsonMessage) {
        try {
            stringRedisTemplate.convertAndSend(WS_CHANNEL, jsonMessage);
            log.debug("Published WS broadcast to Redis channel={}", WS_CHANNEL);
        } catch (Exception e) {
            log.error("Failed to publish WebSocket broadcast to Redis: {}", e.getMessage(), e);
        }
    }
}
