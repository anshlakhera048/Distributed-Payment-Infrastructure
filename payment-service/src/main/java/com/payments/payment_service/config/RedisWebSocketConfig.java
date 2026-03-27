package com.payments.payment_service.config;

import com.payments.payment_service.websocket.RedisWebSocketSubscriber;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

/**
 * Registers the Redis pub/sub listener container for WebSocket fan-out.
 *
 * Only active when {@code app.websocket.mode=redis}. The listener container
 * subscribes to the "ws:payments" channel.  On receipt, it delegates to
 * {@link RedisWebSocketSubscriber#onMessage} which broadcasts to local WS sessions.
 */
@Configuration
@ConditionalOnProperty(name = "app.websocket.mode", havingValue = "redis")
public class RedisWebSocketConfig {

    private static final String WS_CHANNEL_PATTERN = "ws:payments";

    @Bean
    public MessageListenerAdapter webSocketMessageListenerAdapter(RedisWebSocketSubscriber subscriber) {
        // Method name must match the MessageListener interface → "onMessage"
        return new MessageListenerAdapter(subscriber, "onMessage");
    }

    @Bean
    public RedisMessageListenerContainer redisWebSocketListenerContainer(
        RedisConnectionFactory redisConnectionFactory,
        MessageListenerAdapter webSocketMessageListenerAdapter
    ) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory);
        container.addMessageListener(webSocketMessageListenerAdapter, new PatternTopic(WS_CHANNEL_PATTERN));
        return container;
    }
}
