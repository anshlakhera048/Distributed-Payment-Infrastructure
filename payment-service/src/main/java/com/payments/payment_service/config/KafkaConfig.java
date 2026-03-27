package com.payments.payment_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Backpressure semaphore — limits concurrent in-flight Kafka messages
     * being processed. Prevents DB connection pool exhaustion under load spikes.
     *
     * Set to (concurrency × maxPollRecords) = 3 × 10 = 30 max inflight ops.
     * Listener blocks on acquire() before processing; releases on ack.
     */
    private static final int MAX_INFLIGHT = 30;
    static final Semaphore BACKPRESSURE = new Semaphore(MAX_INFLIGHT, true);

    // -----------------------------------------------------------------------
    // Topics
    // -----------------------------------------------------------------------

    @Bean public NewTopic paymentCreatedTopic()   { return TopicBuilder.name("payment.created").partitions(3).replicas(1).build(); }
    @Bean public NewTopic paymentProcessedTopic() { return TopicBuilder.name("payment.processed").partitions(3).replicas(1).build(); }
    @Bean public NewTopic paymentFailedTopic()    { return TopicBuilder.name("payment.failed").partitions(3).replicas(1).build(); }
    @Bean public NewTopic fraudAlertsTopic()      { return TopicBuilder.name("fraud.alerts").partitions(3).replicas(1).build(); }
    @Bean public NewTopic fraudRequestTopic()     { return TopicBuilder.name("fraud.request").partitions(3).replicas(1).build(); }
    @Bean public NewTopic fraudResultTopic()      { return TopicBuilder.name("fraud.result").partitions(3).replicas(1).build(); }
    @Bean public NewTopic paymentCreatedDlqTopic()   { return TopicBuilder.name("payment.created.DLQ").partitions(1).replicas(1).build(); }
    @Bean public NewTopic paymentProcessedDlqTopic() { return TopicBuilder.name("payment.processed.DLQ").partitions(1).replicas(1).build(); }
    @Bean public NewTopic fraudRequestDlqTopic()     { return TopicBuilder.name("fraud.request.DLQ").partitions(1).replicas(1).build(); }
    @Bean public NewTopic fraudResultDlqTopic()      { return TopicBuilder.name("fraud.result.DLQ").partitions(1).replicas(1).build(); }
    @Bean public NewTopic reconciliationTopic()   { return TopicBuilder.name("reconciliation.alerts").partitions(1).replicas(1).build(); }

    // -----------------------------------------------------------------------
    // PARTITION KEY STRATEGY
    // -----------------------------------------------------------------------
    //
    // All business events (payment.created, payment.processed, payment.failed)
    // use userId as the Kafka partition key.
    //
    // RATIONALE vs paymentId:
    //   • paymentId as key: guarantees strict ordering of a single payment's lifecycle
    //     but distributes load randomly (paymentId is UUID → well-spread but no affinity).
    //   • userId as key: guarantees all events for a given user land on the same partition,
    //     enabling:
    //       - Sequential per-user velocity checks without cross-partition coordination
    //       - Stateful stream processing by user (e.g., Kafka Streams)
    //       - Easier debugging: one partition holds the full user's history
    //     Trade-off: hot partitions if a small number of users generate disproportionate load.
    //     Mitigation: add a salt (userId + paymentId % saltFactor) for extreme outliers.
    //
    // fraud.request/fraud.result use the same userId key to keep fraud pipeline on the
    // same partition as the originating payment.created event (co-partitioning).
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Producer — idempotent, all-acks, Kafka headers support
    // -----------------------------------------------------------------------

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        // Delivery timeout: 2 minutes max across all retries
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // -----------------------------------------------------------------------
    // Consumer — bounded poll, manual commits, dedicated thread pools
    // -----------------------------------------------------------------------

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Backpressure: only fetch 10 records per poll — prevents OOM under bursts
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        // Give consumer up to 5 minutes to process a batch before session times out
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10_000);
        // Fetch tuning: wait up to 500ms to accumulate 1KB before returning
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Primary listener factory used by PaymentEventConsumer and PaymentProcessedConsumer.
     *
     * Error handling: ExponentialBackOff (1s → 2s → 4s → 8s, max 4 retries).
     * After exhaustion, the default error handler logs and re-throws — the outer
     * try/catch in the consumer then routes to DLQ.
     *
     * Concurrency: 3 threads per listener (matches 3 partitions per topic).
     *
     * Thread pool: dedicated pool isolated from the HTTP thread pool to prevent
     * request processing starvation during burst traffic.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Dedicated bounded thread pool for Kafka listener threads
        factory.getContainerProperties().setListenerTaskExecutor(kafkaConsumerExecutor());

        // Exponential backoff: 1s, 2s, 4s, 8s — max 4 retries before DLQ
        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2L);
        backOff.setMaxAttempts(4);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(backOff);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    /**
     * Bounded thread pool for Kafka consumer listener threads.
     * Core=3 (one per partition), max=9 (burst handling), queue=0 (direct handoff).
     * Rejects tasks when queue+pool are full → triggers backpressure.
     */
    @Bean(name = "kafkaConsumerExecutor")
    public AsyncTaskExecutor kafkaConsumerExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(3);
        executor.setMaxPoolSize(9);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("kafka-consumer-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();
        return executor;
    }
}

