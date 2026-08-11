# Kafka + Spring Boot Codebase Review Checklist

A structured reviewer for codebases using **Java 21**, **Spring Boot**, and **Apache Kafka**. Use this to audit pull requests, onboard new services, or run periodic architecture reviews.

---

## 1. Java 21 Usage

- [ ] **Records** used for immutable DTOs / Kafka message payloads instead of Lombok-heavy POJOs where appropriate.
- [ ] **Pattern matching for switch** (`switch` expressions with pattern/record deconstruction) used instead of long `if-else` chains, especially for handling different event types.
- [ ] **Sealed interfaces/classes** used to model closed sets of event types (e.g., `OrderEvent` sealed hierarchy) for exhaustive handling.
- [ ] **Virtual threads** (Project Loom) considered for I/O-bound consumer processing — check if `spring.threads.virtual.enabled=true` is set and whether it's safe given blocking Kafka client calls.
- [ ] **Text blocks** used for multi-line SQL/JSON/log templates instead of string concatenation.
- [ ] No usage of deprecated/removed APIs from pre-17 Java that Java 21 has phased out.
- [ ] `var` used judiciously — improves readability, not overused to the point of obscuring types.
- [ ] Null-safety patterns (`Optional`, pattern matching `instanceof`) used instead of raw null checks.

**Record + sealed hierarchy for event payloads:**
```java
public sealed interface OrderEvent permits OrderCreated, OrderCancelled, OrderShipped {}

public record OrderCreated(String orderId, String customerId, BigDecimal amount, Instant createdAt)
        implements OrderEvent {}

public record OrderCancelled(String orderId, String reason, Instant cancelledAt)
        implements OrderEvent {}

public record OrderShipped(String orderId, String trackingNumber, Instant shippedAt)
        implements OrderEvent {}
```

**Exhaustive pattern matching in the listener (no default branch needed, compiler-enforced):**
```java
public void handle(OrderEvent event) {
    String outcome = switch (event) {
        case OrderCreated c   -> processCreated(c);
        case OrderCancelled x -> processCancelled(x);
        case OrderShipped s   -> processShipped(s);
    };
    log.info("Processed event: {}", outcome);
}
```

**Virtual threads for the listener container executor:**
```yaml
spring:
  threads:
    virtual:
      enabled: true
```
```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
        ConsumerFactory<String, OrderEvent> consumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setListenerTaskExecutor(
            new VirtualThreadTaskExecutor("kafka-virtual-"));
    return factory;
}
```
> ⚠️ Review whether the underlying Kafka client calls (e.g., `consumer.poll()`) or downstream blocking I/O inside pinned virtual threads (`synchronized` blocks) could negate the benefit.

---

## 2. Spring Boot Fundamentals

- [ ] Uses **Spring Boot 3.x** baseline (required for Java 21 support).
- [ ] `spring-kafka` version aligned with Spring Boot's managed dependency version (no manual override causing version drift).
- [ ] Configuration externalized via `application.yml`/`application.properties` — no hardcoded broker URLs, topic names, or credentials in code.
- [ ] Environment-specific configs separated via Spring Profiles (`application-dev.yml`, `application-prod.yml`, etc.).
- [ ] Secrets (SASL credentials, SSL keystores) pulled from a vault/secret manager, not committed to the repo.
- [ ] `@ConfigurationProperties` classes used for strongly-typed Kafka config binding instead of scattered `@Value` injections.
- [ ] Health checks/actuator endpoints (`/actuator/health`) reflect Kafka connectivity status.
- [ ] Graceful shutdown enabled (`server.shutdown=graceful`) so in-flight consumer processing completes before pod/container termination.

**Strongly-typed config binding instead of scattered `@Value`:**
```java
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaProperties(
        String bootstrapServers,
        String ordersTopic,
        String ordersDltTopic,
        int retryMaxAttempts,
        long retryBackoffMs) {}
```
```java
@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaConfig {
    // beans reference kafkaProperties.ordersTopic(), etc.
}
```

**Graceful shutdown + actuator:**
```yaml
server:
  shutdown: graceful

spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s

management:
  endpoint:
    health:
      show-details: always
  health:
    kafka:
      enabled: true
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
```

---

## 3. Kafka Producer Best Practices

- [ ] **Idempotence enabled**: `enable.idempotence=true` (default true in modern clients) to avoid duplicate writes on retries.
- [ ] **Acks** set appropriately: `acks=all` for durability-critical topics; document why if using `acks=1`.
- [ ] **Retries** configured (`retries=Integer.MAX_VALUE` or high value) combined with `delivery.timeout.ms` to bound total retry time.
- [ ] **`max.in.flight.requests.per.connection`** set to `5` or less when idempotence is enabled (required for ordering guarantees with idempotence).
- [ ] Producer uses a **transactional producer** (`transactional.id`) if exactly-once semantics across topics/DB are required — verify `KafkaTransactionManager` is wired correctly.
- [ ] **Key selection strategy** is intentional — partition key chosen to guarantee ordering for related events (e.g., entity ID as key).
- [ ] **Custom partitioner** (if used) is justified and tested; default partitioner is fine for most cases.
- [ ] **Compression** enabled (`compression.type=snappy`/`lz4`/`zstd`) for throughput-sensitive topics.
- [ ] **Batching tuned**: `linger.ms` and `batch.size` set based on latency vs. throughput trade-off, not left at defaults blindly.
- [ ] Producer **callbacks/futures are handled** — failures are logged/alerted, not silently swallowed (`send()` result not ignored).
- [ ] Producer instances are **reused/pooled** via Spring's `KafkaTemplate`, not instantiated per-request.
- [ ] Schema validation in place (Avro/Protobuf + Schema Registry, or explicit JSON schema validation) to prevent malformed payloads.
- [ ] No blocking `.get()` on the producer future inside hot paths without a timeout.

**Producer factory with idempotence, acks, retries, compression, batching:**
```java
@Bean
public ProducerFactory<String, OrderEvent> producerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);

    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32_768);

    return new DefaultKafkaProducerFactory<>(props);
}

@Bean
public KafkaTemplate<String, OrderEvent> kafkaTemplate(ProducerFactory<String, OrderEvent> pf) {
    return new KafkaTemplate<>(pf);
}
```

**Sending with a key for ordering + non-blocking callback handling:**
```java
public void publish(OrderEvent event, String orderId) {
    kafkaTemplate.send("orders", orderId, event)
        .whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event for orderId={}", orderId, ex);
                // route to alerting/metrics; consider a local outbox retry
            } else {
                log.debug("Published to partition={} offset={}",
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
}
```

**Transactional producer (exactly-once across Kafka + DB, e.g. outbox pattern):**
```java
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "orders-producer-tx-");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
```
```java
@Transactional
public void placeOrder(Order order) {
    orderRepository.save(order);
    kafkaTemplate.executeInTransaction(kt -> {
        kt.send("orders", order.getId(), toEvent(order));
        return true;
    });
}
```

---

## 4. Kafka Consumer Best Practices

- [ ] **Consumer group ID** is meaningful and unique per logical service (not shared accidentally across unrelated services).
- [ ] **`enable.auto.commit`** is explicitly set (usually `false`) with manual acknowledgment (`AckMode.MANUAL` or `MANUAL_IMMEDIATE`) for at-least-once correctness.
- [ ] Offset commit happens **after successful processing**, not before (avoids message loss on crash).
- [ ] **Idempotent consumer logic** — processing is safe to repeat (dedup via business key, idempotency table, or upsert semantics) since Kafka guarantees at-least-once by default.
- [ ] **`max.poll.records`** and **`max.poll.interval.ms`** tuned to processing time — avoid consumer being kicked from the group due to slow processing.
- [ ] **Concurrency** configured via `@KafkaListener(concurrency=...)` or `ConcurrentKafkaListenerContainerFactory`, matched to partition count (concurrency ≤ partitions).
- [ ] **`session.timeout.ms`** / **`heartbeat.interval.ms`** reviewed — defaults are usually fine, but flagged if overridden without justification.
- [ ] Listener methods are **non-blocking or offloaded** appropriately — long blocking calls (external HTTP, DB) don't stall the poll loop excessively.
- [ ] **Rebalance listener** (`ConsumerAwareRebalanceListener` / `ConsumerRebalanceListener`) implemented if in-flight state needs cleanup on partition revocation.
- [ ] **Deserialization errors** are handled explicitly (see Error Handling section) — not left to crash the container.
- [ ] Consumer **does not silently skip** poison-pill messages without logging/metrics.
- [ ] **Ordering guarantees understood and documented** — if using `concurrency > 1`, understand that ordering is only preserved per-partition, not per-topic.

**Consumer factory with manual ack, deserializer error wrapping, tuned poll settings:**
```java
@Bean
public ConsumerFactory<String, OrderEvent> consumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-service-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    // Wrap real deserializers so bad bytes don't crash the container
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
    props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
    props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
    props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.events");

    return new DefaultKafkaConsumerFactory<>(props);
}

@Bean
public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
        ConsumerFactory<String, OrderEvent> consumerFactory,
        DefaultErrorHandler errorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setConcurrency(3); // <= partition count
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    factory.setCommonErrorHandler(errorHandler);
    return factory;
}
```

**Listener with manual ack + idempotent processing:**
```java
@KafkaListener(topics = "orders", groupId = "orders-service-group")
public void onOrderEvent(OrderEvent event,
                          @Header(KafkaHeaders.RECEIVED_KEY) String key,
                          Acknowledgment ack) {
    if (processedEventStore.alreadyHandled(event.orderId())) {
        ack.acknowledge();
        return; // idempotent no-op on redelivery
    }
    orderProcessingService.process(event);
    processedEventStore.markHandled(event.orderId());
    ack.acknowledge(); // commit only after successful processing
}
```

---

## 5. Dead Letter Queue / Dead Letter Topic (DLQ/DLT)

- [ ] A **DLT strategy exists** for messages that fail after retries (via `DeadLetterPublishingRecoverer` in Spring Kafka).
- [ ] DLT topic naming convention is consistent (e.g., `<original-topic>.DLT` or `<original-topic>-dlq`).
- [ ] **`DefaultErrorHandler`** (Spring Kafka 2.8+) configured with a `BackOff` policy (e.g., `ExponentialBackOffWithMaxRetries`) before routing to DLT.
- [ ] Retry count and backoff intervals are **explicitly configured**, not left to defaults without review (defaults: no retry, immediate failure — must be intentional).
- [ ] **Non-retryable exceptions** are classified separately (e.g., `DeserializationException`, validation errors) and routed to DLT immediately without wasting retry attempts.
- [ ] **Retryable exceptions** (transient: DB timeout, network blip) are distinguished from non-retryable (business/validation errors) via `addNotRetryableExceptions()` / `addRetryableExceptions()`.
- [ ] DLT messages **preserve original headers** plus exception metadata (exception class, message, stack trace, original topic/partition/offset) — verify via `DeadLetterPublishingRecoverer` header enrichment.
- [ ] There's a **defined process for DLT messages**: reprocessing tool/runbook, alerting on DLT volume, or manual review dashboard — a DLT with no consumer/monitoring is a silent data loss risk.
- [ ] DLT topic has appropriate **retention** (often longer than source topic) so failed messages aren't lost before investigation.
- [ ] Metrics/alerts exist on **DLT publish rate** (spike = upstream/downstream issue).
- [ ] For multi-hop pipelines, consider whether a **DLT replay mechanism** exists to reinsert fixed messages back into the main flow.

**`DefaultErrorHandler` with exponential backoff + DLT routing + retryable/non-retryable classification:**
```java
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {

    // Publishes the failed record + exception headers to "<topic>.DLT"
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));

    ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(5);
    backOff.setInitialInterval(1_000L);
    backOff.setMultiplier(2.0);
    backOff.setMaxInterval(30_000L);

    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

    // Don't waste retries on permanent failures — go straight to DLT
    handler.addNotRetryableExceptions(
            DeserializationException.class,
            IllegalArgumentException.class,
            ValidationException.class);

    // Explicitly retryable (transient) exceptions
    handler.addRetryableExceptions(
            org.springframework.dao.TransientDataAccessException.class,
            java.net.SocketTimeoutException.class);

    handler.setRetryListeners((record, ex, deliveryAttempt) ->
        log.warn("Retry attempt {} for topic={} partition={} offset={} due to {}",
                deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage()));

    return handler;
}
```

**Reprocessing DLT messages back into the main topic (manual runbook tool):**
```java
@KafkaListener(topics = "orders.DLT", groupId = "orders-dlt-reviewer")
public void reviewDlt(ConsumerRecord<String, OrderEvent> record,
                       @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {
    log.info("DLT record key={} exception='{}'", record.key(), exceptionMessage);
    // Persist to a review table / dashboard; replay manually once fixed:
    // kafkaTemplate.send("orders", record.key(), record.value());
}
```

---

## 6. Kafka Configuration Review

Check these are set deliberately (not left as accidental defaults) and documented (comments or config docs) with rationale:

### Producer Configs
| Config | What to verify |
|---|---|
| `bootstrap.servers` | Externalized, environment-specific |
| `acks` | `all` for critical data; justified if not |
| `retries` / `delivery.timeout.ms` | Bounded but resilient |
| `enable.idempotence` | `true` unless explicitly disabled with reason |
| `max.in.flight.requests.per.connection` | ≤5 (compatible with idempotence) |
| `compression.type` | Set for throughput topics |
| `linger.ms` / `batch.size` | Tuned, not default-by-accident |
| `key.serializer` / `value.serializer` | Matches schema strategy (Avro/JSON/String) |

### Consumer Configs
| Config | What to verify |
|---|---|
| `group.id` | Unique, meaningful, stable across deploys |
| `auto.offset.reset` | `earliest` vs `latest` — intentional choice documented |
| `enable.auto.commit` | Usually `false` with manual ack |
| `max.poll.records` | Matched to processing throughput |
| `max.poll.interval.ms` | Greater than worst-case processing time per batch |
| `fetch.min.bytes` / `fetch.max.wait.ms` | Tuned for latency vs throughput |
| `isolation.level` | `read_committed` if producers use transactions |
| `key.deserializer` / `value.deserializer` | Matches producer serialization; error handling wrapped (`ErrorHandlingDeserializer`) |

### Topic-Level Configs
- [ ] **Partition count** justified by expected throughput and consumer parallelism needs.
- [ ] **Replication factor** ≥ 3 in production for durability.
- [ ] **`min.insync.replicas`** set (typically 2) to work with `acks=all`.
- [ ] **Retention policy** (`retention.ms`, `retention.bytes`) matches business/compliance requirements.
- [ ] **Cleanup policy** (`delete` vs `compact`) intentional — compacted topics used correctly for latest-state-per-key use cases.

---

## 7. Error Handling in Kafka

- [ ] **`ErrorHandlingDeserializer`** wraps key/value deserializers to catch deserialization exceptions gracefully instead of crashing the listener container.
- [ ] **`DefaultErrorHandler`** (formerly `SeekToCurrentErrorHandler` pre-2.8) is configured at the container factory level, not left implicit.
- [ ] **Exception classification** is explicit:
  - Retryable (transient infra issues) → retried with backoff.
  - Non-retryable (bad data, business rule violations) → sent to DLT immediately.
- [ ] **Backoff strategy** uses exponential backoff with a max retry cap (`ExponentialBackOffWithMaxRetries`) to avoid hammering downstream systems.
- [ ] **Container-level vs listener-level try/catch**: business logic exceptions are caught inside the listener where recoverable, but unhandled exceptions correctly propagate to the container's error handler.
- [ ] **Global exception logging** captures enough context (topic, partition, offset, key, consumer group) for debugging — not just the stack trace.
- [ ] **Circuit breaker pattern** (e.g., Resilience4j) considered for consumers calling flaky downstream services, to avoid retry storms.
- [ ] **Transactional consistency**: if the consumer writes to both Kafka and a DB, verify handling of partial failure (e.g., outbox pattern, `@Transactional` + Kafka transactions, or manual compensation logic).
- [ ] **No swallowed exceptions** — audit for empty `catch` blocks or catch-log-continue patterns that silently drop failed messages without DLT routing.
- [ ] **Poison pill protection**: a message that can never be processed successfully doesn't cause an infinite retry loop or block the partition indefinitely.
- [ ] **Monitoring/alerting** exists on consumer lag, error rates, and DLT publish rates (Micrometer + Prometheus/Grafana or equivalent).
- [ ] **Testing**: unit/integration tests exist simulating deserialization failures, retryable exceptions, and DLT routing (e.g., using `EmbeddedKafka` or Testcontainers).

**Business-logic try/catch inside the listener (recoverable case handled locally, unrecoverable rethrown to container):**
```java
@KafkaListener(topics = "orders", groupId = "orders-service-group")
public void onOrderEvent(OrderEvent event, Acknowledgment ack) {
    try {
        orderProcessingService.process(event);
        ack.acknowledge();
    } catch (RecoverableInventoryException e) {
        // Handled locally, e.g. queued for a scheduled retry — don't propagate
        log.warn("Recoverable issue for orderId={}, will retry via scheduler", event.orderId(), e);
        ack.acknowledge();
    } catch (Exception e) {
        // Let it propagate — DefaultErrorHandler decides retry vs DLT
        throw new ListenerExecutionFailedException("Processing failed for orderId=" + event.orderId(), e);
    }
}
```

**Circuit breaker around a flaky downstream call (Resilience4j):**
```java
@CircuitBreaker(name = "inventoryService", fallbackMethod = "fallbackReserveStock")
public void process(OrderEvent event) {
    inventoryClient.reserveStock(event.orderId());
}

private void fallbackReserveStock(OrderEvent event, Throwable t) {
    log.warn("Inventory service unavailable, throwing to trigger Kafka retry/backoff", t);
    throw new RecoverableInventoryException("inventory-service down", t);
}
```

**Integration test with `EmbeddedKafka` verifying DLT routing:**
```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"orders", "orders.DLT"})
class OrderConsumerErrorHandlingTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void malformedPayload_isRoutedToDlt() throws Exception {
        kafkaTemplate.send("orders", "order-1", "{not-valid-json}");

        ConsumerRecord<String, String> dltRecord = KafkaTestUtils.getSingleRecord(
                dltConsumer, "orders.DLT", Duration.ofSeconds(10));

        assertThat(dltRecord.value()).contains("not-valid-json");
        assertThat(dltRecord.headers().lastHeader(KafkaHeaders.EXCEPTION_MESSAGE)).isNotNull();
    }
}
```

---

## 8. Quick Red Flags Checklist

Use this as a fast scan for common anti-patterns:

- 🚩 `auto.offset.reset` not set and defaulting silently to `latest` (risk of missed messages on new consumer group).
- 🚩 `enable.auto.commit=true` combined with heavy processing (risk of offset committed before processing completes).
- 🚩 No DLT configured — failed messages either block the partition or are lost.
- 🚩 Catch-all `catch (Exception e) { log.error(...); }` with no rethrow and no DLT routing.
- 🚩 Producer `send()` result/future ignored entirely.
- 🚩 Consumer concurrency greater than partition count (wasted/idle threads).
- 🚩 No idempotency handling in consumer despite at-least-once delivery semantics.
- 🚩 Hardcoded topic names/broker addresses scattered across the codebase instead of centralized config.
- 🚩 No monitoring on consumer lag or DLT volume.
- 🚩 Schema changes made without versioning/compatibility checks (breaking consumers).

---

## 9. Review Sign-off Template

```
Reviewer:        ____________________
Date:             ____________________
Service:          ____________________

[ ] Java 21 practices followed
[ ] Spring Boot config externalized & profiled correctly
[ ] Producer config reviewed & justified
[ ] Consumer config reviewed & justified
[ ] DLQ/DLT strategy present and monitored
[ ] Error handling covers retryable/non-retryable cases
[ ] No red flags identified (or flagged below)

Notes / Follow-ups:
_______________________________________________
_______________________________________________
```
