# Distributed Transactions in Spring Boot: Outbox, Saga & Idempotent Consumers

Once a system splits into services with their own databases, you lose the ACID transaction that used to keep "save the order" and "notify everyone else" atomic. This guide covers the three patterns that replace it: the **Outbox pattern** (solving the dual-write problem at the point of publishing), the **Saga pattern** (coordinating a multi-service business transaction with compensations instead of rollback), and **idempotent consumers** (the safety net both patterns depend on to survive at-least-once delivery). This ties directly into the idempotency work already done for Stripe webhooks — the same core problem, at the scale of your whole event-driven architecture.

---

## 1. The Dual-Write Problem

This is the failure mode every pattern here exists to solve:

```java
@Transactional
public void placeOrder(OrderCommand cmd) {
    Order order = orderRepository.save(new Order(cmd));   // write #1: the database
    kafkaTemplate.send("order.created", order.getId().toString(), toJson(order)); // write #2: Kafka
}
```

This looks fine but writes to **two independent systems that cannot be committed atomically**. If the process crashes, the network blips, or Kafka is unreachable *after* the DB transaction commits but *before* the Kafka send completes — the order exists in your database, but nothing downstream (shipping, notifications, inventory) ever finds out. `@Transactional` only covers the database write; it has no power over the Kafka call.

---

## 2. The Outbox Pattern

### 2.1 Core idea

Stop publishing to Kafka directly from business logic. Instead, write the event to an **outbox table in the same database, in the same local transaction** as the business write. A separate relay process then reads the outbox table and publishes to Kafka asynchronously.

```
App ──▶ DB (business row + outbox row, one transaction)
              │
              ▼
         Relay (poller or CDC)
              │
              ▼
            Kafka ──▶ Consumer
```

Because the business write and the outbox write share one local ACID transaction, they're atomic: either both happen or neither does. The relay's job — moving the row from the outbox table to Kafka — is a separate, independently retryable concern.

### 2.2 Outbox table design

```sql
CREATE TABLE outbox_event (
    id              UUID PRIMARY KEY,
    aggregate_type  VARCHAR(100) NOT NULL,   -- e.g. "Order"
    aggregate_id    VARCHAR(100) NOT NULL,   -- e.g. the order ID — use as the Kafka key
    event_type      VARCHAR(100) NOT NULL,   -- e.g. "OrderCreated"
    payload         JSONB NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    processed_at    TIMESTAMP NULL           -- set by the relay once published (poller approach only)
);
CREATE INDEX idx_outbox_unprocessed ON outbox_event (created_at) WHERE processed_at IS NULL;
```

- **Use `aggregate_id` as the Kafka partition key** (e.g., the order ID) — this preserves per-aggregate ordering (all events for one order land in the same partition, in order) even though global ordering across different aggregates still requires deliberate design.

```java
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;

    @Transactional
    public Order placeOrder(OrderCommand cmd) {
        Order order = orderRepository.save(new Order(cmd));

        outboxRepository.save(new OutboxEvent(
            UUID.randomUUID(),
            "Order",
            order.getId().toString(),
            "OrderCreated",
            toJson(new OrderCreatedPayload(order)),
            Instant.now()
        ));

        return order; // both rows commit together, or neither does
    }
}
```

### 2.3 The relay: poller vs. CDC

**Option A — Poller.** A scheduled job queries unprocessed rows, publishes them, and marks them processed.

```java
@Scheduled(fixedDelay = 500)
public void relayOutboxEvents() {
    List<OutboxEvent> pending = outboxRepository.findUnprocessedBatch(100);
    for (OutboxEvent event : pending) {
        kafkaTemplate.send(topicFor(event), event.getAggregateId(), event.getPayload())
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    outboxRepository.markProcessed(event.getId());
                }
                // on failure: leave unprocessed, next poll retries — this is what makes the relay safe
            });
    }
}
```

Trade-offs:
- **Simplicity** — self-contained in the Spring Boot app, no extra infrastructure.
- **Latency** — bounded by the polling interval.
- **DB load** — constant polling, even when the outbox is empty; tune the interval and batch size to your throughput needs.
- **Multi-instance locking required** — if the service runs more than one instance, you must ensure two instances don't relay the same row concurrently (`SELECT ... FOR UPDATE SKIP LOCKED` in Postgres, or a leader-election pattern).

**Option B — Change Data Capture (Debezium).** A connector reads the database's transaction log (WAL in Postgres, binlog in MySQL) directly and streams inserts on the outbox table straight to Kafka — no application polling at all.

```
App ──▶ DB (outbox table)
              │  (Debezium reads the WAL/binlog directly)
              ▼
        Kafka Connect + Debezium
              │
              ▼
            Kafka
```

Trade-offs:
- **Lower latency**, no polling overhead on the database.
- **The application has zero Kafka dependency** — it only ever writes to its own database; Debezium and Kafka Connect handle everything downstream. This is a real decoupling win for the app itself.
- **More moving infrastructure** — you're now operating a Kafka Connect cluster and Debezium connectors, with their own offsets, replication-slot/log-position monitoring, and recovery story. Worth it at scale; possibly overkill for a low-throughput service.
- **CDC is not "no relay."** The connector, its offsets, the DB's log retention, and Kafka Connect availability collectively *are* the relay — they still need monitoring and an operational runbook, just a different one than a poller.

**Recommendation:** start with a poller for a single-service, moderate-throughput use case — it's the fastest to build and reason about. Move to Debezium CDC once you're already operating Kafka Connect elsewhere, need lower latency, or want to fully decouple the app from any Kafka client dependency.

### 2.4 Cleanup and the outbox's own lifecycle

- **Don't let the outbox table grow forever.** Once the poller confirms `processed_at` is set (or, for CDC, once you're confident the connector has durably captured the row), periodically delete or archive old rows — a scheduled cleanup job, separate from the relay job itself.
- **The outbox guarantees at-least-once delivery to Kafka, not exactly-once.** If the relay publishes successfully but crashes before marking the row processed, the next poll republishes it — a duplicate on the Kafka side. This is expected and by design; it's what Section 4 (idempotent consumers) exists to absorb. Don't mistake the outbox pattern for solving duplication — it only solves *loss*.

---

## 3. The Saga Pattern

### 3.1 Why sagas, not distributed transactions

A two-phase commit (2PC) across services' independent databases doesn't scale and creates tight coupling (every participant must be available and lock resources for the duration). The Saga pattern instead breaks one business transaction into a sequence of **local transactions**, each in a single service, coordinated so that if a later step fails, **compensating transactions** undo the effects of the steps that already succeeded — trading strict atomicity for eventual consistency.

Example: placing an order across Order, Payment, Inventory, and Shipping services.

```
Order Service      → creates order            → OrderCreated
Payment Service     → charges customer         → PaymentProcessed | PaymentFailed
Inventory Service   → reserves stock            → InventoryReserved | InventoryFailed
Shipping Service    → schedules shipment        → ShipmentScheduled
```

If `InventoryReserved` never happens because stock ran out, the saga must **compensate**: refund the payment (undo Payment Service's local transaction) and cancel the order (undo Order Service's) — there's no database rollback that does this for you; each compensation is business logic you write explicitly.

### 3.2 Choreography

Each service publishes an event on completing its local transaction; other services subscribe and react. No central coordinator.

```java
@KafkaListener(topics = "order-events", groupId = "payment-service")
public void onOrderCreated(OrderCreatedEvent event) {
    try {
        paymentService.charge(event.getOrderId(), event.getAmount());
        eventPublisher.publish(new PaymentProcessedEvent(event.getOrderId()));
    } catch (PaymentDeclinedException e) {
        eventPublisher.publish(new PaymentFailedEvent(event.getOrderId(), e.getReason()));
    }
}
```

```java
@KafkaListener(topics = "payment-events", groupId = "order-service")
public void onPaymentFailed(PaymentFailedEvent event) {
    orderService.cancelOrder(event.getOrderId(), event.getReason()); // compensation
}
```

- **Good for**: a small number of participants with simple, mostly-linear flows.
- **Cost**: as the number of participants grows, the "who reacts to what" logic is scattered across every service — there's no single place to read the whole business process, which makes onboarding, debugging, and change harder. Production teams frequently report choreographed sagas becoming difficult to reason about once more than a few services are involved.
- Combine naturally with the Outbox pattern from Section 2 for each publish step — the same dual-write problem applies to every event a saga participant emits.

### 3.3 Orchestration

A central orchestrator issues commands to each participant and reacts to their responses, explicitly tracking saga state.

```java
@Service
public class OrderSagaOrchestrator {

    public void handle(OrderCreatedEvent event) {
        sagaStateRepository.save(new SagaState(event.getOrderId(), SagaStep.AWAITING_PAYMENT));
        commandGateway.send(new ChargePaymentCommand(event.getOrderId(), event.getAmount()));
    }

    @KafkaListener(topics = "payment-results")
    public void handle(PaymentResultEvent event) {
        SagaState state = sagaStateRepository.findByOrderId(event.getOrderId());

        if (event.isSuccess()) {
            state.advanceTo(SagaStep.AWAITING_INVENTORY);
            commandGateway.send(new ReserveInventoryCommand(event.getOrderId()));
        } else {
            state.advanceTo(SagaStep.FAILED);
            commandGateway.send(new CancelOrderCommand(event.getOrderId())); // compensation, step 1 of possibly several
        }
        sagaStateRepository.save(state);
    }

    // Similarly: handle InventoryResultEvent → ScheduleShipment or compensate (RefundPayment + CancelOrder)
}
```

- **Good for**: complex, multi-step flows where you need centralized visibility, monitoring, and a single place to see/change the process. Most production systems converge on orchestration once a saga has more than a handful of steps, precisely because debugging a choreographed flow across many services gets messy fast.
- **The saga's own state must be durable** — persist `SagaState` in a database, not in memory, so an orchestrator restart doesn't lose track of in-flight sagas.
- **Frameworks exist if you don't want to hand-roll this**: Axon Saga (widely used with Spring Boot), Eventuate Tram Sagas, or a workflow engine like Camunda for BPMN-modeled processes — worth evaluating once you have more than 2–3 sagas in the codebase, since state tracking, timeouts, and retries are largely the same boilerplate every time.

### 3.4 Designing compensations

- **Every step that can succeed must have a corresponding compensating action**, defined at design time — not improvised after the first production failure. If a step is fundamentally non-compensatable (e.g., an email already sent), either make it the *last* step in the saga, or design an explicit remediation (a follow-up "correction" notification) rather than pretending it can be undone.
- **Compensations must themselves be idempotent** (Section 4) — a compensation can be triggered more than once by the same at-least-once delivery guarantees that affect every other step.
- **Order of compensation matters.** Undo in reverse order of execution — refund payment before (or as part of) cancelling the order that justified the charge, not after leaving the system briefly inconsistent in a way another process might observe.
- **Use a correlation ID (the saga/order ID) on every event and log line** in the saga — this is what makes it possible to reconstruct "what happened to order X" across every participant when something goes wrong. Pairs directly with the trace correlation from the observability guide.
- **Design explicit timeouts.** If a participant never responds (crashed, message lost despite your relay), the saga needs a timeout that triggers either a retry or a compensation — an orchestrator (or a choreography participant) that waits forever for an event that will never come is a silent, permanent stuck-saga bug.

---

## 4. Idempotent Consumers

Both patterns above guarantee **at-least-once** delivery, never exactly-once. Every consumer in the system — saga participants, Kafka listeners, webhook handlers — must be safe to invoke twice with the same message. This is the exact problem already solved for Stripe webhooks; the same pattern applies system-wide.

### 4.1 The core pattern: dedup key + same transaction

```java
@KafkaListener(topics = "order-events", groupId = "inventory-service")
@Transactional
public void onOrderCreated(OrderCreatedEvent event) {

    // Insert-or-skip on a UNIQUE constraint — the DB enforces dedup, avoiding a race
    // between two near-simultaneous redeliveries both "winning" a check-then-act.
    boolean isNew = processedMessageRepository.insertIfAbsent(event.getEventId());

    if (!isNew) {
        return; // already handled — no-op, ack and move on
    }

    inventoryService.reserveStock(event.getOrderId(), event.getItems()); // the actual business work

    // The dedup record and the business write are in the SAME transaction as the block above —
    // if the process crashes between them, the whole transaction rolls back and the next
    // redelivery retries cleanly, rather than leaving a half-done, half-recorded state.
}
```

```sql
CREATE TABLE processed_message (
    message_id   VARCHAR(255) PRIMARY KEY,   -- the event's own ID, not a locally generated one
    processed_at TIMESTAMP NOT NULL
);
```

- **Key the dedup check on the event's own ID** (`event.getEventId()`, set by whoever produced it — e.g., a UUID assigned at outbox-write time in Section 2.2), not on a derived or reconstructed key — the exact same lesson as the Stripe `event.id` deduplication.
- **The dedup write and the business write must share one transaction.** This is the single most-missed detail across every idempotency implementation reviewed for this and the Stripe guide: doing them as two separate steps leaves a gap where a crash produces "did the work, didn't record it" — and the next retry duplicates the work.
- **A DB unique constraint is usually sufficient** at moderate volume; move to a Redis-based dedup (`SETNX`) with a TTL beyond your broker's max redelivery window at high volume, to avoid DB contention — same trade-off already noted for Stripe webhook dedup.

### 4.2 Alternative: idempotent-by-design business logic

Sometimes you don't need a separate dedup table at all — if the business operation is naturally idempotent, a duplicate message is harmless:

```java
// UPSERT keyed on the order ID is naturally idempotent — processing the same
// OrderCreated event twice just writes the same row twice, no different outcome.
@Transactional
public void onOrderCreated(OrderCreatedEvent event) {
    orderReadModelRepository.upsert(event.getOrderId(), event.toReadModel());
}
```

- Prefer this where it fits (projections/read models via UPSERT, state-transition checks like `if (order.status == PENDING) transitionTo(CONFIRMED)`) — it's simpler than maintaining a dedup table.
- It does **not** apply to operations with real side effects outside your own database — charging a card, sending an email, calling another service's non-idempotent API. Those need the explicit dedup-table approach from Section 4.1, or an idempotency key passed to the downstream call itself (exactly the Stripe `RequestOptions.setIdempotencyKey` pattern).

### 4.3 Consumer-side ordering and partial failure

- **Process messages from the same partition in order**, and don't acknowledge/commit an offset until the message is fully and durably handled — an early commit followed by a crash mid-processing is a silent message loss, the mirror image of the duplication problem this whole section addresses.
- **Poison messages need a dead-letter path.** A message that fails processing every time (a permanent bug, not a transient failure) will otherwise block the partition forever under a naive retry-forever policy — route to a dead-letter topic after a bounded number of attempts, and alert on DLT volume (ties into the SLO/alerting guide).
- **Idempotency and retry-safety are two sides of the same coin as the Stripe outbound-call guidance**: there, a stable idempotency key made *your* retried request safe; here, a stable dedup key makes *the broker's* retried delivery safe. Both exist because at-least-once is the honest guarantee distributed systems can make, and idempotency is how you get exactly-once-processing semantics on top of it without needing the infrastructure to promise something it can't.

---

## 5. How the Three Patterns Fit Together

A realistic order-processing flow uses all three at once:

1. **Order Service** receives a request, saves the order, and writes an `OrderCreated` row to its **outbox** in one local transaction (Section 2).
2. The outbox **relay** (poller or Debezium) publishes `OrderCreated` to Kafka — at-least-once, possibly duplicated.
3. **Payment Service**'s consumer is **idempotent** (Section 4): even if `OrderCreated` arrives twice, the customer is charged once (backed by a Stripe idempotency key on the outbound charge call, as in the Stripe payments guide).
4. The overall **saga** (Section 3, orchestrated or choreographed) tracks the order through Payment → Inventory → Shipping, triggering **compensations** — themselves published via each service's own outbox, and themselves idempotently consumed — if any step fails.

None of these three patterns is optional once you've adopted the others: the outbox solves loss, idempotent consumers solve duplication, and the saga coordinates the multi-step business process built on top of both guarantees.

---

## 6. Summary Checklist

**Outbox**
- [ ] Business write and outbox write share one local database transaction
- [ ] `aggregate_id` used as the Kafka partition key to preserve per-aggregate ordering
- [ ] Relay choice (poller vs. Debezium CDC) matches actual throughput/latency/ops-maturity needs, not just novelty
- [ ] Multi-instance pollers use `SELECT ... FOR UPDATE SKIP LOCKED` or equivalent to avoid double-relay
- [ ] Outbox table has a cleanup/archival job, separate from the relay job
- [ ] Understood explicitly: outbox guarantees no loss, not no duplication

**Saga**
- [ ] Choreography vs. orchestration chosen deliberately based on participant count and need for centralized visibility, not by default
- [ ] Every non-terminal step has a defined, idempotent compensating action
- [ ] Saga state (for orchestration) persisted durably, not held in memory
- [ ] Explicit timeouts defined for steps awaiting a response that may never arrive
- [ ] Correlation ID present on every event/log line in the saga for cross-service tracing

**Idempotent consumers**
- [ ] Dedup keyed on the event's own ID, not a locally derived key
- [ ] Dedup record write and business-logic write share one transaction
- [ ] Naturally idempotent operations (UPSERT, state-transition checks) preferred where they fit; explicit dedup table used where side effects require it
- [ ] Outbound calls from within a consumer (e.g., to Stripe, to another service) carry their own idempotency key, layered on top of consumer-level dedup
- [ ] Offsets committed only after durable, complete processing
- [ ] Dead-letter topic and alerting in place for messages that fail repeatedly
