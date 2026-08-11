# Observability in Spring Boot: OpenTelemetry, Structured Logging, Distributed Tracing & SLOs

This guide covers instrumenting a Spring Boot application end-to-end with OpenTelemetry, correlating structured logs with traces, propagating trace context across the trickiest boundaries (Kafka, AWS SDK calls, LLM calls), and designing SLOs/alerts on top of it. It builds on the Kafka, AWS, and LLM-integration reviewers already in the repo — this is the observability layer that ties all three together into one traceable call chain.

> **Note on Spring's tracing stack:** Spring Cloud Sleuth is dead — it ended with the Spring Boot 2.x line. Tracing now lives in **Micrometer Tracing** (the vendor-neutral facade) bridged to **OpenTelemetry** via `micrometer-tracing-bridge-otel`. If you find a tutorial that adds `spring-cloud-starter-sleuth`, it's outdated — skip it.

---

## 1. OpenTelemetry Instrumentation Setup

### 1.1 Dependencies

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>

<!-- Bridges Micrometer's Observation/Tracing API to OpenTelemetry -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-tracing-bridge-otel</artifactId>
</dependency>

<!-- Exports traces via OTLP (HTTP by default; gRPC also supported) -->
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-exporter-otlp</artifactId>
</dependency>

<!-- Metrics: Micrometer + a registry that can also export via OTLP -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-otlp</artifactId>
</dependency>
```

> Spring Boot Actuator auto-configures the `OpenTelemetry` SDK bean and wires an `OtlpHttpSpanExporter` automatically once these dependencies are present — no manual `SdkTracerProvider` setup needed for the common case.

### 1.2 Configuration

```yaml
management:
  tracing:
    sampling:
      probability: 1.0        # 100% in dev; tune down in prod (Section 1.4)
  opentelemetry:
    resource-attributes:
      service.name: order-service
      service.namespace: commerce
      deployment.environment: ${ENVIRONMENT:local}
    tracing:
      export:
        otlp:
          endpoint: http://otel-collector:4318/v1/traces
  otlp:
    metrics:
      export:
        url: http://otel-collector:4318/v1/metrics
        step: 15s
```

- **Route through an OpenTelemetry Collector**, not directly to your observability backend (Jaeger/Tempo/Datadog/etc.) from every service instance. The Collector centralizes sampling policy, batching, retry, and backend-swapping — pointing every service straight at a vendor endpoint couples your whole fleet to that vendor's ingestion API.
- **Set `service.name` and `deployment.environment` explicitly** per service — these are the primary dimensions you'll filter/group by in your tracing backend; don't rely on defaults that make every service look alike.

### 1.3 Auto-instrumentation vs. custom spans

Spring Boot's Observation API auto-instruments the common integration points out of the box once tracing dependencies are present: inbound HTTP requests (`@RestController`), outbound `RestClient`/`WebClient` calls, JDBC (with `datasource-micrometer` or driver-level support), and — as covered below — Kafka producers/consumers when observation is explicitly enabled on them.

For anything auto-instrumentation doesn't cover — a specific business operation, a call to a third-party SDK, an LLM call — add custom spans via the `Observation` API rather than reaching for the raw OpenTelemetry API directly (Spring's own guidance: prefer the Micrometer Observation/Tracing API over touching OTel's API so your instrumentation stays portable across tracer backends):

```java
@Service
public class PricingService {

    private final ObservationRegistry observationRegistry;

    public PricingService(ObservationRegistry observationRegistry) {
        this.observationRegistry = observationRegistry;
    }

    public Price calculatePrice(Order order) {
        return Observation.createNotStarted("pricing.calculate", observationRegistry)
            .lowCardinalityKeyValue("order.type", order.getType().name())
            .highCardinalityKeyValue("order.id", order.getId().toString())
            .observe(() -> pricingEngine.calculate(order));
    }
}
```

- **Low-cardinality vs. high-cardinality tags matter.** Tags meant to become *metric dimensions* (via `@Timed`/`@Counted` or `Observation` used with a `MeterRegistry`) must stay low-cardinality (order type, status, region) — a high-cardinality tag like `order.id` on a metric will explode your time-series database. High-cardinality identifiers belong on **span attributes** (fine, since spans aren't aggregated the way metrics are), which `highCardinalityKeyValue` correctly routes to traces only.

### 1.4 Sampling strategy

- **Never ship 100% sampling to production** at meaningful traffic volume — it's expensive to store and mostly noise. Use head-based sampling (`management.tracing.sampling.probability`) at a modest rate (1–10%, tuned to traffic volume and cost), or tail-based sampling at the Collector level if you need to guarantee that error/slow traces are always kept regardless of the head-sampling decision.
- **Force-sample the traces you actually need to see**: use a custom sampler or per-request override to always capture traces for errors, requests above a latency threshold, or requests carrying a debug flag — random sampling alone will under-represent exactly the traces you want most when debugging an incident.

---

## 2. Structured Logging

### 2.1 JSON structured logs with automatic trace correlation

Spring Boot 3.4+ ships **built-in structured logging support** — no need to hand-roll a Logback JSON encoder:

```yaml
logging:
  structured:
    format:
      console: ecs        # or "logstash", "gelf" — pick per your log pipeline
  level:
    root: INFO
```

With tracing configured (Section 1), Spring's logging integration automatically adds `traceId` and `spanId` into the MDC (Mapped Diagnostic Context) for any log statement emitted inside an active span — no manual `MDC.put` calls required for the synchronous request path.

```json
{
  "@timestamp": "2026-08-11T09:14:22.101Z",
  "log.level": "INFO",
  "message": "Order created",
  "service.name": "order-service",
  "trace.id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span.id": "00f067aa0ba902b7",
  "order.id": "ORD-10293"
}
```

### 2.2 Define an event contract before you ship JSON

- **Agree on a field schema up front** (service name, environment, trace/span IDs, a stable `event.type` or `message` taxonomy, and a small set of domain fields) rather than letting each service log whatever fields a developer feels like at the time — inconsistent field names (`orderId` vs `order_id` vs `order.id`) across services silently break cross-service log search.
- **Don't assume a console-pattern tutorial proves your JSON reaches the central store correctly.** Verify structured fields survive the full path — app → log shipper → index — not just that they print correctly to stdout in a local test.
- **Test correlation on three separate paths, not just one:** an inbound HTTP request, an outbound client call, and an asynchronous handoff (executor task, scheduled job, Kafka record, retry callback, application event). The first two usually work automatically via framework instrumentation; the third is where trace context silently disappears if you don't handle it explicitly (Section 3.3).

### 2.3 What never belongs in structured logs

- Full JWTs/access tokens, passwords, card numbers, or other secrets/PII (ties directly to the OWASP A09 logging-failures and A04 cryptographic-failures categories from the security checklist) — redact or omit before the log line is built, not after.
- Unbounded free-text blobs (full LLM prompts/responses, full HTTP bodies) — log a truncated summary or a reference ID and pull full payloads from a separate, access-controlled store if genuinely needed for debugging.

---

## 3. Distributed Tracing Across Kafka, AWS, and LLM Call Chains

This is the part that differs meaningfully from a single-service HTTP tutorial — context propagation breaks by default at exactly these three boundaries.

### 3.1 Kafka: producer and consumer instrumentation

Trace context does **not** propagate across Kafka automatically until you explicitly enable observation on both the producer and consumer:

```java
@Bean
public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
    KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory);
    template.setObservationEnabled(true); // propagates trace context into record headers
    return template;
}

@Bean
public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
        ConsumerFactory<String, Object> consumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setObservationEnabled(true); // extracts trace context, continues the trace
    return factory;
}
```

With both flags set, Micrometer injects trace context into Kafka record headers on publish and extracts it on consume — the producer's span and the consumer's span join the same trace automatically.

**Where it still breaks: async hops inside the consumer.** If your `@KafkaListener` hands work off to another thread (`CompletableFuture.runAsync`, an `@Async` method, a thread-pool executor), the trace context is thread-bound and does **not** cross automatically — you'll see the consumer span, then a orphaned/disconnected span for the async work:

```java
@KafkaListener(topics = "order-events", groupId = "payment-service")
public void onOrderEvent(OrderEvent event) {
    // Capture the current context BEFORE crossing threads
    ContextSnapshot snapshot = ContextSnapshotFactory.builder().build().captureAll();

    CompletableFuture.runAsync(() -> {
        try (ContextSnapshot.Scope scope = snapshot.setThreadLocals()) {
            paymentProcessor.process(event); // now runs with the original trace context restored
        }
    }, executor);
}
```

- **Decide explicitly, per consumer, whether a message continues the producer's trace or starts a new one linked to it.** For a tightly coupled request/response-style flow (order → payment), continuing the same trace usually gives the clearest picture. For a loosely coupled fan-out (an event with many independent downstream consumers, some processed much later), a **linked** trace (new trace ID, with a link back to the originating trace) often makes more sense — one giant trace spanning an event that gets consumed hours later by a batch job is not useful.
- Kafka Streams instrumentation is more invasive — current tracing libraries generally require manually wrapping transformations/processors to maintain context; budget extra time if your pipeline uses the Streams DSL rather than plain producer/consumer.

### 3.2 AWS SDK calls

The AWS SDK for Java v2 has built-in support for OpenTelemetry instrumentation of outbound calls (S3, DynamoDB, SQS, SNS, etc.) via the AWS SDK's OpenTelemetry integration:

```xml
<dependency>
    <groupId>io.opentelemetry.instrumentation</groupId>
    <artifactId>opentelemetry-aws-sdk-2.2</artifactId>
</dependency>
```

```java
@Bean
public S3Client s3Client(OpenTelemetry openTelemetry) {
    return S3Client.builder()
        .overrideConfiguration(ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(AwsSdkTelemetry.create(openTelemetry).newExecutionInterceptor())
            .build())
        .build();
}
```

- This gives you a child span per AWS API call (S3 `PutObject`, DynamoDB `Query`, SQS `SendMessage`, etc.) nested under the request trace, with AWS-specific attributes (bucket name, table name, request ID) attached — invaluable when a request is slow and you need to know whether it was your code or an AWS call that stalled.
- **For SQS specifically, propagation across the queue works the same way Kafka does** — trace context can be injected into message attributes on send and extracted on receive; without wiring this explicitly, a trace stops at the point a message is queued and a new, disconnected trace starts when it's dequeued, same failure mode as Section 3.1.
- **Correlate AWS request IDs into your spans as attributes** (`aws.request.id`) — when you need to open an AWS support case or grep CloudTrail, this is what bridges your trace to AWS's own logs.

### 3.3 LLM call chains (OpenAI/Gemini via Spring AI)

LLM calls are usually the **slowest, most expensive, and most failure-prone** spans in a request — instrument them deliberately, not as an afterthought:

```java
@Service
public class LlmChatService {

    private final ChatClient chatClient;
    private final ObservationRegistry observationRegistry;

    public String ask(String prompt) {
        return Observation.createNotStarted("llm.chat", observationRegistry)
            .lowCardinalityKeyValue("llm.provider", "openai")
            .lowCardinalityKeyValue("llm.model", "gpt-4o-mini")
            .observe(() -> {
                ChatResponse response = chatClient.prompt().user(prompt).call().chatResponse();

                Observation.Context context = observationRegistry.getCurrentObservation().getContext();
                context.addLowCardinalityKeyValue(KeyValue.of("llm.finish_reason",
                    response.getResult().getMetadata().getFinishReason()));

                return response.getResult().getOutput().getContent();
            });
    }
}
```

Spring AI's `ChatClient` already emits Micrometer observations for chat calls out of the box in recent versions — verify what your version auto-instruments before adding a fully manual span, and layer custom attributes on top rather than duplicating the whole span.

**Attributes worth capturing on every LLM span:**

| Attribute | Why |
|---|---|
| `llm.provider`, `llm.model` | Cost and performance vary enormously by model — you need this to explain a spike in latency or spend. |
| `llm.prompt.tokens`, `llm.completion.tokens` | Token counts are your primary cost driver; track them per-span to catch a prompt-bloat regression. |
| `llm.finish_reason` | Distinguishes a clean completion from a truncation (`length`) or a content-filter stop — both look like "success" if you only check for an exception. |
| `llm.latency_ms` | Captured automatically by the span duration, but worth a dedicated metric too since LLM latency has a very different (long-tailed) distribution than typical HTTP calls. |
| **Never**: full prompt/response text as a span attribute | Attributes are exported to your tracing backend and often retained/indexed differently than logs — treat prompt/response content the same as the logging guidance in Section 2.3: reference IDs, not raw content, unless your backend is specifically approved for it. |

- **This span is also where RAG retrieval and MCP tool calls should nest**, per the guides already in the repo — a single user request that does retrieval → LLM call → tool call should show up as one trace with clearly delineated child spans for each stage, so a slow response can be diagnosed as "retrieval was slow" vs. "the model was slow" vs. "a tool call hung" at a glance.
- **Correlate cost, not just latency.** Emit a Micrometer counter/summary for token usage per model/provider (low-cardinality tags only) so token spend is visible on the same dashboards as latency and error rate — this is the metric a slow LLM incident usually turns into a budget conversation about.

### 3.4 Custom header propagation (non-standard/legacy clients)

If an upstream client sends trace identifiers in non-standard headers instead of the W3C `traceparent` header OTel expects by default, implement a custom `TextMapPropagator` to extract/inject them rather than asking every client to change — this is common when integrating with legacy systems or partners who standardized on a different tracing header before you adopted OTel.

---

## 4. SLO & Alerting Design

Tracing and structured logs tell you *what happened*; SLOs and alerts tell you *when to care*. Build these deliberately on top of the telemetry above rather than alerting on raw infrastructure metrics alone.

### 4.1 Define SLIs before SLOs

Pick a small number of **Service Level Indicators** that reflect what users actually experience, derived from the spans/metrics you're already emitting:

- **Availability**: proportion of requests that did not return a server error (5xx, or a business-level failure like "payment could not be processed").
- **Latency**: proportion of requests under a target duration (e.g., p95 < 300ms for a synchronous API, or p95 < 5s for an LLM-backed endpoint — LLM-backed SLOs need separate, more generous latency budgets than pure-CRUD endpoints; don't hold them to the same target).
- **Correctness/Freshness** (where relevant): e.g., proportion of Kafka-consumed events processed within N seconds of being published — measurable directly from the trace timestamps captured in Section 3.1.

### 4.2 Set SLOs and error budgets

- **An SLO is a target on an SLI over a rolling window** (e.g., "99.5% of requests succeed within 300ms, measured over a rolling 28 days") — not a single dashboard threshold. Pick windows long enough to be statistically meaningful but short enough to catch a regression before it becomes normal.
- **Track an error budget** (the allowed 0.5% failure rate in the example above) and treat its consumption rate as a leading indicator — a service burning its monthly budget in the first three days needs attention regardless of whether any single alert has fired yet.
- **Set separate SLOs per critical path**, not one blanket SLO for the whole service — an LLM chat endpoint, a payment endpoint, and a health-check endpoint have entirely different acceptable latency and failure profiles; a shared SLO either over-promises on the slow path or under-alerts on the fast one.

### 4.3 Alert on burn rate, not just static thresholds

- **Prefer multi-window, multi-burn-rate alerting** (the pattern popularized by Google's SRE workbook) over a single static threshold: alert loudly and fast when the error budget is being consumed at a rate that would exhaust it in hours, and alert more quietly when it's being consumed at a rate that would exhaust it over days — this distinguishes "call someone now" from "look at this during business hours" automatically from the same underlying SLI.
- **Alert on symptoms visible to users, not on every internal cause.** A single upstream dependency blip that your retry logic (from the Stripe/resilience guidance) already absorbs shouldn't page anyone — alert when *user-visible* SLOs are actually threatened, and use traces/logs for root-causing after the fact, not as the alert trigger itself.
- **Tie alerts back to the trace/log correlation you built in Sections 1–3.** An alert that fires with a trace ID (or a link to a pre-built query filtered to the affected time window and service) turns a 3am page into "click the link, see the slow span" instead of a cold start through dashboards.

### 4.4 Dashboards: the three pillars, correlated

- Build dashboards that let you pivot from a metric spike → to the traces that make it up → to the logs for a specific slow/failed trace, using the shared `trace.id`/`service.name` fields established in Sections 1–2. Siloed dashboards (a metrics dashboard with no path to the underlying traces) turn every investigation into manual cross-referencing.
- Include your Kafka consumer lag, AWS SDK call latency/error rate, and LLM token spend/latency as first-class panels alongside standard HTTP metrics — these are exactly the boundaries most likely to be the actual root cause given this stack, per Section 3.

---

## 5. Summary Checklist

**Instrumentation**
- [ ] Micrometer Tracing + `micrometer-tracing-bridge-otel`, exporting via OTLP to a Collector (not directly to a vendor backend)
- [ ] `service.name` / `deployment.environment` set explicitly per service
- [ ] Custom spans added via the Observation API for business-critical operations not auto-instrumented
- [ ] High-cardinality identifiers kept on span attributes, never on metric tags
- [ ] Sampling rate tuned for production volume/cost; error and slow traces force-sampled

**Structured logging**
- [ ] JSON structured logging enabled (Spring Boot's built-in support, ECS/logstash/GELF format)
- [ ] `traceId`/`spanId` confirmed present in logs across sync HTTP, outbound client, and async/Kafka paths — tested independently, not assumed
- [ ] Field schema agreed across services before shipping (no `orderId` vs `order_id` drift)
- [ ] Secrets, tokens, and full prompt/response bodies excluded from log payloads

**Distributed tracing across Kafka/AWS/LLM**
- [ ] `setObservationEnabled(true)` on both Kafka producer and consumer factories
- [ ] `ContextSnapshot` used to carry trace context across any thread hop inside a Kafka listener
- [ ] Explicit decision per consumer: continue the trace vs. start a linked trace
- [ ] AWS SDK v2 OpenTelemetry instrumentation added; AWS request IDs captured as span attributes
- [ ] LLM calls wrapped in spans with provider/model/token/finish-reason attributes — never raw prompt/response content
- [ ] RAG retrieval and MCP tool calls nest as child spans under the originating LLM request span

**SLOs & alerting**
- [ ] SLIs defined per critical path from real telemetry, not generic infra metrics
- [ ] SLOs set as rolling-window targets with tracked error budgets, separate per critical path
- [ ] Multi-window burn-rate alerting in place, not single static thresholds
- [ ] Alerts fire on user-visible symptoms, not every absorbed internal failure
- [ ] Alerts link directly to correlated traces/logs for the affected window
