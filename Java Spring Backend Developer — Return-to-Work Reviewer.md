# Java Spring Backend Developer — Return-to-Work Reviewer

**Author:** Manus AI | **Date:** August 2026

This reviewer is designed as a focused refresher for a Java Spring backend developer returning to work after a two-week break. It covers the core knowledge areas you are likely to touch in a modern backend role: Java 21, Spring Framework and Spring Boot, REST and HTTP, general backend fundamentals, deployment strategies, observability, Kubernetes for developers, microservices architecture, resiliency patterns, and essential AWS services. Each section is written to be skimmed quickly while still containing enough depth for interview-style questions and day-to-day decision-making.

---

## Table of Contents

1. [Java 21 Essentials](#1-java-21-essentials)
2. [Spring Framework and Spring Boot](#2-spring-framework-and-spring-boot)
3. [REST Concepts and HTTP Status Codes](#3-rest-concepts-and-http-status-codes)
4. [Backend Fundamentals](#4-backend-fundamentals)
5. [Deployment Strategies](#5-deployment-strategies)
6. [Observability](#6-observability)
7. [Kubernetes for Developers](#7-kubernetes-for-developers)
8. [Microservices Architecture](#8-microservices-architecture)
9. [Resiliency Patterns](#9-resiliency-patterns)
10. [Basic AWS Services](#10-basic-aws-services)
11. [Quick-Reference Cheat Sheets](#11-quick-reference-cheat-sheets)
12. [References](#references)

---

## 1. Java 21 Essentials

Java 21 (released September 19, 2023) is the current LTS release after Java 17 and is the baseline for modern Spring Boot 3.x development. Its headline feature is **virtual threads (Project Loom)**, which fundamentally changed how JVM applications handle high concurrency. Many language features also reached their final form in Java 21, building on foundations laid in Java 17 [1] [2].

### 1.1 Virtual Threads (JEP 444)

Virtual threads are lightweight threads managed by the JVM rather than the operating system. Unlike platform threads, which pin to an OS thread for their entire lifetime and thus become a scarce resource (typically thousands per machine), millions of virtual threads can be created cheaply. This makes the traditional **one thread per request** model of Spring MVC economically viable at enormous scale, without resorting to reactive programming [2].

```java
// Launching virtual threads
Thread.startVirtualThread(() -> doWork());

try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    executor.submit(() -> fetchUser(id));
}

// Spring Boot 3.2+ makes this automatic for the web layer:
// spring.threads.virtual.enabled=true
```

Two caveats matter in practice. First, **pinning**: code inside `synchronized` blocks or calling native foreign functions pins the carrier thread, reducing the benefit — prefer `ReentrantLock` for hot paths. Second, thread-local variables are far more expensive on virtual threads, so avoid them in request-scoped code.

### 1.2 Records

Records (final since Java 16) are immutable data carriers with compiler-generated constructor, accessors, `equals()`, `hashCode()`, and `toString()`. They are ideal for DTOs, events, and entities where state identity equals field identity — effectively a Lombok-`@Data` replacement without a library dependency [3].

```java
public record OrderDto(UUID id, String product, int quantity) {}
```

### 1.3 Sealed Classes (final since Java 17)

Sealed types let a class or interface declare exactly which subtypes may extend it, using the `permits` clause. This gives the compiler full knowledge of the type hierarchy, which powers exhaustive `switch` matching — a natural fit for modeling domain states and events [3].

```java
public sealed interface PaymentStatus
        permits Pending, Completed, Failed, Refunded {}
public final record Pending(Instant at) implements PaymentStatus {}
```

### 1.4 Pattern Matching (`instanceof` and `switch`, final in Java 21)

Java 21 finalized both **record patterns** (JEP 440) and **pattern matching for `switch`** (JEP 441). Record patterns destructure a record directly in the match; switch patterns allow type, value, and null matching with guarded `when` clauses [1].

```java
// Record pattern — destructure in one step
if (obj instanceof Point(int x, int y)) {
    return x + y;
}

// Switch with patterns and guards
switch (order) {
    case null            -> throw new IllegalArgumentException("null order");
    case CancelledOrder c when c.reason() == Reason.FRAUD
                           -> handleFraud(c);
    case CancelledOrder c -> handleCancellation(c);
    case ShippedOrder s   -> track(s);
}
```

Combined with sealed interfaces, the compiler now **warns when a switch is not exhaustive** — a powerful way to make new domain cases visible at compile time rather than at runtime.

### 1.5 Other Notable Java 21 Features

| Feature (JEP) | Status in 21 | What it gives you |
|---|---|---|
| Sequenced Collections (431) | Final | `first()`, `last()`, `reversed()` on `List`, `Deque`, `SortedSet` |
| String Templates (430) | Preview | `STR."Hello \{name}"` — embedded expressions in string literals |
| Generational ZGC (439) | Final | Separate young-generation collection, dramatically lower allocation latency |
| Structured Concurrency (453) | Preview | Treat concurrent subtasks as a single unit of work; cancel all on failure |
| Unnamed Variables (456) | Final | `var _ = expensive();` when the value is intentionally ignored |
| Unnamed Classes & `main` (445/463) | Preview | Single-file scripts: `void main() { IO.println("hi"); }` runnable with `java script.java` [4]. |

### 1.6 Java 17 Foundations You Must Also Know

Since Java 21 features compose with Java 17 features, be comfortable with: **text blocks** (`"""..."""` for multi-line strings, ideal for JSON/SQL/GraphQL), **`var` local type inference**, **switch expressions** (`->` arrow syntax returning values), **enhanced `instanceof`** with pattern variables, and the **Stream API** (intermediate vs terminal operations, `collect`, `flatMap`, `groupingBy`).

### 1.7 Performance Angle: GraalVM Native and AOT

Java 21 plus GraalVM native image (or the newer **Project Leyden / AOT cache** mechanism) can cut Spring Boot startup from ~30 seconds to a few milliseconds and shrink memory use dramatically — the classic case being 6.6 GB down to ~1 GB for the same throughput [3]. This is what makes Java viable in serverless (AWS Lambda) and fast-scaling Kubernetes environments.

---

## 2. Spring Framework and Spring Boot

### 2.1 The Spring Family

| Project | Role |
|---|---|
| **Spring Framework** | The core: DI/IoC, AOP, MVC, transactions, data access |
| **Spring Boot** | Opinionated auto-configuration, embedded servers, starters, actuator |
| **Spring Data JPA** | Repository abstraction over JPA/Hibernate |
| **Spring Security** | Authentication, authorization, OAuth2/OIDC, CSRF, method security |
| **Spring Cloud** | Distributed system patterns (config, discovery, gateway, circuit breaker) |
| **Spring WebFlux** | Reactive, non-blocking stack (Project Reactor) |
| **Micrometer** | Metrics facade (prometheus registry, actuator integration) |
| **Spring Modulith** | Modular monolith verification and documentation |

### 2.2 Core Concepts

**Inversion of Control (IoC) and Dependency Injection.** Instead of objects creating their dependencies, the Spring container (the `ApplicationContext`) creates beans and wires them. Injection happens via constructor injection (preferred — testable, immutable), setter injection, or field injection (`@Autowired`, discouraged for new code).

**Beans and configuration.** A bean is any object managed by the container. Define beans via `@Bean` in `@Configuration` classes, or through stereotype annotations (`@Component`, `@Service`, `@Repository`, `@Controller`) plus component scanning. `@ConfigurationProperties` binds `application.yml` values to typed POJOs, which is safer than scattering `@Value` annotations.

**Bean scopes.** `singleton` (default — one per container), `prototype` (new instance per injection), `request`, `session`, and `application`.

**AOP (Aspect-Oriented Programming).** Cross-cutting concerns (logging, transactions, security) are applied as aspects through proxying. This matters in interviews: Spring creates **JDK dynamic proxies for interfaces** and **CGLIB proxies for concrete classes**. Self-invocation (a method in a bean calling its own `@Transactional` method) bypasses the proxy, so the annotation is silently ignored — a classic gotcha.

### 2.3 Spring Boot Highlights

Spring Boot's value proposition is convention over configuration: an embedded servlet container (Tomcat by default), auto-configuration conditional on classpath contents, production-ready `/actuator` endpoints, and externalized configuration (application properties/YAML, environment variables, profiles).

**Release awareness (as of late 2025):**

| Version | Key additions |
|---|---|
| **3.2** (Nov 2023) | First-class virtual threads (`spring.threads.virtual.enabled=true`), `RestClient`, HTTP interfaces [5] |
| **3.3** (May 2024) | Class Data Sharing (CDS) for faster startup, SBOM actuator endpoint, Prometheus 1.x metrics, `@SpanTag` observability [6] |
| **3.4** (Nov 2024) | Enhanced `@ServiceConnection` testcontainers support, RestClient auto-configured for Reactor Netty / JDK HttpClient |
| **4.0** (Nov 2025) | Declarative HTTP clients (`@HttpExchange`), API versioning support, consolidated Spring Security 7 (built-in MFA, WebAuthn/passkeys), modularized boot jars [4] |

**Important migration context.** Spring Boot 3 requires Java 17+ and moved the entire Java EE ecosystem from the `javax.*` namespace to `jakarta.*` (Jakarta EE 9/10). `WebSecurityConfigurerAdapter` was removed — security is now configured via a `SecurityFilterChain` bean with the lambda DSL. `RestTemplate` is in maintenance mode; new code should use `RestClient` (synchronous, fluent) or the declarative `@HttpExchange` interface clients [5].

### 2.4 REST Endpoints with Spring

```java
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    // Constructor injection — the standard
    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderDto> getOrder(@PathVariable UUID id) {
        return orderService.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public ResponseEntity<OrderDto> create(@RequestBody @Valid CreateOrderRequest request) {
        OrderDto created = orderService.create(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .location(URI.create("/api/orders/" + created.id()))
                .body(created);
    }
}
```

Standard annotation set: `@GetMapping`, `@PostMapping`, `@PutMapping`, `@PatchMapping`, `@DeleteMapping`, `@PathVariable`, `@RequestParam`, `@RequestBody`, `@Valid` (Bean Validation / Jakarta Validation), `@ResponseStatus`, `@ExceptionHandler`.

**Centralized error handling** uses `@ControllerAdvice` with `@ExceptionHandler`. Spring Boot 6.1+ also supports RFC 9457 Problem Details out of the box via `ProblemDetail` and `ResponseEntity.ofProblem()` — the modern way to structure error responses.

### 2.5 Data Access

**Spring Data JPA** removes boilerplate through the `JpaRepository<T, ID>` interface, which provides CRUD, pagination (`Pageable`), sorting, and derived query methods (`findByEmail(String email)`) plus JPQL (`@Query`) and native queries.

Essential JPA knowledge includes the **entity lifecycle** (transient → managed → detached → removed), the difference between `getOne()/getReferenceById()` (proxy, lazy) and `findById()` (fetches), **N+1 query problems** and how `JOIN FETCH` or entity graphs fix them, **dirty checking**, and transaction boundaries with `@Transactional` (propagation levels like `REQUIRES_NEW`, isolation levels, `readOnly = true`, and rollback rules — by default only `RuntimeException` subclasses trigger rollback unless configured).

### 2.6 Testing

| Layer | Tool | Notes |
|---|---|---|
| Unit | JUnit 5, Mockito | No Spring context; fastest |
| Slice | `@WebMvcTest`, `@DataJpaTest`, `@JsonTest` | Loads only web/JPA/JSON layer with mocks |
| Integration | `@SpringBootTest` | Full context; use Testcontainers for real dependencies |
| Contract | Spring Cloud Contract / Pact | Verifies provider-consumer compatibility |

`@MockBean` (and its successor `mockStatic`-aware APIs) replaces beans in the context with mocks; prefer slice tests for speed.

---

## 3. REST Concepts and HTTP Status Codes

### 3.1 REST Constraints and Principles

REST (Representational State Transfer) is an architectural style, not a protocol. Its core constraints are: **client-server separation**, **statelessness** (each request carries all information the server needs — which is why authentication tokens travel on every request), **uniform interface**, **cacheability**, and optional **layered system** and **code-on-demand**.

In practice, a well-designed REST API means: resources identified by **nouns in plural URIs** (`/api/users/42/orders`), HTTP methods carrying the semantics (`GET` = retrieve, `POST` = create, `PUT` = replace, `PATCH` = partial update, `DELETE` = remove), standard status codes, JSON request/response bodies, and **idempotency** where appropriate (`GET`, `PUT`, `DELETE`, and idempotent `POST`s can be retried safely).

### 3.2 HTTP Status Codes

Status codes group into five classes by their first digit [7].

| Class | Meaning | Key codes to know cold |
|---|---|---|
| **1xx** | Informational | `100 Continue`, `101 Switching Protocols` |
| **2xx** | Success | `200 OK`, `201 Created`, `204 No Content` |
| **3xx** | Redirection | `301 Moved Permanently`, `302 Found`, `304 Not Modified` (caching), `307/308` (preserve method) |
| **4xx** | Client error | `400 Bad Request`, `401 Unauthorized`, `403 Forbidden`, `404 Not Found`, `405 Method Not Allowed`, `409 Conflict`, `410 Gone`, `415 Unsupported Media Type`, `422 Unprocessable Entity`, `429 Too Many Requests` |
| **5xx** | Server error | `500 Internal Server Error`, `502 Bad Gateway`, `503 Service Unavailable`, `504 Gateway Timeout` |

**Nuances that matter in interviews and code reviews:**

- `401` means *unauthenticated* (identity unknown); `403` means *unauthorized* (identity known, permission denied).
- `201 Created` should carry a `Location` header pointing to the new resource; `204` carries no body (common for deletes).
- `409` signals a semantic conflict — the classic case is creating a resource whose business key already exists.
- `422` is increasingly used for valid-syntax-but-invalid-semantics request bodies (e.g., failed validation) since RFC 9110 moved it out of WebDAV-only territory.
- `429` pairs with the `Retry-After` header and is the HTTP-native expression of rate limiting.
- `503` should also carry `Retry-After`; it is the correct response during deployments or deliberate degradation, not `500`.

### 3.3 Common REST Design Decisions

**Pagination**: cursor-based (`?after=token&limit=20`) for high-volume or real-time data, offset-based (`?page=1&size=20`) for simpler cases; always return total/hints and avoid deep offset scans. **Filtering, sorting, field selection**: `?status=ACTIVE&sort=createdAt,desc&fields=id,name`. **Versioning**: URL path (`/v2/`), header, or content negotiation — path versioning is simplest; header-based keeps URIs clean. **HATEOAS**: links embedded in responses (`_links`) enabling discoverability, though rarely used beyond HAL/JSON:API conventions. **Async patterns**: for long-running operations, return `202 Accepted` with a `Location` pointing to a status resource or use webhooks/async callbacks.

**Caching headers**: `Cache-Control: max-age=3600`, `ETag`/`If-None-Match` for conditional requests (server returns `304`), `Last-Modified`/`If-Modified-Since`.

---

## 4. Backend Fundamentals

### 4.1 Databases and Persistence

**SQL vs NoSQL.** Relational databases (PostgreSQL, MySQL) give ACID transactions, joins, and schemas; NoSQL covers key-value (Redis), document (MongoDB), wide-column (Cassandra), and graph (Neo4j). Choose by access pattern: complex queries and transactions → SQL; flexible schemas and horizontal scale → document; session caching and leaderboards → key-value.

**ACID** stands for Atomicity (all or nothing), Consistency (invariants hold), Isolation (concurrent transactions don't interfere), Durability (committed data survives crashes). **Isolation levels**, weakest to strongest: Read Uncommitted → Read Committed → Repeatable Read → Serializable. PostgreSQL's default is Read Committed. Dirty reads, non-repeatable reads, and phantoms are the anomalies higher levels prevent.

**Indexing essentials.** B-tree indexes speed up equality and range lookups at the cost of write performance and storage. Composite indexes are ordered — column order matters (`(a, b)` helps `WHERE a = ? AND b = ?` but not `WHERE b = ?` alone). Left-prefix wildcard searches (`LIKE '%foo'`) cannot use indexes. Use `EXPLAIN ANALYZE` to read query plans.

### 4.2 Caching

The standard strategy tiers are **cache-aside** (application reads cache first, misses fall through to DB and populate the cache), **write-through/write-behind**, and **refresh-ahead**. Redis is the typical choice. Two failure modes to know: the **thundering herd / cache stampede** (many requests hit DB simultaneously on expiry — fix with lock or early recomputation) and **cache invalidation** ("one of the two hard problems in computer science"). Use TTLs as a safety net and invalidate on writes.

### 4.3 Concurrency and Threads

Beyond Java 21 virtual threads, know the classics: `synchronized` vs `ReentrantLock`, `volatile` (visibility, not atomicity), `AtomicInteger`/CAS operations, thread pools (`Executors.newFixedThreadPool`, `ThreadPoolExecutor` tuning), `CompletableFuture` (combining async stages), and the producer-consumer model via `BlockingQueue`. Deadlock conditions (mutual exclusion, hold-and-wait, no preemption, circular wait) and how to avoid them (consistent lock ordering, timeouts) remain interview staples.

### 4.4 Security Fundamentals

**Authentication vs authorization**: authentication proves *who* you are; authorization decides *what* you may do. **OAuth 2.0** is an authorization framework (the client obtains an access token for delegated access); **OIDC (OpenID Connect)** layers identity (the ID token) on top. Modern SPAs/single-page clients use the **Authorization Code flow with PKCE**; machine-to-machine uses **client credentials**. **JWTs** are self-contained signed (never encrypted by default — sign with RS256/ES256, keep secret keys secret) tokens containing claims with expiry; store them client-side carefully (httpOnly cookies beat localStorage for XSS resistance) and always validate signature, issuer, audience, and expiry. **CSRF** is mitigated by SameSite cookies or synchronizer tokens; **CORS** is a browser-enforced policy configured via `Access-Control-*` headers — never a security boundary on its own. OWASP Top 10 remains the checklist: injection, broken authentication, sensitive data exposure, XXE, broken access control, security misconfiguration, XSS, insecure deserialization, insufficient logging, SSRF.

### 4.5 Message Brokers and Asynchrony

Queues decouple producers and consumers and absorb load spikes. **Amazon SQS / Kafka / RabbitMQ** differ by model: SQS is a managed queue with at-least-once delivery; Kafka is a distributed commit log optimized for throughput and replay; RabbitMQ uses exchanges and routing with AMQP. Design for **at-least-once semantics** — consumers must be **idempotent** (deduplication keys, unique constraints, outbox pattern). **Dead-letter queues** catch poison messages. **Pub/sub (SNS, Kafka topics)** fans a message out to many subscribers, unlike point-to-point queues.

### 4.6 Essential Design Patterns

The **Repository** pattern abstracts persistence; the **Service layer** holds business logic and transaction boundaries; **DTOs** separate API contracts from domain models; the **Factory** and **Strategy** patterns replace if-ladders; the **Observer** pattern underpins event-driven code; **Decorator** and **Proxy** power Spring's own infrastructure (transactions, security). Know when *not* to use patterns — YAGNI applies.

---

## 5. Deployment Strategies

Deployment strategy is the answer to "how do we get new code into production without breaking the world?" The family of techniques is often called **progressive delivery** because each one progressively reduces the blast radius of a bad release [8].

### 5.1 Strategy Comparison

| Strategy | How it works | Downtime | Rollback | Cost | Best when |
|---|---|---|---|---|---|
| **Recreate (big-bang)** | Tear down old, deploy new | Yes | Full redeploy | Lowest | Dev/test, batch jobs |
| **Rolling update** | Replace instances in batches (Kubernetes default) | Minimal | Slow, versioned rollback | Low | Small frequent changes, backward-compatible versions |
| **Blue/Green** | Two identical envs behind a load balancer; switch traffic atomically | Zero | Instant (switch back) | ~2x infra | Major releases, instant rollback requirement |
| **Canary** | Route a small % of traffic to the new version, ramp up on evidence | Zero | Fast (reroute) | Moderate | Large user bases, metric-driven release validation |
| **A/B testing** | Route by user attributes, not version weight | Zero | Fast | Moderate | Feature/product experiments |
| **Shadow (dark)** | Mirror live traffic to the new version without affecting responses | Zero | N/A | Moderate | Load validation of new versions |

### 5.2 Details Worth Knowing

**Blue/green** keeps a fully idle standby environment ("green") while live traffic uses "blue"; after testing green, the load balancer flips. The trade-off is paying for double capacity, and database migrations must be backward-compatible because the old version must still run during the switch window [8].

**Canary deployments** start at, say, 5% of traffic and escalate (5% → 25% → 100%) as error rates, latency, and business metrics hold. They require a traffic-splitting mechanism — weighted load balancers, Istio/service mesh virtual services, or an API gateway. Their superpower is catching problems that tests miss but only affecting a small fraction of users.

**Rolling updates** are the Kubernetes `Deployment` default: new pods come up (respecting readiness probes) before old ones are terminated. They demand backward compatibility between adjacent versions, and rollback walks backward through the batches, which is slower than a blue/green flip.

**Feature flags** (LaunchDarkly, Unleash, open-source alternatives) decouple *deployment* from *release*: code ships hidden behind a toggle, then activates per user segment. This pairs naturally with canary and A/B strategies.

**Database migrations** deserve their own discipline: apply migrations that are backward-compatible (add columns nullable, never rename/drop in the same release), deploy code, then clean up in a later release. Tools like Flyway and Liquibase version these.

---

## 6. Observability

Observability answers "why is this system behaving this way?" from its external outputs, as opposed to monitoring which answers "is it behaving as expected?" The three classic telemetry pillars are **metrics, logs, and traces** [9].

### 6.1 The Three Pillars

| Signal | What it is | Typical tools | Questions it answers |
|---|---|---|---|
| **Metrics** | Numeric aggregates over time (counters, gauges, histograms) | Prometheus, Grafana, CloudWatch | "Is throughput dropping? Is latency rising?" |
| **Logs** | Discrete timestamped events with context | ELK, Loki, CloudWatch Logs | "What exactly happened for this request?" |
| **Traces** | End-to-end journey of a request across services, composed of spans | Jaeger, Tempo, Zipkin, AWS X-Ray | "Where in the call chain did the 8 seconds go?" |

A **span** records one operation (name, start/end, status, tags); a **trace** links spans across service boundaries through a propagated **trace ID** (carried in HTTP headers like `traceparent`, the W3C Trace Context standard).

### 6.2 Standards and Instrumentation

**OpenTelemetry (OTel)** is the CNCF vendor-neutral standard for generating, collecting, and exporting telemetry. In Java, the OTel Java agent or SDK auto-instruments HTTP clients, JDBC, gRPC, and more; Spring Boot's `opentelemetry` starter wires it in. Micrometer remains Spring's metrics facade (`@Timed`, `MeterRegistry`), and Spring Boot Actuator exposes `/actuator/health`, `/actuator/metrics`, `/actuator/prometheus` endpoints [7].

**Health checks and probes** (`/actuator/health`) feed orchestrators: liveness probes restart broken apps, readiness probes withhold traffic from not-yet-ready instances (crucial during DB connection pool warm-up).

### 6.3 Operational Concepts

Measure services with **RED** (Rate, Errors, Duration) and infrastructure with **USE** (Utilization, Saturation, Errors). Frame reliability formally: an **SLI** is the measured quantity (p95 latency < 500 ms), an **SLO** is the target (99.9% of requests meet the SLI), and an **SLA** is the contractual commitment with consequences. Common latency percentiles: p50 (median), p95, p99 (tail — where real user pain lives), and p99.9 for SLO-boundary work. Use **error budgets** (100% − SLO) to decide when feature work must yield to reliability work.

**Structured logging** (JSON logs with trace/span IDs, correlation IDs, and consistent field names) is what makes logs queryable and correlatable with traces — plain `System.out.println` is not observability.

---

## 7. Kubernetes for Developers

You do not need to operate clusters to be productive in Kubernetes; you need to understand the objects your code lives in and the day-to-day `kubectl` workflow.

### 7.1 Core Objects

| Object | Purpose |
|---|---|
| **Pod** | Smallest deployable unit; one or more co-located containers sharing network (same IP, `localhost`) and volumes |
| **Deployment** | Declarative management of stateless replica sets with rolling updates |
| **ReplicaSet** | Maintains the desired pod count (usually managed by a Deployment) |
| **StatefulSet** | Pods with stable identity and storage — databases, queues |
| **DaemonSet** | One pod per node (log agents, monitoring) |
| **Job / CronJob** | Finite workloads; scheduled recurring work |
| **Service** | Stable network endpoint for a pod set: `ClusterIP` (default, internal), `NodePort`, `LoadBalancer` (cloud LB), `ExternalName` |
| **Ingress** | L7 HTTP(S) routing rules (path/host-based) via an ingress controller (nginx, ALB) |
| **ConfigMap / Secret** | Configuration and sensitive values injected as env vars or files |
| **PersistentVolume / PVC** | Durable storage decoupled from pod lifecycle |
| **Namespace** | Logical isolation boundary (dev/staging/prod, team separation) |

### 7.2 Developer Mental Model

A **Deployment** declares "I want 3 replicas of image `app:v2`"; Kubernetes' control plane reconciles reality to match. During a rollout, it uses a **rolling update strategy** (configurable via `maxUnavailable`/`maxSurge`). Pods are **ephemeral and disposable** — never store state in a pod's filesystem without a volume, and never rely on pod IP addresses (they change; Services give stable DNS names like `orders-svc.default.svc.cluster.local`).

**Probes** are how Kubernetes knows your app's state: `livenessProbe` (restart if broken), `readinessProbe` (withhold traffic until ready — e.g., until migrations run or the cache warms), `startupProbe` (for slow starters, prevents premature liveness killing).

**Resource requests and limits** (`resources.requests.memory/cpu` and `limits`) determine scheduling and throttling. Requests must be sized honestly: over-requesting wastes cluster capacity; under-requesting gets your pod OOM-killed under load.

**Scaling**: Horizontal Pod Autoscaler (HPA) scales replica count on CPU/memory or custom metrics (e.g., queue depth); Vertical Pod Autoscaler (VPA) adjusts requests; Karpenter/Cluster Autoscaler add nodes.

### 7.3 Daily kubectl

```bash
kubectl get pods -n <namespace>          # list pods
kubectl describe pod <name>              # events, conditions, resource detail
kubectl logs -f <pod> --since=5m         # follow logs
kubectl exec -it <pod> -- sh             # shell into a container
kubectl port-forward svc/orders-svc 8080:80  # reach a service locally
kubectl rollout status deployment/orders # watch a deployment
kubectl rollout undo deployment/orders   # rollback
kubectl apply -f deployment.yaml         # declarative update
```

For deeper service-to-service concerns (mTLS, traffic shaping, retries, canary splitting), a **service mesh** (Istio, Linkerd) or an API gateway handles the cross-cutting layer — that is usually platform-team territory, but knowing *what* they do (sidecar/injection, virtual services, destination rules) helps you read incidents.

---

## 8. Microservices Architecture

### 8.1 Definition and Trade-offs

Microservices decompose an application into independently deployable services, each owning its data and communicating over the network (HTTP/REST, gRPC, messaging). The payoff is independent scaling, technology flexibility, team autonomy, and smaller blast radius. The cost is exactly what a monolith gives you for free: distributed transactions, network failure modes, deployment orchestration, and debugging complexity. **The right first architecture for most teams is a well-modularized monolith**; split when team size, scale, or deployment cadence demands it (the two-pizza-team heuristic).

### 8.2 Bounded Contexts and Decomposition

Domain-Driven Design's **bounded context** is the primary unit of decomposition: each service maps to a business capability (orders, inventory, payments) with a clear ubiquitous language. Avoid decomposing by data layer (one service per table) or by technical layer — that recreates the distributed monolith.

### 8.3 Data Ownership and Consistency

Each service **owns its data**; other services access it only through APIs or events — shared databases are the fastest route back to a monolith. Without distributed transactions, consistency becomes **eventual**: the **Saga pattern** coordinates multi-step business transactions via a chain of local transactions with compensating actions (choreography via events, or orchestration via a coordinator). The **outbox pattern** (write domain state and event to an outbox table in one transaction, then relay to Kafka/SNS) gives reliable event publishing without dual-write races.

### 8.4 Communication Styles

| Dimension | Synchronous (REST/gRPC) | Asynchronous (events/messaging) |
|---|---|---|
| Coupling | Temporal coupling — caller waits | Loose — sender doesn't wait |
| Failure | Immediate errors, need resiliency patterns | Requires idempotency, DLQs, ordering care |
| Use for | Queries, request-response flows | Commands/events, integration, decoupling |

The **API Gateway** is the single entry point for clients (routing, auth offloading, rate limiting, aggregation — the BFF pattern: Backend-for-Frontend tailors APIs per client). **Service discovery** (built into Kubernetes DNS, or Consul/Eureka) lets services find each other without hardcoded endpoints. **Contract testing** (Pact, Spring Cloud Contract) prevents integration breakage between independently deployed services.

### 8.5 The Twelve-Factor App

The classic checklist for cloud-native services: one codebase tracked in version control, explicit dependencies, config in the environment, stateless processes, port binding, concurrency via process model, disposability (fast startup/shutdown), dev/prod parity, logs as event streams, and admin tasks as one-off processes.

---

## 9. Resiliency Patterns

Resiliency patterns keep a distributed system functional when parts of it fail — and parts *will* fail: networks partition, databases stall, third-party APIs degrade. The de facto Java implementation library is **Resilience4j** (Hystrix is retired; Spring Cloud Circuit Breaker uses Resilience4j underneath) [10].

### 9.1 Pattern Catalog

| Pattern | What it does | Know this |
|---|---|---|
| **Retry** | Re-attempts failed operations, ideally with exponential backoff + jitter | Only for **idempotent** or safe-to-repeat operations; never blindly retry `POST`s without idempotency keys |
| **Circuit Breaker** | Tracks failures; opens (fails fast) beyond a threshold, then half-opens to probe recovery | States: closed → open → half-open; protects the *caller* and gives the callee time to recover |
| **Bulkhead** | Isolates resources (thread pools, connection pools) per dependency | One slow dependency can't exhaust all threads and take down unrelated calls |
| **Rate Limiter** | Bounds request rate (token bucket / sliding window) | Protects downstream limits; pairs with HTTP `429` + `Retry-After` |
| **Timeout** | Bounds how long a call may take | Always set; without it, one stalled service hangs all callers |
| **Fallback / Degradation** | Returns a safe alternative (cached value, default, simplified flow) | Degrade gracefully rather than fail loudly when possible |
| **Request Hedging** | Sends a second request after a delay; uses the first response | Cures tail latency at the cost of extra load |
| **Chaos Engineering** | Injects failures in production (Chaos Monkey, Gremlin) to verify assumptions | Test hypotheses, start small, blast radius controlled |

### 9.2 Resilience4j at a Glance

```java
CircuitBreakerConfig config = CircuitBreakerConfig.custom()
    .failureRateThreshold(50)          // open at 50% failures
    .waitDurationInOpenState(Duration.ofSeconds(30))
    .slidingWindowSize(20)
    .minimumNumberOfCalls(10)          // sample before deciding
    .build();

var breaker = CircuitBreakerRegistry.of(config).circuitBreaker("paymentService");
Supplier<Response> decorated = CircuitBreaker.decorateSupplier(breaker, () -> callPaymentApi());
```

### 9.3 Designing for Failure

Combine patterns deliberately: a **timeout** around every remote call, a **circuit breaker** for calls that fail persistently, **retries with backoff** for transient failures (and idempotency keys so repeated calls are safe), **bulkheads** so one dependency's meltdown stays local, and **DLQs** so poisoned messages are quarantined rather than retried forever. Also design **graceful shutdown** (drain in-flight requests, finish transactions, then exit — Kubernetes gives 30s via `terminationGracePeriodSeconds`) and verify with real failure tests, not just unit tests of happy paths.

---

## 10. Basic AWS Services

A backend developer working on AWS does not need all 200+ services — a solid working knowledge of a core set covers most architectures.

### 10.1 Service Map

| Category | Services | What a backend dev uses them for |
|---|---|---|
| **Compute** | EC2, Lambda, ECS/EKS, Elastic Beanstalk | VMs; event-driven serverless functions; container orchestration (EKS = managed Kubernetes); legacy PaaS |
| **Storage** | S3, EBS, EFS, Glacier | Object storage (uploads, static assets); block storage for instances; shared file systems; archival |
| **Databases** | RDS, Aurora, DynamoDB, ElastiCache, DocumentDB | Managed PostgreSQL/MySQL; Aurora's serverless/auto-scaling; low-latency NoSQL with single-digit-ms reads; Redis/Memcached caching |
| **Networking** | VPC, Subnets, Security Groups, ALB/NLB, Route 53, CloudFront, API Gateway, VPC Endpoints | Private network isolation; L4/L7 load balancing; DNS; CDN; REST API front-door with auth/rate limiting; private AWS access without the internet |
| **Messaging** | SQS, SNS, EventBridge, Kinesis, MSK, Step Functions, SES | Queues; pub/sub notifications; event bus; streaming; managed Kafka; workflow orchestration; email |
| **Security & Identity** | IAM, Cognito, KMS, Secrets Manager, ACM, WAF | Least-privilege roles/policies; user authentication; encryption keys; secret storage; TLS certificates; web firewall |
| **Observability** | CloudWatch, X-Ray, CloudTrail | Metrics/logs/alarms; distributed tracing; API call auditing |
| **Deployment & IaC** | CodePipeline, CodeDeploy, CloudFormation, CDK, Terraform | CI/CD; blue/green & canary deploys built-in; infrastructure as code |

### 10.2 Concepts That Interviewers Love

**Regions and Availability Zones**: regions are geographic areas; AZs are isolated data centers within a region — architect for multi-AZ availability and cross-region DR. **IAM roles vs users**: applications assume *roles* (temporary credentials), never long-lived access keys. **The Well-Architected Framework**: six pillars — operational excellence, security, reliability, availability & disaster recovery, performance efficiency, cost optimization, and sustainability. **Serverless trade-offs**: Lambda gives zero-admin scaling and per-invocation pricing but has cold starts, 15-minute execution limits, and local-state limits — a strong fit for event-driven glue code, weaker for long-running connection-heavy services. **S3 consistency**: strongly consistent reads/writes since late 2020 — no more eventual-consistency caveats. **DynamoDB basics**: partition key design determines performance (hot partitions are the classic anti-pattern); single-digit ms reads; on-demand or provisioned capacity with auto-scaling.

### 10.3 A Canonical Serverless-Backend Shape

A typical modern AWS backend you will encounter: clients hit **CloudFront + API Gateway** → **Lambda or ECS service** → **RDS/Aurora or DynamoDB**, with **SQS/SNS/EventBridge** decoupling async work, **ElastiCache** in front of hot reads, **S3** for artifacts, **IAM roles** for every permission, **Secrets Manager** for DB credentials, and **CloudWatch/X-Ray** for telemetry — all provisioned through **Terraform or CDK**.

---

## 11. Quick-Reference Cheat Sheets

### 11.1 Spring Annotation Quick Guide

| Annotation | Layer / Purpose |
|---|---|
| `@RestController`, `@RequestMapping`, `@GetMapping/Post/Put/Patch/Delete` | Web layer routing |
| `@Service`, `@Repository`, `@Component`, `@ControllerAdvice` | Stereotypes; `@ControllerAdvice` for global exception handling |
| `@Autowired`, `@Qualifier`, `@Value`, `@ConfigurationProperties` | Injection and config binding |
| `@Bean`, `@Configuration`, `@Profile`, `@ConditionalOn...` | Bean definition and environment gating |
| `@Transactional`, `@EventListener`, `@Async` | Transactions, events, background work |
| `@Entity`, `@Table`, `@Id`, `@OneToMany`, `@ManyToOne`, `@JoinColumn` | JPA mapping |
| `@Valid`, `@NotNull`, `@Size`, `@Pattern` | Bean validation |
| `@EnableScheduling`, `@Scheduled`, `@Cacheable`, `@Retryable` | Scheduling, caching, retry (requires respective starters) |
| `@SpringBootTest`, `@WebMvcTest`, `@DataJpaTest`, `@MockBean` | Testing |

### 11.2 HTTP Codes One-Liner Review

`200` OK · `201` created · `204` no content · `301/302` redirect · `304` not modified · `400` bad request · `401` who are you · `403` you can't · `404` gone · `405` wrong method · `409` conflict · `415` bad media type · `422` invalid body · `429` slow down · `500` our fault · `502` upstream bad · `503` try again later · `504` upstream timeout.

### 11.3 Interview-Style Self-Check

1. Explain the difference between `RestTemplate`, `RestClient`, and `@HttpExchange` HTTP interface clients.
2. Why does `@Transactional` on a private method or on a self-call not work?
3. When would you choose a canary deployment over blue/green, and vice versa?
4. What does a circuit breaker's half-open state do, and why is it necessary?
5. How do you prevent duplicate message processing in a queue-based architecture?
6. Explain the saga pattern and when you would use choreography vs orchestration.
7. What is the difference between liveness and readiness probes?
8. How would you design an idempotent payment API?
9. When do virtual threads help, and when do they not (pinning, thread locals)?
10. What telemetry would you add to a newly deployed service on day one?

---

## References

[1]: https://www.baeldung.com/java-lts-21-new-features "Baeldung — New Features in Java 21"
[2]: https://www.azul.com/blog/jdk-21-delivers-virtual-threads-other-new-features-and-long-term-support/ "Azul — JDK 21 Delivers Virtual Threads, Other New Features, and Long-Term Support"
[3]: https://spring.io/blog/2023/09/20/hello-java-21 "Spring Blog — Hello, Java 21 (Josh Long)"
[4]: https://spring.io/blog/2025/12/30/this-year-in-spring-december-30th-2025 "Spring Blog — This Year in Spring, December 30th 2025"
[5]: https://www.infoq.com/articles/spring-boot-3-2-spring-6-1/ "InfoQ — Spring Boot 3.2 and Spring Framework 6.1 Add Java 21, Virtual Threads"
[6]: https://spring.io/blog/2024/05/23/spring-boot-3-3-0-available-now "Spring Blog — Spring Boot 3.3.0 Available Now"
[7]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status "MDN Web Docs — HTTP Response Status Codes"
[8]: https://www.getunleash.io/blog/comparing-deployment-strategies-canary-blue-green-and-rolling "Unleash — Comparing Deployment Strategies: Canary, Blue-Green, and Rolling"
[9]: https://opentelemetry.io/docs/concepts/observability-primer/ "OpenTelemetry — Observability Primer"
[10]: https://resilience4j.readme.io/docs/getting-started "Resilience4j — Getting Started"

1. [Baeldung — New Features in Java 21](https://www.baeldung.com/java-lts-21-new-features)
2. [Azul — JDK 21 Delivers Virtual Threads, Other New Features, and Long-Term Support](https://www.azul.com/blog/jdk-21-delivers-virtual-threads-other-new-features-and-long-term-support/)
3. [Spring Blog — Hello, Java 21 (Josh Long)](https://spring.io/blog/2023/09/20/hello-java-21)
4. [Spring Blog — This Year in Spring, December 30th 2025](https://spring.io/blog/2025/12/30/this-year-in-spring-december-30th-2025)
5. [InfoQ — Spring Boot 3.2 and Spring Framework 6.1 Add Java 21, Virtual Threads](https://www.infoq.com/articles/spring-boot-3-2-spring-6-1/)
6. [Spring Blog — Spring Boot 3.3.0 Available Now](https://spring.io/blog/2024/05/23/spring-boot-3-3-0-available-now)
7. [MDN Web Docs — HTTP Response Status Codes](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status)
8. [Unleash — Comparing Deployment Strategies: Canary, Blue-Green, and Rolling](https://www.getunleash.io/blog/comparing-deployment-strategies-canary-blue-green-and-rolling)
9. [OpenTelemetry — Observability Primer](https://opentelemetry.io/docs/concepts/observability-primer/)
10. [Resilience4j — Getting Started](https://resilience4j.readme.io/docs/getting-started)
