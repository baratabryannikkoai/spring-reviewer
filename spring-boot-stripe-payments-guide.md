# Handling Payments with Stripe in Spring Boot: Idempotency, Safe Retries & Webhooks

This guide covers a production-oriented Stripe integration in Spring Boot: creating payments safely (idempotency + retries), and receiving Stripe's webhook notifications reliably (signature verification, deduplication, fast acknowledgment).

```
┌─────────────┐   1. create PaymentIntent    ┌────────────┐
│  Your App   │ ─────(idempotency key)──────▶ │   Stripe   │
│ (Spring     │                                │            │
│  Boot)      │◀──── 2. webhook events ────────│            │
└─────────────┘   (signature-verified,          └────────────┘
                    deduplicated, async)
```

Two failure modes drive almost every design decision here:
1. **Your request to Stripe might succeed but the response might be lost** (network blip, timeout) → you retry → without an idempotency key, you'd charge the customer twice.
2. **Stripe's webhook delivery is at-least-once**, not exactly-once → the same event can arrive more than once → your handler must be safe to run twice.

---

## 1. Setup

### 1.1 Dependency

```xml
<dependency>
    <groupId>com.stripe</groupId>
    <artifactId>stripe-java</artifactId>
    <version>29.0.0</version> <!-- check for the current version -->
</dependency>
```

### 1.2 Configuration

```yaml
stripe:
  api-key: ${STRIPE_SECRET_KEY}
  webhook-secret: ${STRIPE_WEBHOOK_SECRET}
  connect-timeout-ms: 5000
  read-timeout-ms: 15000
```

```java
@Configuration
public class StripeConfig {

    @Value("${stripe.api-key}")
    private String apiKey;

    @Value("${stripe.connect-timeout-ms}")
    private int connectTimeoutMs;

    @Value("${stripe.read-timeout-ms}")
    private int readTimeoutMs;

    @PostConstruct
    public void init() {
        Stripe.apiKey = apiKey;
        // Always bound network calls — Stripe outages or slow networks shouldn't hang your threads indefinitely
        Stripe.setConnectTimeout(connectTimeoutMs);
        Stripe.setReadTimeout(readTimeoutMs);
        Stripe.setMaxNetworkRetries(0); // we implement our own retry policy below — see Section 2.3
    }
}
```

- Never hardcode `sk_live_...`/`whsec_...` values; load from a secrets manager or env vars, and use **separate keys per environment** (test vs. live) with separate webhook endpoints and signing secrets in the Stripe Dashboard.
- Restrict the API key's permissions (restricted keys) to only what your service needs (e.g., a service that only creates PaymentIntents shouldn't hold a key that can issue refunds).

---

## 2. Creating Payments Safely

### 2.1 Idempotency keys — the core safety mechanism

Every state-changing Stripe call (`PaymentIntent.create`, `Refund.create`, etc.) should carry an **idempotency key**. Stripe deduplicates requests with the same key for 24 hours: if you retry with the same key, Stripe returns the original result instead of creating a second charge.

**Best practice: derive the idempotency key from your own domain, not a random UUID generated at call time.** If you generate a fresh UUID on every retry attempt, retries stop being idempotent — you need the *same* key to survive across retries of the *same logical operation*.

```java
// Good: derived from a stable identifier owned by your system (e.g., your internal order ID)
String idempotencyKey = "create-payment-intent-" + order.getId();

// Bad: a new UUID per attempt defeats the purpose — a retry would get a NEW key,
// and Stripe would create a second PaymentIntent.
```

### 2.2 Creating a `PaymentIntent`

```java
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    public PaymentIntent createPaymentIntent(Order order) throws StripeException {

        String idempotencyKey = "create-payment-intent-" + order.getId();

        PaymentIntentCreateParams params = PaymentIntentCreateParams.builder()
            .setAmount(order.getAmountInCents())
            .setCurrency(order.getCurrency().toLowerCase())
            .setCustomer(order.getStripeCustomerId())
            .putMetadata("order_id", order.getId().toString())   // always tag with your own ID for traceability
            .setAutomaticPaymentMethods(
                PaymentIntentCreateParams.AutomaticPaymentMethods.builder()
                    .setEnabled(true)
                    .build())
            .build();

        RequestOptions requestOptions = RequestOptions.builder()
            .setIdempotencyKey(idempotencyKey)
            .build();

        return PaymentIntent.create(params, requestOptions);
    }
}
```

- **Always set `metadata` linking back to your internal ID** (order ID, invoice ID). When a webhook or a support ticket references a Stripe object, this is how you find the corresponding row in your own database without guesswork.
- Store the amount/currency **server-side**, computed from your own order data — never trust an amount passed from the client, or you've built a price-tampering vulnerability.

### 2.3 Safe retries around transient failures

Stripe's own SDK can auto-retry (`Stripe.setMaxNetworkRetries`), but for full control in a Spring app, implement retry explicitly so you can log, bound, and distinguish retryable vs. non-retryable failures:

```java
@Service
public class ResilientPaymentService {

    private final PaymentService paymentService;
    private final RetryTemplate retryTemplate;

    public ResilientPaymentService(PaymentService paymentService) {
        this.paymentService = paymentService;
        this.retryTemplate = RetryTemplate.builder()
            .maxAttempts(4)
            .exponentialBackoff(500, 2.0, 8000) // 500ms, 1s, 2s, 4s...
            .retryOn(ApiConnectionException.class) // network-level failures only
            .retryOn(RateLimitException.class)     // 429s
            .build();
    }

    public PaymentIntent createPaymentIntentWithRetry(Order order) {
        return retryTemplate.execute(ctx ->
            paymentService.createPaymentIntent(order)
        );
    }
}
```

**What is and isn't safe to retry:**

| Exception | Retry? | Why |
|---|---|---|
| `ApiConnectionException` (network failure, timeout) | **Yes** | You don't know if Stripe received/processed the request — the idempotency key makes retrying safe rather than risky. |
| `RateLimitException` (429) | **Yes**, with backoff | Transient; Stripe expects clients to back off. |
| `ApiException` (Stripe-side 5xx) | **Yes**, with backoff | Server error on Stripe's end, not your request's fault. |
| `CardException` (card declined) | **No** | This is a definitive business outcome, not a transient failure — retrying with the same card won't succeed. Surface it to the user. |
| `InvalidRequestException` (bad params) | **No** | A bug in your request — retrying an identical malformed request fails identically forever. Fix the request, don't retry it. |
| `AuthenticationException` (bad API key) | **No** | Config problem; retrying doesn't fix a wrong key. Alert, don't retry. |

- **Always keep the same idempotency key across all attempts of one logical operation.** This is what makes the above retry policy safe: even if the first attempt actually succeeded on Stripe's side but the response was lost to a network error, the retry with the same key returns the original PaymentIntent instead of creating a duplicate charge.
- **Cap total retry time** below your upstream caller's timeout (e.g., the HTTP request from your frontend) so retries don't turn a slow failure into a hung request.
- Persist the `idempotencyKey` (or the resulting PaymentIntent ID) against your `Order` record as soon as you have it, so that even a full application restart mid-retry can resume safely by reusing the same key rather than starting over with a new one.

### 2.4 Confirming payment status — trust webhooks, not just the synchronous response

A `PaymentIntent` created via the API can be `requires_action`, `processing`, or `succeeded` — many payment methods complete asynchronously (3D Secure, bank redirects, etc.). **Don't mark an order as paid based solely on the synchronous API response.** Treat the webhook (Section 3) as the source of truth for final payment state; the synchronous response only tells you the request was accepted.

---

## 3. Receiving Stripe Webhooks

### 3.1 Endpoint: verify signature against the raw body

```java
@RestController
@RequestMapping("/webhooks")
public class StripeWebhookController {

    private static final Logger log = LoggerFactory.getLogger(StripeWebhookController.class);

    private final String webhookSecret;
    private final WebhookEventService webhookEventService;

    public StripeWebhookController(
            @Value("${stripe.webhook-secret}") String webhookSecret,
            WebhookEventService webhookEventService) {
        this.webhookSecret = webhookSecret;
        this.webhookEventService = webhookEventService;
    }

    @PostMapping("/stripe")
    public ResponseEntity<String> handleWebhook(
            @RequestBody String payload,                          // MUST be the raw, unparsed body
            @RequestHeader("Stripe-Signature") String sigHeader) {

        Event event;
        try {
            event = Webhook.constructEvent(payload, sigHeader, webhookSecret);
        } catch (SignatureVerificationException e) {
            log.warn("Rejected webhook: invalid signature");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid signature");
        } catch (Exception e) {
            log.warn("Rejected webhook: malformed payload", e);
            return ResponseEntity.badRequest().body("Malformed payload");
        }

        // Deduplicate + enqueue for async processing, then return immediately (Section 3.3/3.4)
        boolean accepted = webhookEventService.acceptForProcessing(event);
        return accepted
            ? ResponseEntity.ok("Received")
            : ResponseEntity.ok("Already processed"); // still 200 — Stripe should not retry duplicates
    }
}
```

**Critical setup detail:** Spring Security or a global body-parsing filter can consume/transform the request body before it reaches your controller. Make sure the raw bytes reaching `Webhook.constructEvent` are **exactly** what Stripe sent — no JSON re-serialization, no charset conversion, no trimming. If you use Spring Security, exclude this endpoint from CSRF protection (it's not a browser-originated form submission) and ensure no filter pre-parses the body.

- **Never skip signature verification**, even "temporarily" in production — without it, anyone who discovers your endpoint URL can POST forged events and trigger your business logic (e.g., fake `payment_intent.succeeded` to unlock paid content for free).
- **Use a separate webhook signing secret per environment** (test/live), matching the separate endpoints you configure in the Stripe Dashboard.
- Set the **timestamp tolerance** conservatively (Stripe's SDK defaults to 5 minutes) to mitigate replay of an old, previously-valid signed payload; don't widen it without a specific reason.

### 3.2 Idempotent event processing

Stripe guarantees **at-least-once delivery** and retries non-2xx responses on an increasing backoff schedule for up to 72 hours — so your handler **will** see duplicate `event.id`s in normal operation, not just in edge cases.

```java
@Service
public class WebhookEventService {

    private final ProcessedWebhookEventRepository processedEventRepository;
    private final ApplicationEventPublisher eventPublisher;

    public WebhookEventService(ProcessedWebhookEventRepository repo, ApplicationEventPublisher publisher) {
        this.processedEventRepository = repo;
        this.eventPublisher = publisher;
    }

    @Transactional
    public boolean acceptForProcessing(Event event) {
        // Insert-or-skip on a UNIQUE(event_id) constraint — the DB enforces the dedup, not application logic,
        // so a race between two near-simultaneous deliveries can't both "win".
        boolean isNew = processedEventRepository
            .insertIfAbsent(event.getId(), event.getType(), Instant.now());

        if (!isNew) {
            return false; // already seen — no-op, but still ack Stripe with 200
        }

        // Publish for async handling (Section 3.3) rather than processing inline here
        eventPublisher.publishEvent(new StripeWebhookReceivedEvent(event));
        return true;
    }
}
```

```sql
CREATE TABLE processed_webhook_event (
    event_id     VARCHAR(255) PRIMARY KEY,   -- Stripe's evt_... ID, globally unique
    event_type   VARCHAR(100) NOT NULL,
    received_at  TIMESTAMP NOT NULL
);
```

- **Key the idempotency check on Stripe's `event.id`**, not on the underlying object ID (`payment_intent.id`) — a single PaymentIntent can legitimately produce multiple distinct events over its lifecycle (`requires_action` → `succeeded`), and you want to process each event once, not conflate them.
- **The dedup check and the business-logic write must be atomically consistent.** The pattern above uses a unique-constraint insert as the guard; if your business logic runs in a separate step, wrap the "mark processed" write and the actual state mutation (e.g., marking the order paid) in the **same database transaction** — otherwise a crash between the two leaves you having done the work without recording it, and the next retry double-processes.
- A DB unique constraint is usually sufficient at moderate volume; at high webhook volume, a Redis-based dedup (`SETNX event:{id}` with a TTL beyond Stripe's 72-hour retry window) avoids DB contention.

### 3.3 Respond fast — process asynchronously

Stripe expects a response within a short window (a few seconds); if you don't respond in time it's recorded as a failed delivery and queued for retry — even if your business logic would have eventually succeeded. **Verify the signature and durably record the event synchronously, but do the actual business work (fulfillment, emails, downstream syncs) asynchronously.**

```java
@Component
public class StripeWebhookHandler {

    private final OrderService orderService;

    public StripeWebhookHandler(OrderService orderService) {
        this.orderService = orderService;
    }

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onWebhookReceived(StripeWebhookReceivedEvent wrapper) {
        Event event = wrapper.event();

        switch (event.getType()) {
            case "payment_intent.succeeded" -> handlePaymentSucceeded(event);
            case "payment_intent.payment_failed" -> handlePaymentFailed(event);
            case "charge.refunded" -> handleChargeRefunded(event);
            case "charge.dispute.created" -> handleDisputeCreated(event);
            default -> {
                // Fine to ignore event types you don't act on — you don't need to handle every event Stripe sends.
            }
        }
    }

    private void handlePaymentSucceeded(Event event) {
        PaymentIntent intent = (PaymentIntent) event.getDataObjectDeserializer()
            .getObject()
            .orElseThrow(() -> new IllegalStateException(
                "Could not deserialize PaymentIntent for event " + event.getId()));

        String orderId = intent.getMetadata().get("order_id"); // set at creation time — see Section 2.2
        orderService.markPaid(orderId, intent.getId());
    }

    // handlePaymentFailed / handleChargeRefunded / handleDisputeCreated follow the same pattern
}
```

- `@TransactionalEventListener(phase = AFTER_COMMIT)` ensures the "event accepted" row from Section 3.2 is durably committed before business processing starts — if processing crashes, retry logic can find the accepted-but-unprocessed event and resume, rather than losing track of it.
- **Route unhandled event types to a default no-op branch, not an error.** New event types get added to Stripe's API over time; an unrecognized `event.type` should never fail the webhook or trigger alerting by default. Log it at DEBUG for awareness.
- **Handle deserialization failures explicitly.** `event.getDataObjectDeserializer().getObject()` can return empty if the event was serialized with an API version your SDK doesn't fully recognize — treat this as a **retryable condition**, not silent data loss: log loudly, and consider re-fetching the object explicitly via `PaymentIntent.retrieve(id)` as a fallback rather than dropping the event.
- **Pin your Stripe API version** (`Stripe.setApiVersion(...)` or `apiVersion` on the client) and keep it consistent between your webhook endpoint configuration in the Dashboard and your SDK version, so event payload shapes don't drift unexpectedly between what Stripe sends and what your SDK expects to parse.

### 3.4 Handling specific event types

| Event | Typical action |
|---|---|
| `payment_intent.succeeded` | Mark order/invoice as paid; trigger fulfillment. |
| `payment_intent.payment_failed` | Notify customer; allow retry with a new payment method. |
| `charge.refunded` | Update order status; reverse fulfillment if applicable. |
| `charge.dispute.created` | Flag account/order for review; notify a human — disputes have deadlines. |
| `customer.subscription.updated` / `.deleted` | Sync your local subscription/entitlement state to Stripe's, since Stripe (not your app) is the source of truth for subscription lifecycle. |
| `invoice.payment_failed` | Trigger dunning flow (retry email, grace period) rather than immediately revoking access. |

### 3.5 Monitoring and operational hygiene

- **Log every accepted event** (ID, type, verification outcome) even if you don't act on its type — this is your audit trail when a customer disputes "I paid but nothing happened."
- **Alert on a rising signature-verification failure rate** — a spike can mean either an attempted attack or, more mundanely, a misconfigured webhook secret after a rotation.
- **Test with the Stripe CLI**, not hand-rolled fixtures: `stripe listen --forward-to localhost:8080/webhooks/stripe` for local development, and `stripe trigger payment_intent.succeeded` to generate canonically correct, properly-signed test events. Use `stripe events resend` to replay a real historical event through your handler when debugging.
- **Rotate webhook signing secrets** with a documented runbook (add new endpoint with new secret, verify it receives traffic, remove old endpoint) rather than an in-place swap that could cause a gap in verified delivery.
- **Reconcile periodically.** Webhooks are reliable but not infallible (a misconfiguration, a sustained outage past the 72-hour retry window). Run a periodic job that lists recent Stripe events/PaymentIntents via the API and cross-checks them against your local records to catch anything missed.

---

## 4. Summary Checklist

**Creating payments**
- [ ] Idempotency key derived from a stable internal ID, reused across all retry attempts of the same operation
- [ ] Amount/currency computed server-side from trusted order data, never from client input
- [ ] Metadata tags every Stripe object with your internal order/invoice ID
- [ ] Retry policy distinguishes transient failures (network, 429, 5xx) from terminal ones (card declined, invalid request, bad auth) — only the former are retried
- [ ] Retries bounded by both attempt count and total elapsed time
- [ ] Final payment state trusted from webhooks, not just the synchronous create response

**Webhooks**
- [ ] Signature verified against the **raw, unmodified** request body on every request
- [ ] Separate signing secrets per environment, loaded from secrets management
- [ ] Deduplication keyed on Stripe's `event.id` via a DB unique constraint or equivalent
- [ ] Dedup-record write and business-logic write are transactionally consistent
- [ ] Endpoint responds quickly (verify + durably record only); business logic runs asynchronously
- [ ] Unrecognized event types are ignored gracefully, not treated as errors
- [ ] Deserialization failures are retried/logged, not silently dropped
- [ ] Event acceptance and processing outcomes are logged for audit and reconciliation
- [ ] Tested against real, CLI-generated signed events — not hand-built fixtures
- [ ] Periodic reconciliation job cross-checks Stripe's records against local state
