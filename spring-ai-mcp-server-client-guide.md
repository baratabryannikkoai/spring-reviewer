# Building an MCP Server & Client with Spring AI: Best Practices

This guide builds a Model Context Protocol (MCP) **server** that exposes tools, and an MCP **client** application that connects to it and lets an LLM (OpenAI) call those tools through `ChatClient`. It uses Spring AI's MCP Boot Starters and annotation-based programming model (`@McpTool`), which is the current recommended approach over the older manual `ToolCallback` registration.

```
┌────────────────────┐        MCP (Streamable-HTTP)        ┌──────────────────────┐
│   MCP CLIENT APP    │ ───────────────────────────────────▶│    MCP SERVER APP    │
│  (Spring Boot +     │◀───────────────────────────────────  │  (Spring Boot,       │
│   ChatClient + LLM) │        tool calls / results          │   exposes @McpTool)  │
└────────────────────┘                                       └──────────────────────┘
```

---

## 1. Transport Choice — Pick Deliberately

MCP supports three transports in Spring AI. This is the single most consequential decision, so make it explicitly rather than defaulting:

| Transport | When to use | Notes |
|---|---|---|
| **STDIO** | Local subprocess tools (CLI agents like Claude Code, Gemini CLI spawning your server) | Fast, no network exposure. **Fails under concurrent load** — one process, one client. Never use for a shared/remote server. |
| **SSE** | Legacy remote/web clients | Being superseded by Streamable-HTTP; keep only for backward compatibility with older clients. Each connection holds an open HTTP connection, which doesn't scale well under heavy concurrent agent traffic. |
| **Streamable-HTTP** | Remote, multi-client, production deployments | **Recommended default** for any server not running as a local subprocess. Works well behind load balancers/API gateways, supports optional streaming, handles concurrency better than SSE. |

This guide uses **Streamable-HTTP** for both server and client, since that's the production-appropriate default.

---

## 2. MCP Server

### 2.1 Dependencies

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-mcp-server-webmvc</artifactId>
</dependency>
```

> Use `spring-ai-starter-mcp-server` (no `-webmvc`/`-webflux` suffix) only if you specifically need STDIO for a locally-spawned subprocess server. For a reactive stack, `spring-ai-starter-mcp-server-webflux` is the Streamable-HTTP-capable WebFlux equivalent.

### 2.2 `application.yml`

```yaml
spring:
  application:
    name: order-tools-mcp-server
  ai:
    mcp:
      server:
        name: order-tools-server
        version: 1.0.0
        protocol: STREAMABLE          # replaces SSE; explicit is better than relying on defaults
        instructions: >
          Tools for looking up and managing customer orders.
          Use get-order-status before update-order-status to confirm the order exists.

server:
  port: 8081
```

> **Critical if you ever add STDIO**: never let anything write to stdout/stderr besides the MCP protocol frames — application logs, banners, or stray `System.out.println` calls will corrupt the JSON-RPC stream and silently break the connection. Set `spring.main.banner-mode=off` and redirect logging to a file for STDIO servers. This doesn't apply to Streamable-HTTP, but keep the discipline anyway if the same code might run in both modes.

### 2.3 Defining tools with `@McpTool`

```java
@Component
public class OrderTools {

    private final OrderRepository orderRepository;

    public OrderTools(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @McpTool(
        name = "get-order-status",
        description = "Look up the current status and delivery estimate for a customer order by its order ID.",
        annotations = @McpTool.McpAnnotations(
            readOnlyHint = true,       // does not modify state — lets clients treat it as safe to retry
            idempotentHint = true,
            destructiveHint = false
        )
    )
    public OrderStatusResult getOrderStatus(
            @McpToolParam(description = "The order ID, e.g. ORD-10293", required = true) String orderId) {

        // Validate input defensively — the LLM constructs this argument, don't trust it blindly
        if (!orderId.matches("ORD-\\d{5,}")) {
            throw new McpToolExecutionException("Invalid order ID format: " + orderId);
        }

        Order order = orderRepository.findByOrderId(orderId)
            .orElseThrow(() -> new McpToolExecutionException("No order found with ID: " + orderId));

        return new OrderStatusResult(order.getStatus(), order.getEstimatedDelivery());
    }

    @McpTool(
        name = "cancel-order",
        description = "Cancel a customer order. This is irreversible once processed.",
        annotations = @McpTool.McpAnnotations(
            readOnlyHint = false,
            destructiveHint = true,    // flags this as a destructive action to the client/host UI
            idempotentHint = true      // cancelling an already-cancelled order is a safe no-op
        )
    )
    public CancelOrderResult cancelOrder(
            @McpToolParam(description = "The order ID to cancel", required = true) String orderId,
            @McpToolParam(description = "Reason for cancellation", required = false) String reason) {

        Order order = orderRepository.findByOrderId(orderId)
            .orElseThrow(() -> new McpToolExecutionException("No order found with ID: " + orderId));

        if (order.getStatus() == OrderStatus.SHIPPED) {
            throw new McpToolExecutionException("Cannot cancel an order that has already shipped.");
        }

        order.cancel(reason);
        orderRepository.save(order);
        return new CancelOrderResult(orderId, "CANCELLED");
    }
}

public record OrderStatusResult(String status, LocalDate estimatedDelivery) {}
public record CancelOrderResult(String orderId, String newStatus) {}
```

**Tool design best practices:**

- **One clear responsibility per tool.** Don't build a single `manageOrder(action, ...)` mega-tool — the model reasons far better over several narrowly-scoped tools with precise descriptions than one overloaded tool with an `action` enum.
- **Write descriptions for the model, not for a human reading your code.** `description` is the primary signal the LLM uses to decide *whether* and *when* to call a tool — be explicit about preconditions ("call get-order-status first"), units, and expected formats.
- **Use the annotation hints** (`readOnlyHint`, `destructiveHint`, `idempotentHint`) accurately. Hosts use these to decide whether to prompt the user for confirmation before a destructive call — mislabeling a destructive tool as read-only is a safety bug, not a style nit.
- **Validate every parameter server-side.** The LLM constructs tool arguments from natural language and can get them wrong or be manipulated via prompt injection from untrusted content earlier in the conversation — treat tool input exactly like untrusted user input from an HTTP endpoint (format checks, bounds checks, authorization checks).
- **Return structured results (records/POJOs), not ad hoc strings.** Spring AI auto-generates the JSON schema from your method signature; structured output is easier for the model to parse reliably and easier for you to version.
- **Throw a dedicated exception type** (`McpToolExecutionException` or similar) for expected failure cases so the client gets a clean tool-error result instead of a raw stack trace leaking internals.
- **Keep tools stateless where possible.** If a tool needs session/user context, use `McpMeta` or the request context objects (`McpSyncRequestContext`) rather than static/singleton mutable state.

### 2.4 Authentication & authorization

An MCP server is a network-exposed API — treat it like one:

- Put it behind standard Spring Security (OAuth2/bearer token, mTLS, or an API gateway) exactly as you would any other HTTP service. MCP does not replace your auth layer.
- Enforce **per-tool authorization**, not just "is this caller authenticated" — e.g., `cancel-order` should check the caller's principal has permission for that specific order/tenant, the same way a REST controller would.
- Validate `Origin`/allowed-host configuration for HTTP transports to guard against DNS-rebinding-style attacks against locally-bound servers.
- Never expose a tool that runs arbitrary code, shell commands, or unrestricted file/database access without heavy sandboxing — a compromised or confused LLM caller can and will misuse an overly powerful tool.

### 2.5 Exposing resources and prompts (optional)

Beyond tools, MCP servers can expose read-only **resources** and reusable **prompts**:

```java
@Component
public class OrderPrompts {

    @McpPrompt(name = "order-summary", description = "Generate a customer-facing order summary")
    public String orderSummaryPrompt(@McpToolParam(description = "Order ID") String orderId) {
        return """
            Summarize the status of order %s in a friendly, concise tone suitable for a customer email.
            """.formatted(orderId);
    }
}
```

Use `@McpResource` for exposing read-only data (docs, schemas, static reference data) the client can fetch without it counting as a model-invoked "action."

---

## 3. MCP Client

### 3.1 Dependencies

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-mcp-client</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-model-openai</artifactId>
</dependency>
```

### 3.2 `application.yml`

```yaml
spring:
  ai:
    openai:
      api-key: ${OPENAI_API_KEY}
      chat:
        options:
          model: gpt-4o-mini
    mcp:
      client:
        type: SYNC
        initialized: true
        request-timeout: 20s          # always bound this — a hung tool call shouldn't hang the request
        toolcallback:
          enabled: true                # auto-registers MCP tools as Spring AI ToolCallbacks
        streamable-http:
          connections:
            order-tools:
              url: http://order-tools-mcp-server:8081
```

> Reference the server by a **service name/DNS entry** (e.g., via service discovery or an internal load balancer), not a hardcoded IP, so the client survives server redeployments.

### 3.3 Wiring tools into `ChatClient`

```java
@Service
public class SupportAssistantService {

    private final ChatClient chatClient;

    public SupportAssistantService(ChatClient.Builder builder, ToolCallbackProvider mcpTools) {
        this.chatClient = builder
            .defaultTools(mcpTools)   // registers every tool discovered from connected MCP servers
            .defaultSystem("""
                You are a customer support assistant. Use the available tools to look up
                real order data before answering questions about order status. Never guess
                an order's status — always call get-order-status first. Confirm with the
                user before calling any tool that cancels or modifies an order.
                """)
            .build();
    }

    public String handle(String userMessage) {
        return chatClient.prompt()
            .user(userMessage)
            .call()
            .content();
    }
}
```

Spring AI's `ToolCallingAdvisor` (auto-registered on `ChatClient`) manages the full tool-calling loop: it sends the tool schemas to the model, executes whichever tools the model decides to call, feeds results back, and repeats until the model produces a final answer — no manual loop required.

### 3.4 Client-side best practices

- **Scope tool exposure per use case.** If a `ChatClient` only needs read access, register just the read-only tools (e.g., via a filtered `ToolCallbackProvider`) rather than every tool the server happens to expose — this limits blast radius if the model is manipulated into calling something it shouldn't.
- **Require confirmation for destructive tools in the host UI/flow** when `destructiveHint = true` is set, rather than letting the model execute silently — surface a confirmation step in your application layer before the tool call proceeds, especially for anything irreversible.
- **Set explicit timeouts** (`request-timeout`) — an unresponsive MCP server otherwise blocks the whole chat turn.
- **Handle tool-list changes gracefully.** Implement `@McpToolListChanged` if the server's available tools can change at runtime, so the client doesn't operate on a stale tool set.
- **Log tool invocations and results** (arguments, tool name, latency, success/failure) at the client — this is your audit trail for "why did the assistant do that," which matters a lot once a tool can cancel an order or send an email.
- **Retry only idempotent tools.** Use the `idempotentHint` metadata to decide whether automatic retry-on-timeout is safe; retrying a non-idempotent tool blindly can duplicate side effects (e.g., double-cancel, double-charge).
- **Isolate MCP server failures from the rest of the app.** Wrap tool registration/connection in a circuit breaker or health check so one unreachable MCP server degrades gracefully (fewer tools available) instead of failing every chat request.

---

## 4. Testing

- **Unit test tool methods directly** as plain Java methods (`OrderTools` above has no MCP-specific code inside it beyond annotations) — no protocol machinery needed for this layer.
- **Integration test the server** using the MCP Java SDK's client in test scope, asserting on the tool list (names, schemas) and on call results for both success and validation-failure paths.
- **Test the full loop** (client → LLM → tool call → server → response) against a **mocked or sandboxed** version of the server in CI — never run tests that hit your real OpenAI account and a production MCP server together in a pipeline; use recorded/mocked chat responses to keep tests deterministic and free.
- **Test authorization boundaries explicitly** — a call for `cancel-order` on an order the caller doesn't own must fail, and you should have a test asserting that, not just trust the code review.

---

## 5. Summary Checklist

- [ ] Transport chosen deliberately (Streamable-HTTP for production remote servers; STDIO only for local subprocess use cases)
- [ ] Each tool has a single responsibility and a description written for the model's benefit
- [ ] `readOnlyHint` / `destructiveHint` / `idempotentHint` set accurately on every tool
- [ ] All tool parameters validated server-side as untrusted input
- [ ] Tool results are structured types, not raw strings
- [ ] Server is authenticated/authorized like any other network API; per-tool authorization enforced
- [ ] Destructive tools require confirmation before execution on the client/host side
- [ ] Explicit timeouts configured on both client and server
- [ ] Tool invocations logged for auditability
- [ ] Retries limited to idempotent tools only
- [ ] Server and client each independently testable without hitting real external services in CI
