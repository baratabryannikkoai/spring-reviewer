# Spring Boot + AWS Integration Reviewer

A study/reference guide covering AWS service integration in Spring Boot backend services, integration best practices, and AWS Cognito-based JWT authentication with Spring Security.

---

## Table of Contents

1. [AWS SDK Setup in Spring Boot](#1-aws-sdk-setup-in-spring-boot)
2. [Amazon S3 Integration](#2-amazon-s3-integration)
3. [Amazon SNS Integration](#3-amazon-sns-integration)
4. [Amazon SQS Integration](#4-amazon-sqs-integration)
5. [Other Common AWS Services](#5-other-common-aws-services)
6. [Best Practices When Integrating with AWS](#6-best-practices-when-integrating-with-aws)
7. [AWS Cognito + Spring Security JWT Auth](#7-aws-cognito--spring-security-jwt-auth)
8. [Quick Review Checklist](#8-quick-review-checklist)

---

## 1. AWS SDK Setup in Spring Boot

### 1.1 Dependency Options

**Option A — Spring Cloud AWS (recommended for Spring-idiomatic integration)**
```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.awspring.cloud</groupId>
      <artifactId>spring-cloud-aws-dependencies</artifactId>
      <version>3.1.1</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.awspring.cloud</groupId>
    <artifactId>spring-cloud-aws-starter-s3</artifactId>
  </dependency>
  <dependency>
    <groupId>io.awspring.cloud</groupId>
    <artifactId>spring-cloud-aws-starter-sns</artifactId>
  </dependency>
  <dependency>
    <groupId>io.awspring.cloud</groupId>
    <artifactId>spring-cloud-aws-starter-sqs</artifactId>
  </dependency>
</dependencies>
```

**Option B — Raw AWS SDK v2** (more control, less "magic")
```xml
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>bom</artifactId>
  <version>2.28.0</version>
  <type>pom</type>
  <scope>import</scope>
</dependency>
```

> ⚠️ Always use **AWS SDK v2** (`software.amazon.awssdk`), not the legacy v1 (`com.amazonaws`). v1 is in maintenance mode.

### 1.2 Basic Client Configuration

```java
@Configuration
public class AwsConfig {

    @Value("${aws.region}")
    private String region;

    @Bean
    public S3Client s3Client() {
        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    @Bean
    public SnsClient snsClient() {
        return SnsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    @Bean
    public SqsClient sqsClient() {
        return SqsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }
}
```

**Reviewer note:** `DefaultCredentialsProvider` resolves credentials in this order — env vars → system properties → `~/.aws/credentials` → container/instance metadata (ECS task role / EC2 instance profile). Never hardcode keys in `@Value` or `application.yml`.

---

## 2. Amazon S3 Integration

### 2.1 Upload / Download / Delete

```java
@Service
@RequiredArgsConstructor
public class S3StorageService {

    private final S3Client s3Client;

    @Value("${aws.s3.bucket}")
    private String bucket;

    public void upload(String key, MultipartFile file) throws IOException {
        s3Client.putObject(
            PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(file.getContentType())
                .build(),
            RequestBody.fromInputStream(file.getInputStream(), file.getSize())
        );
    }

    public byte[] download(String key) {
        ResponseBytes<GetObjectResponse> object = s3Client.getObjectAsBytes(
            GetObjectRequest.builder().bucket(bucket).key(key).build()
        );
        return object.asByteArray();
    }

    public void delete(String key) {
        s3Client.deleteObject(DeleteObjectRequest.builder()
            .bucket(bucket).key(key).build());
    }

    public URL generatePresignedUrl(String key, Duration expiry) {
        try (S3Presigner presigner = S3Presigner.create()) {
            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(expiry)
                .getObjectRequest(b -> b.bucket(bucket).key(key))
                .build();
            return presigner.presignGetObject(presignRequest).url();
        }
    }
}
```

### 2.2 Review Points

- Use **presigned URLs** for direct client uploads/downloads instead of proxying large files through your backend.
- Stream large files (`RequestBody.fromInputStream`) — avoid loading entire files into memory as `byte[]`.
- Enable **S3 bucket versioning** and lifecycle rules for cost control (transition to Glacier, expire old versions).
- Use **server-side encryption** (SSE-S3 or SSE-KMS) by default.
- Validate file types/sizes at the application layer before upload — S3 won't do this for you.

---

## 3. Amazon SNS Integration

SNS is a **pub/sub** service — good for fan-out notifications (e.g., one event → multiple subscribers: email, SQS queues, Lambda).

```java
@Service
@RequiredArgsConstructor
public class SnsNotificationService {

    private final SnsClient snsClient;

    @Value("${aws.sns.topic-arn}")
    private String topicArn;

    public void publish(String message, Map<String, String> attributes) {
        Map<String, MessageAttributeValue> messageAttributes = attributes.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(e.getValue())
                        .build()
            ));

        snsClient.publish(PublishRequest.builder()
            .topicArn(topicArn)
            .message(message)
            .messageAttributes(messageAttributes)
            .build());
    }
}
```

### Review Points

- Use **message attributes** for filtering — let SNS route to specific subscribers via **subscription filter policies** instead of filtering logic in your app.
- Prefer **FIFO SNS topics** when ordering/deduplication matters (e.g., financial events); standard topics are at-least-once, unordered.
- Combine with SQS (SNS → SQS fan-out) so consumers get durable, retryable delivery instead of relying on direct HTTP/Lambda subscribers.
- For structured payloads, publish JSON and set `messageStructure("json")` if you need protocol-specific formatting (email vs SMS vs SQS).

---

## 4. Amazon SQS Integration

SQS is for **point-to-point, decoupled, asynchronous processing** — a producer places messages on a queue; one or more consumers process them.

### 4.1 Producer

```java
@Service
@RequiredArgsConstructor
public class SqsProducerService {

    private final SqsTemplate sqsTemplate; // from spring-cloud-aws-starter-sqs

    public void send(String queueName, Object payload) {
        sqsTemplate.send(to -> to.queue(queueName).payload(payload));
    }
}
```

### 4.2 Consumer

```java
@Component
public class SqsListenerService {

    @SqsListener("${aws.sqs.queue-name}")
    public void listen(OrderEvent event, Acknowledgement acknowledgement) {
        try {
            process(event);
            acknowledgement.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process message, will retry via redrive policy", e);
            // don't ack -> message becomes visible again after visibility timeout
        }
    }
}
```

### Review Points

- Always configure a **Dead Letter Queue (DLQ)** with a `maxReceiveCount` redrive policy — poison messages should not loop forever.
- Set an appropriate **visibility timeout** — longer than your worst-case processing time, or the message will be redelivered while still being processed.
- Use **long polling** (`ReceiveMessageWaitTimeSeconds > 0`) to reduce empty receives and cost.
- Make consumers **idempotent** — SQS standard queues guarantee at-least-once delivery, so duplicate processing is possible.
- For ordering guarantees, use **FIFO queues** with a `MessageGroupId`; understand the throughput trade-off (up to 300 msg/s without batching, 3000/s with).
- Batch `sendMessageBatch` / `deleteMessageBatch` calls where possible to reduce API calls and cost.

---

## 5. Other Common AWS Services

| Service | Typical Use in Spring Boot | Key Library/Annotation |
|---|---|---|
| **RDS** | Relational DB via JDBC/Spring Data | Standard `DataSource`, use IAM DB auth or Secrets Manager for creds |
| **DynamoDB** | NoSQL data store | `spring-cloud-aws-starter-dynamodb` or `DynamoDbEnhancedClient` |
| **Secrets Manager** | Store DB creds, API keys | `spring-cloud-aws-starter-secrets-manager` (`${sm://secret-name}`) |
| **Parameter Store (SSM)** | App config / feature flags | `spring-cloud-aws-starter-parameter-store` (`${ssm:/app/config}`) |
| **KMS** | Encrypt/decrypt sensitive fields | `KmsClient` directly, or via SSE integration on S3 |
| **CloudWatch** | Logs & custom metrics | Micrometer `CloudWatchMeterRegistry`, or the CloudWatch Logs agent |
| **Lambda** | Event-driven compute, triggered by SNS/SQS/S3 events | Usually a separate deployable, not embedded in the Spring app |
| **EventBridge** | Event bus for decoupled service architectures | `EventBridgeClient` |
| **Cognito** | Auth/identity — see Section 7 | Spring Security OAuth2 Resource Server |

---

## 6. Best Practices When Integrating

### 6.1 Credentials & Security
- **Never hardcode AWS keys.** Use IAM roles (ECS task role, EC2 instance profile, EKS IRSA) in production; use named profiles or env vars locally.
- Apply **least-privilege IAM policies** — scope permissions to specific ARNs/resources, not `"Resource": "*"`.
- Rotate credentials via Secrets Manager rather than static `.env` files when not using role-based auth.

### 6.2 Configuration & Environments
- Externalize region, bucket names, ARNs, and queue URLs via `application.yml` + Spring profiles (`dev`, `staging`, `prod`) — never hardcode ARNs in code.
- Use **LocalStack** for local development/integration tests to avoid touching real AWS resources.

```yaml
aws:
  region: ${AWS_REGION:ap-southeast-1}
  s3:
    bucket: ${S3_BUCKET_NAME}
  sqs:
    queue-name: ${SQS_QUEUE_NAME}
  sns:
    topic-arn: ${SNS_TOPIC_ARN}
```

### 6.3 Resilience
- Wrap AWS calls with **retry + exponential backoff** (SDK v2 has built-in retry policies — tune `RetryPolicy` rather than reinventing it).
- Use **circuit breakers** (Resilience4j) around AWS calls that are on a critical request path, so an AWS outage doesn't cascade.
- Set explicit **timeouts** on SDK clients (`apiCallTimeout`, `apiCallAttemptTimeout`) — don't rely on defaults for latency-sensitive paths.
- Design for **eventual consistency** — S3 and DynamoDB are eventually consistent in some read paths; don't assume read-after-write everywhere.

### 6.4 Performance & Cost
- **Reuse SDK client instances** as singletons (Spring `@Bean`) — clients are thread-safe and expensive to create repeatedly.
- Use **async clients** (`S3AsyncClient`, etc.) for high-throughput, non-blocking I/O paths.
- Batch operations (SQS `sendMessageBatch`, S3 multipart upload for large files) to reduce API call overhead and cost.
- Monitor usage with **CloudWatch metrics/alarms** and set budget alerts — SNS/SQS/S3 costs scale with volume.

### 6.5 Observability
- Correlate logs with a **trace/request ID** propagated through SNS/SQS message attributes so you can follow a message across services.
- Emit structured logs (JSON) so CloudWatch Logs Insights queries are usable.
- Add health indicators (Spring Boot Actuator) that verify connectivity to critical AWS dependencies.

### 6.6 Testing
- Unit test with mocked clients (Mockito) — don't hit real AWS in unit tests.
- Integration test against **LocalStack** or **Testcontainers** for realistic behavior (bucket policies, queue redrive, etc.).
- Use **contract tests** for message schemas (SNS/SQS payloads) so producer/consumer teams don't drift.

---

## 7. AWS Cognito + Spring Security JWT Auth

### 7.1 How It Works (Conceptual Flow)

1. Client authenticates against a **Cognito User Pool** (via hosted UI, or Cognito Identity Provider SDK) and receives an **ID token** and/or **access token** (both JWTs).
2. Client sends the token as `Authorization: Bearer <token>` on API requests.
3. Spring Boot acts as an **OAuth2 Resource Server**: it validates the JWT signature against Cognito's public JWKS endpoint, checks issuer/audience/expiry, and extracts claims (e.g., `cognito:groups`, `scope`) for authorization.
4. No session state is stored server-side — the JWT itself is the credential.

### 7.2 Dependencies

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-oauth2-resource-server</artifactId>
</dependency>
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-security</artifactId>
</dependency>
```

### 7.3 Configuration

```yaml
spring:
  security:
    oauth2:
      resourceserver:
        jwt:
          issuer-uri: https://cognito-idp.${AWS_REGION}.amazonaws.com/${USER_POOL_ID}
```

Spring Boot auto-discovers the JWKS URI from `${issuer-uri}/.well-known/openid-configuration`, so you typically don't need to set `jwk-set-uri` manually.

### 7.4 Security Filter Chain

```java
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf(AbstractHttpConfigurer::disable) // stateless API, JWT-based
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/api/public/**").permitAll()
                .requestMatchers("/api/admin/**").hasAuthority("ROLE_ADMIN")
                .anyRequest().authenticated()
            )
            .oauth2ResourceServer(oauth2 -> oauth2
                .jwt(jwt -> jwt.jwtAuthenticationConverter(cognitoJwtAuthConverter()))
            );
        return http.build();
    }

    @Bean
    public JwtAuthenticationConverter cognitoJwtAuthConverter() {
        JwtGrantedAuthoritiesConverter grantedAuthoritiesConverter = new JwtGrantedAuthoritiesConverter();
        grantedAuthoritiesConverter.setAuthorityPrefix("ROLE_");
        grantedAuthoritiesConverter.setAuthoritiesClaimName("cognito:groups");

        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(grantedAuthoritiesConverter);
        return converter;
    }
}
```

**Why `cognito:groups`?** Cognito User Pool groups appear in the JWT as the `cognito:groups` claim. Mapping that claim to Spring Security authorities lets you use `hasAuthority("ROLE_ADMIN")` or `@PreAuthorize("hasRole('ADMIN')")` directly, tying IAM-style groups to method-level security.

### 7.5 Access Token vs ID Token

| Token | Purpose | Contains |
|---|---|---|
| **Access Token** | Authorize API calls (what Spring should validate) | `scope`, `client_id`, `cognito:groups` (if configured), expiry |
| **ID Token** | Identify the user (who they are) | `email`, `sub`, `name`, other profile claims |

> **Reviewer note:** For securing backend APIs, validate the **access token**, not the ID token — the access token is scoped for authorization; the ID token is meant for the client app to know who's signed in.

### 7.6 Extracting the Authenticated User

```java
@GetMapping("/me")
public ResponseEntity<UserProfile> me(@AuthenticationPrincipal Jwt jwt) {
    String username = jwt.getClaimAsString("username");
    String email = jwt.getClaimAsString("email");
    List<String> groups = jwt.getClaimAsStringList("cognito:groups");
    return ResponseEntity.ok(new UserProfile(username, email, groups));
}
```

### 7.7 Common Pitfalls / Review Checklist for Cognito Auth

- ✅ Confirm `issuer-uri` matches the **exact** User Pool region + ID — a mismatch silently fails JWKS validation.
- ✅ Validate `aud`/`client_id` claim matches your App Client ID (Spring does this by default when using `issuer-uri`, but double check custom `JwtDecoder` beans if you rolled your own).
- ✅ Set token expiry appropriately in the Cognito App Client (short-lived access tokens + refresh token flow, not long-lived access tokens).
- ✅ Use HTTPS everywhere — JWTs are bearer tokens; anyone who obtains one can use it.
- ✅ Don't put sensitive data in JWT claims — tokens are base64-encoded, not encrypted, and are often logged or cached client-side.
- ✅ Handle **token refresh** on the client side (refresh token flow) — don't force re-login on every access token expiry.
- ✅ If using **Cognito Identity Pools** (not just User Pools) for temporary AWS credentials, keep that concern separate from API authentication — Identity Pools are for AWS resource access (e.g., direct-to-S3 uploads), User Pools are for application login.
- ✅ Test with an **expired** and a **tampered** token to confirm 401s are returned correctly, not 500s.
- ✅ For multi-tenant apps, verify that `cognito:groups` or custom claims are actually being scoped correctly per tenant — don't trust group names alone without checking tenant context.

---

## 8. Quick Review Checklist

Use this as a fast pass when reviewing a PR or service:

- [ ] AWS SDK v2 used (not legacy v1)
- [ ] No hardcoded credentials anywhere in code or config
- [ ] IAM policies follow least privilege
- [ ] SDK clients are singleton beans, not created per-request
- [ ] Retries/timeouts configured on AWS clients
- [ ] S3 uploads use presigned URLs or streaming (not full in-memory buffering)
- [ ] SQS consumers are idempotent and have a DLQ configured
- [ ] SNS/SQS messages carry a correlation/trace ID
- [ ] Sensitive config (ARNs, bucket names) externalized via profile-based config
- [ ] LocalStack/Testcontainers used for integration tests, not live AWS
- [ ] Cognito `issuer-uri` correctly scoped to region + user pool
- [ ] Access token (not ID token) used for API authorization
- [ ] `cognito:groups` mapped to Spring Security authorities correctly
- [ ] Stateless session management (`SessionCreationPolicy.STATELESS`) confirmed
- [ ] 401 vs 403 behavior verified (unauthenticated vs unauthorized)
- [ ] CloudWatch alarms/metrics in place for critical AWS dependencies
