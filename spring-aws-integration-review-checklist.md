# Spring Boot AWS Integration Guide & Review Checklist

A structured reference and reviewer for **Java 21 / Spring Boot** services integrating with AWS: **SNS, SQS, RDS, DynamoDB, and S3**. Uses **Spring Cloud AWS** (`io.awspring.cloud`) built on **AWS SDK v2**. Pairs with the [Kafka Codebase Review Checklist](./kafka-codebase-review-checklist.md) and [Kubernetes Deployment Review Checklist](./kubernetes-deployment-review-checklist.md).

---

## 0. Foundations: Credentials, Config, and Dependencies

- [ ] **AWS SDK v2** used exclusively — no legacy SDK v1 (`com.amazonaws:aws-java-sdk-*`) dependencies mixed in.
- [ ] **Spring Cloud AWS BOM** used to align versions across SNS/SQS/S3/DynamoDB/RDS starters instead of pinning each individually.
- [ ] **No hardcoded access keys/secret keys** anywhere in code or config. Credential resolution relies on the **default credential provider chain**: IAM roles (IRSA on EKS, instance profile on EC2), environment variables, or a local profile for dev only.
- [ ] On EKS, **IRSA (IAM Roles for Service Accounts)** used — ServiceAccount annotated with an IAM role ARN, no static credentials mounted as Secrets.
- [ ] Region is externalized via config (`AWS_REGION` / `spring.cloud.aws.region.static`), not hardcoded.
- [ ] For local development, **LocalStack** used to emulate AWS services instead of hitting real AWS from a dev machine.
- [ ] Client-level configuration (timeouts, retry policy, max connections) is explicit, not left at SDK defaults without review.

**Maven BOM + dependencies:**
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
    <artifactId>spring-cloud-aws-starter-sns</artifactId>
  </dependency>
  <dependency>
    <groupId>io.awspring.cloud</groupId>
    <artifactId>spring-cloud-aws-starter-sqs</artifactId>
  </dependency>
  <dependency>
    <groupId>io.awspring.cloud</groupId>
    <artifactId>spring-cloud-aws-starter-s3</artifactId>
  </dependency>
  <dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>dynamodb-enhanced</artifactId>
  </dependency>
</dependencies>
```

**Base config — `application.yml`:**
```yaml
spring:
  cloud:
    aws:
      region:
        static: ap-southeast-1
      credentials:
        # Omit entirely in prod — let IRSA / instance profile resolve credentials.
        # Only set explicitly for local/dev against LocalStack.
        access-key: ${AWS_ACCESS_KEY_ID:}
        secret-key: ${AWS_SECRET_ACCESS_KEY:}
      sqs:
        endpoint: ${AWS_SQS_ENDPOINT:}   # only set for LocalStack
      sns:
        endpoint: ${AWS_SNS_ENDPOINT:}
      s3:
        endpoint: ${AWS_S3_ENDPOINT:}
```

**IRSA ServiceAccount (EKS) — no static credentials:**
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: orders-service
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/orders-service-role
```

---

## 1. Amazon SNS (Simple Notification Service)

- [ ] SNS used for **fan-out pub/sub** (one event, many independent subscribers) — not as a substitute for point-to-point queuing (that's SQS's job).
- [ ] **SNS → SQS fan-out pattern** used when multiple downstream consumers each need their own durable, independently-consumable copy of a message (each subscriber gets its own SQS queue subscribed to the topic).
- [ ] Messages published with a **message group ID** if using a **FIFO topic**, to preserve ordering per logical entity.
- [ ] **Message attributes** used for filtering (`FilterPolicy` on subscriptions) instead of downstream consumers filtering after receipt — reduces unnecessary message delivery and processing.
- [ ] Publish failures are **handled explicitly** — not fire-and-forget without error handling/retry/alerting.
- [ ] For **cross-account/cross-region** topics, resource policies reviewed and least-privilege scoped.
- [ ] Topic and subscription **encryption at rest** (KMS) enabled for sensitive payloads.
- [ ] **Idempotency** considered downstream, since SNS (like SQS) offers at-least-once delivery — duplicate messages are possible.

**Publishing with `SnsTemplate`:**
```java
@Service
@RequiredArgsConstructor
public class OrderEventPublisher {

    private final SnsTemplate snsTemplate;
    private static final String TOPIC_ARN = "arn:aws:sns:ap-southeast-1:123456789012:order-events";

    public void publishOrderCreated(OrderCreated event) {
        try {
            snsTemplate.convertAndSend(TOPIC_ARN, event, Map.of(
                    "eventType", MessageAttributeValue.builder()
                            .dataType("String")
                            .stringValue("ORDER_CREATED")
                            .build()
            ));
        } catch (MessagingException ex) {
            log.error("Failed to publish OrderCreated event for orderId={}", event.orderId(), ex);
            // route to metrics/alerting; consider a local outbox for guaranteed delivery
            throw ex;
        }
    }
}
```

**Raw SDK client for more control (e.g., FIFO topic with message group/dedup IDs):**
```java
@Bean
public SnsClient snsClient() {
    return SnsClient.builder()
            .region(Region.AP_SOUTHEAST_1)
            .build(); // credentials resolved via default provider chain
}
```
```java
public void publishToFifoTopic(OrderCreated event) {
    PublishRequest request = PublishRequest.builder()
            .topicArn(FIFO_TOPIC_ARN)
            .message(toJson(event))
            .messageGroupId(event.orderId())            // ordering scope
            .messageDeduplicationId(event.eventId())     // dedup within 5-min window
            .build();
    snsClient.publish(request);
}
```

**Subscription filter policy (Terraform, referenced for context on the consumer side):**
```json
{
  "eventType": ["ORDER_CREATED", "ORDER_CANCELLED"]
}
```

---

## 2. Amazon SQS

- [ ] **Standard vs FIFO** queue choice is deliberate and documented — FIFO for strict ordering + exactly-once processing semantics, Standard for higher throughput where ordering doesn't matter.
- [ ] **Visibility timeout** set higher than the expected max processing time, to avoid a message becoming visible again (and reprocessed by another consumer) while still being handled.
- [ ] **Long polling** enabled (`ReceiveMessageWaitTimeSeconds > 0`, typically 20s) instead of short polling — reduces empty receives and cost.
- [ ] A **Dead Letter Queue (DLQ)** is configured via **redrive policy** with a sane `maxReceiveCount` (typically 3–5) before a message is routed off the main queue.
- [ ] DLQ has **monitoring/alerting** on message count — an unmonitored DLQ is a silent failure sink (same principle as Kafka DLT).
- [ ] Listener **acknowledgment mode** matches processing guarantees needed — manual ack after successful processing (default in Spring Cloud AWS is auto-delete on successful listener return, which is usually correct, but batch/manual ack reviewed for complex flows).
- [ ] **Idempotent consumer logic** — SQS guarantees at-least-once delivery, duplicates are possible (especially Standard queues).
- [ ] Payloads that may exceed the 256KB SQS limit use the **S3 extended client pattern** (large payload stored in S3, SQS message carries a pointer).
- [ ] **Batch operations** (`sendMessageBatch`, batched receive/delete) used where throughput matters, instead of one-at-a-time API calls.
- [ ] Consumer concurrency (`maxConcurrentMessages` / listener container concurrency) tuned to downstream capacity — avoid overwhelming a database or third-party API.
- [ ] Queue **encryption at rest** (SQS-managed SSE or KMS) enabled for sensitive data.

**Listener with `@SqsListener`:**
```java
@Component
@RequiredArgsConstructor
public class OrderEventListener {

    private final OrderProcessingService orderProcessingService;

    @SqsListener(value = "order-events-queue", maxConcurrentMessages = "5", maxMessagesPerPoll = "10")
    public void onMessage(OrderCreated event, @Header("eventType") String eventType) {
        if (orderProcessingService.alreadyProcessed(event.orderId())) {
            return; // idempotent no-op; message still deleted on successful return
        }
        orderProcessingService.process(event);
    }
}
```

**Manual acknowledgment for finer control:**
```java
@SqsListener(value = "order-events-queue", acknowledgementMode = SqsListenerAcknowledgementMode.MANUAL)
public void onMessage(OrderCreated event, Acknowledgement acknowledgement) {
    orderProcessingService.process(event);
    acknowledgement.acknowledge(); // only deletes from queue after this call
}
```

**Queue configuration (Terraform, referenced for context — visibility timeout + DLQ redrive):**
```hcl
resource "aws_sqs_queue" "order_events_dlq" {
  name                      = "order-events-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue" "order_events" {
  name                       = "order-events-queue"
  visibility_timeout_seconds = 60
  receive_wait_time_seconds  = 20 # long polling

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.order_events_dlq.arn
    maxReceiveCount     = 5
  })
}
```

**Publishing with `SqsTemplate`:**
```java
@Service
@RequiredArgsConstructor
public class OrderCommandSender {

    private final SqsTemplate sqsTemplate;

    public void send(OrderCreated event) {
        sqsTemplate.send(to -> to
                .queue("order-events-queue")
                .payload(event)
                .header("eventType", "ORDER_CREATED"));
    }

    public void sendBatch(List<OrderCreated> events) {
        List<Message<OrderCreated>> messages = events.stream()
                .map(e -> MessageBuilder.withPayload(e).build())
                .toList();
        sqsTemplate.sendMany("order-events-queue", messages);
    }
}
```

---

## 3. Amazon RDS

- [ ] Connection pooling via **HikariCP** (Spring Boot default) is tuned — `maximum-pool-size` sized relative to RDS instance's `max_connections` and the number of app instances (avoid connection exhaustion across a fleet of pods).
- [ ] **IAM database authentication** considered for short-lived, credential-rotation-free DB auth instead of static passwords, where the workload supports it.
- [ ] If static credentials are used, they come from **Secrets Manager** (or the k8s Secret sourced from it via External Secrets Operator) — not hardcoded, not in plaintext config.
- [ ] **Read replicas** used for read-heavy workloads via a separate read-only `DataSource`/routing `DataSource`, not all traffic hitting the writer instance.
- [ ] **Multi-AZ** enabled for production for automatic failover; application's retry/reconnect logic tested against a failover event.
- [ ] **SSL/TLS enforced** for connections (`sslmode=require` for Postgres, `useSSL=true` for MySQL) — verify RDS parameter group enforces it too.
- [ ] Schema migrations managed via **Flyway or Liquibase**, run as part of deployment (init container / CI job), not manually.
- [ ] **`@Transactional`** boundaries reviewed — no overly broad transactions spanning slow I/O (e.g., HTTP calls) that hold DB connections open unnecessarily.
- [ ] Query performance reviewed — N+1 query patterns checked (especially with JPA/Hibernate lazy loading), appropriate indexes exist for hot query paths.
- [ ] **RDS Proxy** considered for Lambda-heavy or high-churn-connection architectures to pool connections at the infra layer.
- [ ] Automated backups and retention period configured to meet RPO requirements; point-in-time recovery tested at least once.

**DataSource + HikariCP config:**
```yaml
spring:
  datasource:
    url: jdbc:postgresql://orders-db.cluster-xyz.ap-southeast-1.rds.amazonaws.com:5432/orders?sslmode=require
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
    hikari:
      maximum-pool-size: 10
      minimum-idle: 2
      connection-timeout: 30000
      idle-timeout: 600000
      max-lifetime: 1800000
      leak-detection-threshold: 60000
  jpa:
    hibernate:
      ddl-auto: validate   # never "update"/"create" in prod — Flyway owns schema
    properties:
      hibernate:
        jdbc:
          batch_size: 50
        order_inserts: true
        order_updates: true

  flyway:
    enabled: true
    locations: classpath:db/migration
    baseline-on-migrate: true
```

**Read/write routing DataSource for read replicas:**
```java
public class ReplicaRoutingDataSource extends AbstractRoutingDataSource {
    @Override
    protected Object determineCurrentLookupKey() {
        return TransactionSynchronizationManager.isCurrentTransactionReadOnly() ? "replica" : "writer";
    }
}

@Bean
public DataSource routingDataSource(DataSource writerDataSource, DataSource replicaDataSource) {
    ReplicaRoutingDataSource routingDataSource = new ReplicaRoutingDataSource();
    routingDataSource.setTargetDataSources(Map.of(
            "writer", writerDataSource,
            "replica", replicaDataSource));
    routingDataSource.setDefaultTargetDataSource(writerDataSource);
    return routingDataSource;
}
```
```java
@Transactional(readOnly = true)
public List<OrderSummary> findRecentOrders(String customerId) {
    return orderRepository.findRecentByCustomerId(customerId); // routed to replica
}
```

**Secrets Manager-backed credentials via injected env vars (populated by ESO in k8s):**
```yaml
spring:
  datasource:
    username: ${DB_USERNAME}   # from k8s Secret <- AWS Secrets Manager via ESO
    password: ${DB_PASSWORD}
```

---

## 4. Amazon DynamoDB

- [ ] **DynamoDB Enhanced Client** used for type-safe object mapping instead of hand-building `AttributeValue` maps with the low-level client.
- [ ] **Partition key design** reviewed to avoid hot partitions — high-cardinality, evenly-distributed key chosen; avoid monotonically increasing keys (e.g., raw timestamps) as the sole partition key for write-heavy tables.
- [ ] **Access patterns designed up front** (single-table design or purpose-built GSIs) — DynamoDB is not queried ad hoc like SQL; verify the table/index design actually supports the app's real query patterns.
- [ ] **Capacity mode** (`PROVISIONED` vs `PAY_PER_REQUEST`) chosen deliberately based on traffic predictability; auto-scaling configured if using `PROVISIONED`.
- [ ] **Conditional writes** (`ConditionExpression`) used to prevent race conditions on updates instead of read-then-write patterns.
- [ ] **Batch operations** (`BatchGetItem`, `BatchWriteItem`) used for bulk access instead of looped single-item calls; batch size limits (25 items/request) respected with chunking.
- [ ] **Pagination** handled correctly for `Query`/`Scan` — `LastEvaluatedKey` loop implemented, not assuming a single page returns all results.
- [ ] **`Scan` operations avoided** in hot paths — prefer `Query` against a well-designed key/GSI; `Scan` reserved for infrequent batch/admin jobs.
- [ ] **TTL (Time To Live)** attribute configured for tables with naturally expiring data (e.g., idempotency dedup records) instead of manual cleanup jobs.
- [ ] **Point-in-time recovery (PITR)** enabled for production tables.
- [ ] Retry/backoff for `ProvisionedThroughputExceededException` handled — SDK v2 default retry policy reviewed, not disabled.

**Entity mapping with the Enhanced Client:**
```java
@DynamoDbBean
public class OrderRecord {
    private String orderId;      // partition key
    private String status;       // sort key or GSI key
    private BigDecimal amount;
    private Instant createdAt;
    private Long ttl;

    @DynamoDbPartitionKey
    public String getOrderId() { return orderId; }

    @DynamoDbSortKey
    public String getStatus() { return status; }

    @DynamoDbSecondaryPartitionKey(indexNames = "status-index")
    public String getStatusIndexKey() { return status; }
}
```

**Client + table bean setup:**
```java
@Bean
public DynamoDbEnhancedClient dynamoDbEnhancedClient() {
    DynamoDbClient client = DynamoDbClient.builder()
            .region(Region.AP_SOUTHEAST_1)
            .build();
    return DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();
}

@Bean
public DynamoDbTable<OrderRecord> orderTable(DynamoDbEnhancedClient enhancedClient) {
    return enhancedClient.table("orders", TableSchema.fromBean(OrderRecord.class));
}
```

**Conditional write (optimistic concurrency, avoids read-then-write race):**
```java
public void markShipped(String orderId, String currentStatus) {
    OrderRecord record = new OrderRecord();
    record.setOrderId(orderId);
    record.setStatus("SHIPPED");

    orderTable.updateItem(builder -> builder
            .item(record)
            .conditionExpression(Expression.builder()
                    .expression("status = :expectedStatus")
                    .expressionValues(Map.of(":expectedStatus", AttributeValue.builder().s(currentStatus).build()))
                    .build()));
}
```

**Paginated query against a GSI:**
```java
public List<OrderRecord> findOrdersByStatus(String status) {
    List<OrderRecord> results = new ArrayList<>();
    QueryConditional condition = QueryConditional.keyEqualTo(k -> k.partitionValue(status));

    orderTable.index("status-index")
            .query(r -> r.queryConditional(condition))
            .stream()
            .forEach(page -> results.addAll(page.items())); // SDK handles LastEvaluatedKey paging internally per page
    return results;
}
```

**Batch write, chunked to the 25-item API limit:**
```java
public void saveAll(List<OrderRecord> records) {
    Lists.partition(records, 25).forEach(chunk -> {
        WriteBatch.Builder<OrderRecord> batch = WriteBatch.builder(OrderRecord.class).mappedTableResource(orderTable);
        chunk.forEach(batch::addPutItem);
        enhancedClient.batchWriteItem(r -> r.writeBatches(batch.build()));
    });
}
```

---

## 5. Amazon S3

- [ ] **Bucket policies enforce least privilege** — application IAM role/policy scoped to specific bucket + prefix, not `s3:*` on `*`.
- [ ] **Block Public Access** enabled at the bucket (and account) level unless the bucket is genuinely meant to serve public content (and even then, prefer CloudFront + OAC over a public bucket).
- [ ] **Server-side encryption** enabled (SSE-S3 or SSE-KMS) — verify via bucket default encryption setting, not relying solely on per-request headers.
- [ ] **Versioning** enabled for buckets holding important/irreplaceable data, with a lifecycle policy to manage old version cleanup/cost.
- [ ] **Multipart upload** used for large files (SDK's `S3TransferManager` handles this automatically) instead of a single `PutObject` call for large payloads.
- [ ] **Presigned URLs** used for direct client upload/download where appropriate, to avoid proxying large file bytes through the application server.
- [ ] **Streaming** used for large object reads/writes (`ResponseInputStream`, `RequestBody.fromInputStream`) — avoid loading entire large objects into memory (`byte[]`/`String`) in the app.
- [ ] **Lifecycle rules** configured (transition to IA/Glacier, expiration) for cost management on buckets with predictable data aging.
- [ ] Object keys designed to avoid **request-rate hot-partitioning** on high-throughput prefixes (S3 auto-scales partitions but very bursty sequential-key writes can still throttle early).
- [ ] Retries and timeouts configured for S3 client calls; transient errors (`SlowDown`, 5xx) handled with backoff rather than immediate failure.
- [ ] Checksums (`Content-MD5` or SDK-computed CRC32/SHA) verified for critical uploads to detect corruption.

**S3 client bean + basic put/get with `S3Template` (Spring Cloud AWS):**
```java
@Service
@RequiredArgsConstructor
public class InvoiceStorageService {

    private final S3Template s3Template;
    private static final String BUCKET = "orders-invoices-prod";

    public void upload(String orderId, InputStream content, long contentLength) {
        s3Template.upload(BUCKET, "invoices/%s.pdf".formatted(orderId), content,
                ObjectMetadata.builder().contentType("application/pdf").build());
    }

    public InputStream download(String orderId) {
        S3Resource resource = s3Template.download(BUCKET, "invoices/%s.pdf".formatted(orderId));
        return resource.getInputStream(); // stream directly to caller, don't buffer fully in memory
    }
}
```

**Multipart upload for large files via `S3TransferManager`:**
```java
@Bean
public S3TransferManager s3TransferManager(S3AsyncClient s3AsyncClient) {
    return S3TransferManager.builder().s3Client(s3AsyncClient).build();
}

public void uploadLargeFile(Path filePath, String key) {
    UploadFileRequest request = UploadFileRequest.builder()
            .putObjectRequest(b -> b.bucket(BUCKET).key(key))
            .source(filePath)
            .build();

    FileUpload upload = transferManager.uploadFile(request);
    upload.completionFuture().join(); // or handle async with .whenComplete(...)
}
```

**Presigned URL for direct client upload (avoids proxying bytes through the app):**
```java
@Bean
public S3Presigner s3Presigner() {
    return S3Presigner.builder().region(Region.AP_SOUTHEAST_1).build();
}

public URL generateUploadUrl(String key, Duration expiry) {
    PutObjectRequest objectRequest = PutObjectRequest.builder()
            .bucket(BUCKET)
            .key(key)
            .build();

    PresignedPutObjectRequest presigned = s3Presigner.presignPutObject(b -> b
            .signatureDuration(expiry)
            .putObjectRequest(objectRequest));

    return presigned.url();
}
```

**Bucket policy — least privilege (Terraform, referenced for context):**
```hcl
resource "aws_s3_bucket_public_access_block" "invoices" {
  bucket                  = aws_s3_bucket.invoices.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "invoices" {
  bucket = aws_s3_bucket.invoices.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}
```

---

## 6. Cross-Cutting Best Practices

### 6.1 Error Handling & Resilience
- [ ] AWS SDK exceptions distinguished: **retryable** (`SdkException` with throttling/5xx) vs **non-retryable** (`SdkClientException` validation errors, 4xx) — don't retry blindly on everything.
- [ ] **Resilience4j** (or SDK-native retry policies) used for circuit breaking around AWS calls that are part of a critical request path, so an AWS outage/throttling event doesn't cascade.
- [ ] SDK's built-in **adaptive retry mode** considered for services prone to throttling (DynamoDB, S3 under load) instead of a fixed retry count.
```java
@Bean
public DynamoDbClient dynamoDbClient() {
    return DynamoDbClient.builder()
            .region(Region.AP_SOUTHEAST_1)
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder()
                            .retryMode(RetryMode.ADAPTIVE)
                            .build())
                    .apiCallTimeout(Duration.ofSeconds(5))
                    .build())
            .build();
}
```

### 6.2 Observability
- [ ] SDK metrics (`software.amazon.awssdk:metrics-spi` + CloudWatch or Micrometer publisher) enabled to track request latency, retries, and throttling per AWS service.
- [ ] Structured logging includes AWS request IDs (`x-amzn-RequestId`) on failures for support-case correlation.
- [ ] X-Ray or OpenTelemetry SDK instrumentation enabled to trace calls across SQS → app → DynamoDB/S3/RDS for end-to-end visibility.

### 6.3 IAM & Security
- [ ] Every service has a **dedicated IAM role** — no shared "god role" reused across unrelated services.
- [ ] IAM policies scoped to **specific resource ARNs** (specific bucket/table/queue/topic), not wildcarded across all resources of a type.
- [ ] Sensitive payloads (PII in SQS/SNS messages, S3 objects, DynamoDB items) reviewed against data classification policy — encrypted at rest and in transit, and field-level encryption considered for highly sensitive attributes.

**Example least-privilege IAM policy (SQS consumer):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:ap-southeast-1:123456789012:order-events-queue"
    }
  ]
}
```

### 6.4 Testing
- [ ] **Testcontainers LocalStack module** (or a standalone LocalStack container) used for integration tests against SQS/SNS/S3/DynamoDB — not mocking the SDK client at the unit-test level for integration-level confidence.
- [ ] Contract/idempotency tests exist simulating duplicate message delivery for SQS/SNS consumers.
```java
@Testcontainers
@SpringBootTest
class OrderEventListenerIntegrationTest {

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.4"))
            .withServices(SQS, SNS, S3, DYNAMODB);

    @DynamicPropertySource
    static void awsProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.aws.sqs.endpoint", () -> localstack.getEndpointOverride(SQS).toString());
        registry.add("spring.cloud.aws.region.static", localstack::getRegion);
    }

    @Test
    void duplicateMessage_isProcessedIdempotently() {
        // publish the same message twice, assert side effects happened exactly once
    }
}
```

### 6.5 Cost Awareness
- [ ] Polling intervals, batch sizes, and capacity modes reviewed with cost in mind (e.g., SQS long polling reduces API call costs; DynamoDB `PAY_PER_REQUEST` vs `PROVISIONED` trade-off understood for the workload's traffic shape).
- [ ] S3 storage class and lifecycle rules match actual access patterns (don't leave everything in Standard indefinitely).

---

## 7. Quick Red Flags Checklist

- 🚩 Static AWS access keys/secret keys committed to the repo or baked into container images.
- 🚩 IAM policy using `"Resource": "*"` or `"Action": "s3:*"` for a single-purpose service.
- 🚩 S3 bucket without Block Public Access enabled, holding non-public data.
- 🚩 SQS queue with no DLQ / redrive policy configured.
- 🚩 DynamoDB table using `Scan` in a request-serving hot path.
- 🚩 RDS credentials hardcoded in `application.yml` committed to source control.
- 🚩 No idempotency handling despite SQS/SNS at-least-once delivery semantics.
- 🚩 Entire large S3 objects loaded into memory (`byte[]`) instead of streamed.
- 🚩 HikariCP pool size unbounded or default, unaccounted for against RDS `max_connections` across the full pod fleet.
- 🚩 No monitoring/alerting on DLQ depth, DynamoDB throttling, or S3 4xx/5xx rates.

---

## 8. Review Sign-off Template

```
Reviewer:         ____________________
Date:             ____________________
Service:          ____________________

[ ] Credentials resolved via IAM role (IRSA), no static keys
[ ] SNS usage justified (fan-out) with filter policies where applicable
[ ] SQS DLQ + redrive policy configured and monitored
[ ] RDS connection pool sized correctly; migrations via Flyway/Liquibase
[ ] DynamoDB access patterns match table/GSI design; no hot-path Scans
[ ] S3 buckets private by default, encrypted, lifecycle rules set
[ ] IAM policies least-privilege, resource-scoped
[ ] Idempotency handled for at-least-once delivery (SQS/SNS)
[ ] Integration tests use LocalStack/Testcontainers
[ ] No red flags identified (or flagged below)

Notes / Follow-ups:
_______________________________________________
_______________________________________________
```
