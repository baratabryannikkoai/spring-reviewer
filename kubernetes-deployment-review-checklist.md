# Kubernetes Deployment Review Checklist

A structured reviewer for the Kubernetes deployment layer of a **Java 21 / Spring Boot / Kafka** service. Use this alongside the [Kafka Codebase Review Checklist](./kafka-codebase-review-checklist.md) to audit manifests, Kustomize overlays, and cluster-facing configuration.

---

## 1. Multi-Environment Deployment with Kustomize

- [ ] A `base/` directory holds environment-agnostic manifests (Deployment, Service, ConfigMap, ServiceAccount, HPA, etc.).
- [ ] `overlays/<env>/` directories (e.g., `dev`, `staging`, `prod`) patch the base — no duplicated full manifests per environment.
- [ ] Base manifests contain **no environment-specific values** (no hardcoded replica counts, resource limits, hostnames, or image tags that differ per env).
- [ ] `kustomization.yaml` at each overlay uses `patchesStrategicMerge` / `patches` (Kustomize v4+ syntax) rather than fragile JSON patches where avoidable.
- [ ] **Image tag** is set per-environment via `images:` field in the overlay, not baked into the base Deployment.
- [ ] **Namespace** is set via overlay (`namespace:` field), not hardcoded in base manifests.
- [ ] **Name prefixes/suffixes** (`namePrefix`/`nameSuffix`) or **labels** (`commonLabels`) used to distinguish environment resources and avoid collisions.
- [ ] `replicas` count differs sensibly per environment (e.g., 1 for dev, 3+ for prod) via overlay patch.
- [ ] Overlay directory structure is consistent and predictable:
  ```
  k8s/
  ├── base/
  │   ├── deployment.yaml
  │   ├── service.yaml
  │   ├── configmap.yaml
  │   ├── hpa.yaml
  │   ├── serviceaccount.yaml
  │   └── kustomization.yaml
  └── overlays/
      ├── dev/
      │   ├── kustomization.yaml
      │   ├── configmap-patch.yaml
      │   └── replica-patch.yaml
      ├── staging/
      │   └── ...
      └── prod/
          └── ...
  ```
- [ ] `kubectl kustomize overlays/<env>` (or `kustomize build`) is run in CI to validate rendered output before apply — catches merge errors early.
- [ ] No use of unmanaged `kubectl apply -f` against raw manifests in any environment — everything flows through Kustomize (or a GitOps controller referencing it).
- [ ] Components (`components/`) used for optional, reusable cross-cutting concerns (e.g., a "debug-sidecar" component) if the project has such variability.

**Base `kustomization.yaml`:**
```yaml
# k8s/base/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - deployment.yaml
  - service.yaml
  - configmap.yaml
  - secret.yaml
  - hpa.yaml
  - serviceaccount.yaml
  - poddisruptionbudget.yaml

commonLabels:
  app.kubernetes.io/name: orders-service
  app.kubernetes.io/part-of: order-management
```

**Production overlay `kustomization.yaml`:**
```yaml
# k8s/overlays/prod/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: orders-prod
namePrefix: prod-

resources:
  - ../../base

images:
  - name: orders-service
    newName: registry.example.com/orders-service
    newTag: 1.4.2

replicas:
  - name: orders-service
    count: 4

patches:
  - path: resource-limits-patch.yaml
    target:
      kind: Deployment
      name: orders-service

configMapGenerator:
  - name: orders-service-config
    behavior: merge
    envs:
      - config.prod.env

secretGenerator:
  - name: orders-service-secrets
    behavior: merge
    envs:
      - secrets.prod.env

generatorOptions:
  disableNameSuffixHash: true
```

**Dev overlay (lighter footprint, verbose logging):**
```yaml
# k8s/overlays/dev/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: orders-dev
namePrefix: dev-

resources:
  - ../../base

images:
  - name: orders-service
    newName: registry.example.com/orders-service
    newTag: dev-latest

replicas:
  - name: orders-service
    count: 1

configMapGenerator:
  - name: orders-service-config
    behavior: merge
    literals:
      - LOG_LEVEL=DEBUG
      - SPRING_PROFILES_ACTIVE=dev
```

**Resource patch for prod (`resource-limits-patch.yaml`):**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-service
spec:
  template:
    spec:
      containers:
        - name: orders-service
          resources:
            requests:
              cpu: 500m
              memory: 768Mi
            limits:
              cpu: "1"
              memory: 1Gi
```

---

## 2. ConfigMap and Secrets

- [ ] **Non-sensitive config** (Kafka topic names, feature flags, log levels, timeouts) lives in `ConfigMap`; **sensitive values** (DB passwords, SASL credentials, API keys, TLS keys) live in `Secret` — never mixed.
- [ ] Secrets are **not committed in plaintext** to the repo. Options reviewed: `SealedSecrets`, `SOPS`, External Secrets Operator (ESO) pulling from Vault/AWS Secrets Manager/GCP Secret Manager, or CI-injected secrets.
- [ ] `configMapGenerator` / `secretGenerator` used in Kustomize so content changes trigger a **new hash-suffixed name**, forcing a rolling pod restart on config change (unless `disableNameSuffixHash: true` is intentionally set for prod stability).
- [ ] ConfigMap/Secret consumed via **environment variables** or **mounted volumes** — pattern is consistent across the codebase (prefer volume mounts for larger config like `application.yml`; env vars for small discrete values).
- [ ] Spring Boot's `application.yml` externalized config keys map cleanly to injected env vars (`SPRING_KAFKA_BOOTSTRAP_SERVERS`, `SPRING_DATASOURCE_PASSWORD`, etc.) — naming convention documented.
- [ ] Secrets have **restricted RBAC access** — only the relevant ServiceAccount/namespace can read them (`Role`/`RoleBinding` scoped, not cluster-wide).
- [ ] **`immutable: true`** set on ConfigMaps/Secrets where content shouldn't change without a new object (performance + safety, avoids accidental in-place edits).
- [ ] No secret values appear in **pod logs, `kubectl describe` output, or CI logs** — verify `envFrom` combined with careful logging practices (avoid logging `System.getenv()` dumps).
- [ ] If using **Secret volume mounts**, confirm `defaultMode` restricts file permissions (e.g., `0400`) and `readOnly: true` is set on the volume mount.
- [ ] Rotation strategy exists for long-lived secrets (DB creds, Kafka SASL) — e.g., ESO polling interval, or a documented manual rotation runbook.

**ConfigMap (non-sensitive) — `base/configmap.yaml`:**
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: orders-service-config
data:
  SPRING_PROFILES_ACTIVE: "prod"
  SPRING_KAFKA_BOOTSTRAP_SERVERS: "kafka-broker:9092"
  APP_KAFKA_ORDERS_TOPIC: "orders"
  APP_KAFKA_ORDERS_DLT_TOPIC: "orders.DLT"
  LOG_LEVEL: "INFO"
  MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE: "health,info,metrics,prometheus"
```

**Secret (sensitive) — sourced via External Secrets Operator, not committed:**
```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: orders-service-secrets
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: orders-service-secrets
    creationPolicy: Owner
  data:
    - secretKey: SPRING_DATASOURCE_PASSWORD
      remoteRef:
        key: secret/orders-service/db
        property: password
    - secretKey: SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG
      remoteRef:
        key: secret/orders-service/kafka
        property: sasl-jaas-config
```

**Deployment consuming both via `envFrom`, plus a mounted volume for `application.yml` overrides:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-service
spec:
  template:
    spec:
      containers:
        - name: orders-service
          image: orders-service
          envFrom:
            - configMapRef:
                name: orders-service-config
            - secretRef:
                name: orders-service-secrets
          volumeMounts:
            - name: app-config
              mountPath: /config
              readOnly: true
      volumes:
        - name: app-config
          configMap:
            name: orders-service-config
            defaultMode: 0440
```

---

## 3. External Exposure (Service / LoadBalancer / Ingress)

- [ ] Internal-only traffic (e.g., inter-service calls within the cluster) uses `ClusterIP` — not exposed externally unless required.
- [ ] Public-facing HTTP APIs are exposed via **Ingress** (preferred) fronting a `ClusterIP` Service, rather than a raw `LoadBalancer` Service per deployment — avoids provisioning a cloud LB per microservice.
- [ ] If a **`LoadBalancer`** Service type is used directly (e.g., for a gateway or a protocol Ingress can't handle), it's justified and documented (cost/complexity trade-off understood — each `LoadBalancer` typically provisions a real cloud load balancer).
- [ ] **TLS termination** configured — either at the Ingress (cert-manager issued cert) or at the LB — no plaintext HTTP exposed externally.
- [ ] **Ingress annotations** reviewed for the specific controller in use (NGINX, ALB, Traefik, etc.) — e.g., rate limiting, timeout, body size limits set appropriately for the API's needs.
- [ ] Service `selector` labels correctly match Pod template labels — verify with `kubectl get endpoints` that the Service actually routes to running pods.
- [ ] **`sessionAffinity`** reviewed — set to `ClientIP` only if the app is stateful and requires sticky sessions (Spring Boot REST APIs are typically stateless; avoid unless justified).
- [ ] Correct **`targetPort`** vs container's actual listening port (Spring Boot default `8080`, or custom `server.port`) — verified they match.
- [ ] **NetworkPolicy** in place restricting which namespaces/pods can reach this Service, especially for internal-only services — default-deny with explicit allow rules.
- [ ] DNS name / hostname per environment is distinct (`orders-api-dev.example.com`, `orders-api.example.com`) and provisioned via Ingress `host` rules, not hardcoded IPs.
- [ ] If exposing a **gRPC** or non-HTTP1.1 protocol, confirm the Ingress controller/LB supports it (e.g., NGINX Ingress needs `nginx.ingress.kubernetes.io/backend-protocol: "GRPC"`).

**ClusterIP Service (internal), fronted by Ingress:**
```yaml
apiVersion: v1
kind: Service
metadata:
  name: orders-service
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: orders-service
  ports:
    - name: http
      port: 80
      targetPort: 8080
    - name: metrics
      port: 9090
      targetPort: 9090
```

**Ingress with TLS (cert-manager) — production overlay:**
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: orders-service
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
    nginx.ingress.kubernetes.io/rewrite-target: /
    nginx.ingress.kubernetes.io/limit-rps: "50"
spec:
  ingressClassName: nginx
  tls:
    - hosts:
        - orders-api.example.com
      secretName: orders-service-tls
  rules:
    - host: orders-api.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: orders-service
                port:
                  number: 80
```

**LoadBalancer Service (only when a dedicated cloud LB is genuinely required):**
```yaml
apiVersion: v1
kind: Service
metadata:
  name: orders-gateway
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
    service.beta.kubernetes.io/aws-load-balancer-internal: "false"
spec:
  type: LoadBalancer
  selector:
    app.kubernetes.io/name: orders-gateway
  ports:
    - port: 443
      targetPort: 8443
```

**NetworkPolicy restricting inbound access:**
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: orders-service-allow-ingress
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: orders-service
  policyTypes:
    - Ingress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: ingress-nginx
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: api-gateway
      ports:
        - protocol: TCP
          port: 8080
```

---

## 4. Horizontal Pod Autoscaler (HPA)

- [ ] HPA target references the correct Deployment (`scaleTargetRef`) and `apiVersion: autoscaling/v2` is used (not deprecated `v1`).
- [ ] **`minReplicas`** and **`maxReplicas`** set per environment via overlay (e.g., dev: 1–2, prod: 3–10) — not left as a single hardcoded value shared across envs.
- [ ] Scaling metric is appropriate for a Kafka consumer workload — CPU/memory alone is often insufficient; consider **consumer lag-based scaling** (KEDA `ScaledObject` with a Kafka trigger) in addition to or instead of plain CPU-based HPA.
- [ ] `resources.requests` are set on the container (HPA CPU/memory percentage targets are calculated relative to requests) — HPA silently does nothing useful without these.
- [ ] **Scaling behavior** (`behavior.scaleUp` / `behavior.scaleDown`) configured with stabilization windows to avoid flapping (rapid scale up/down cycles).
- [ ] Verified `kubectl top pods` / metrics-server (or Prometheus Adapter for custom metrics) is actually deployed in the cluster — HPA silently fails to scale without a working metrics pipeline.
- [ ] Understand and document the **interaction between HPA and Kafka partition count**: scaling consumer pods beyond the partition count for a topic yields idle pods doing no work — `maxReplicas` should generally not exceed the highest partition count among consumed topics (per consumer group).
- [ ] **PodDisruptionBudget (PDB)** exists alongside HPA so voluntary disruptions (node drains, cluster upgrades) don't take down too many pods at once, conflicting with autoscaling assumptions.
- [ ] Load/soak testing performed to validate scale-up latency is acceptable for the workload's traffic/message-burst patterns (cold start time, JVM warm-up factored in).
- [ ] If using **KEDA**, confirm `pollingInterval`, `cooldownPeriod`, and `lagThreshold` are tuned — overly aggressive polling adds broker load; overly lazy polling causes slow reaction to backlog spikes.

**Standard HPA (CPU + memory based) — base:**
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: orders-service
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: orders-service
  minReplicas: 2
  maxReplicas: 6
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 75
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 30
      policies:
        - type: Pods
          value: 2
          periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Pods
          value: 1
          periodSeconds: 120
```

**Prod overlay patch — wider scaling range:**
```yaml
# k8s/overlays/prod/hpa-patch.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: orders-service
spec:
  minReplicas: 4
  maxReplicas: 12
```

**KEDA `ScaledObject` for Kafka consumer-lag-based scaling (recommended for consumer workloads):**
```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: orders-service-consumer
spec:
  scaleTargetRef:
    name: orders-service
  minReplicaCount: 2
  maxReplicaCount: 8   # <= partition count of "orders" topic
  cooldownPeriod: 120
  pollingInterval: 15
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: kafka-broker:9092
        consumerGroup: orders-service-group
        topic: orders
        lagThreshold: "50"
        offsetResetPolicy: earliest
```

**Matching PodDisruptionBudget:**
```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: orders-service
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: orders-service
```

---

## 5. Additional Deployment Concerns (Recommended Additions)

These aren't in the original scope but are commonly missing and worth reviewing for a production-grade Spring Boot/Kafka deployment.

### 5.1 Resource Requests & Limits
- [ ] Every container has explicit `resources.requests` and `resources.limits` for both `cpu` and `memory` — required for HPA, scheduling, and QoS class determination.
- [ ] JVM heap sizing (`-Xmx`) aligned with the container memory limit (e.g., via `-XX:MaxRAMPercentage=75.0`) to avoid OOMKills from JVM overcommitting beyond the cgroup limit.
```yaml
resources:
  requests:
    cpu: 250m
    memory: 512Mi
  limits:
    cpu: "1"
    memory: 1Gi
env:
  - name: JAVA_TOOL_OPTIONS
    value: "-XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0"
```

### 5.2 Liveness, Readiness, and Startup Probes
- [ ] Probes use Spring Boot Actuator's dedicated **Kubernetes probe groups** (`/actuator/health/liveness`, `/actuator/health/readiness`) rather than the generic `/actuator/health`.
- [ ] **`startupProbe`** configured for JVM apps with non-trivial boot time, so `livenessProbe` doesn't kill a pod that's still starting up.
- [ ] Readiness probe correctly reflects Kafka consumer/producer connectivity (`management.health.kafka.enabled=true`) so traffic/rebalancing isn't routed to a pod that can't reach the broker.
```yaml
readinessProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 10
  failureThreshold: 3
livenessProbe:
  httpGet:
    path: /actuator/health/liveness
    port: 8080
  periodSeconds: 15
  failureThreshold: 3
startupProbe:
  httpGet:
    path: /actuator/health/liveness
    port: 8080
  failureThreshold: 30
  periodSeconds: 10
```
```yaml
# application.yml
management:
  endpoint:
    health:
      probes:
        enabled: true
  health:
    livenessstate:
      enabled: true
    readinessstate:
      enabled: true
```

### 5.3 Rolling Update Strategy
- [ ] `strategy.type: RollingUpdate` with `maxSurge`/`maxUnavailable` tuned to avoid dropping consumer capacity during deploys (important for Kafka consumer group rebalancing — too many pods restarting at once causes rebalance storms).
- [ ] `terminationGracePeriodSeconds` sized to allow in-flight message processing and clean consumer group leave (`preStop` hook or graceful shutdown) before SIGKILL.
```yaml
spec:
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    spec:
      terminationGracePeriodSeconds: 45
      containers:
        - name: orders-service
          lifecycle:
            preStop:
              exec:
                command: ["sh", "-c", "sleep 10"]
```

### 5.4 Security Context
- [ ] Containers run as **non-root** (`runAsNonRoot: true`, explicit `runAsUser`), with a **read-only root filesystem** where feasible.
- [ ] `allowPrivilegeEscalation: false` and Linux capabilities dropped (`drop: ["ALL"]`) unless a specific capability is required.
```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false
  capabilities:
    drop: ["ALL"]
  seccompProfile:
    type: RuntimeDefault
```

### 5.5 Image & Supply Chain
- [ ] Image tags are **immutable** (semantic version or commit SHA) — never `:latest` in staging/prod overlays.
- [ ] `imagePullPolicy: IfNotPresent` (or `Always` only where genuinely needed) and `imagePullSecrets` configured for private registries.
- [ ] Base image is a minimal/distroless JRE image; image scanning (Trivy/Grype) integrated into CI before promotion to prod overlay.

### 5.6 Observability
- [ ] Prometheus scrape annotations or a `ServiceMonitor`/`PodMonitor` (if using the Prometheus Operator) present, pointing at the Actuator `/actuator/prometheus` endpoint.
- [ ] Structured JSON logging configured so cluster log aggregation (Loki/ELK/CloudWatch) can parse fields — not plain text logs in prod.
- [ ] Pod labels include enough metadata (`app.kubernetes.io/version`, `app.kubernetes.io/component`) for tracing a running pod back to a specific build/commit.
```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: orders-service
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: orders-service
  endpoints:
    - port: metrics
      path: /actuator/prometheus
      interval: 30s
```

### 5.7 Namespace & RBAC Isolation
- [ ] Each environment lives in its own namespace (`orders-dev`, `orders-staging`, `orders-prod`), with `ResourceQuota` and `LimitRange` set per namespace to prevent noisy-neighbor resource exhaustion.
- [ ] Dedicated `ServiceAccount` per service (not `default`) with minimal RBAC — avoids over-privileged pods.

---

## 6. Quick Red Flags Checklist

- 🚩 Full manifest duplication per environment instead of Kustomize base + overlay.
- 🚩 Secrets committed in plaintext to the repository (even in an overlay directory).
- 🚩 `LoadBalancer` Service type used per-microservice without justification (unnecessary cloud cost).
- 🚩 No TLS on externally exposed Ingress/Service.
- 🚩 HPA configured without `resources.requests` set on the container — HPA percentage-based metrics have nothing to calculate against.
- 🚩 `maxReplicas` on the consumer Deployment exceeds the topic's partition count — wasted idle pods.
- 🚩 No `PodDisruptionBudget` — node drains/upgrades can take down all replicas simultaneously.
- 🚩 Liveness probe hitting a plain `/actuator/health` that includes downstream dependency checks — causes crash-loop when a dependency (not the app itself) is unhealthy.
- 🚩 Containers running as root with a writable root filesystem.
- 🚩 `:latest` image tag used in staging/prod overlays.
- 🚩 No `NetworkPolicy` — any pod in the cluster can reach internal-only services.
- 🚩 `terminationGracePeriodSeconds` too short for Kafka consumer group to leave cleanly, causing avoidable rebalances on every deploy.

---

## 7. Review Sign-off Template

```
Reviewer:         ____________________
Date:             ____________________
Service:          ____________________
Environment(s):   ____________________

[ ] Kustomize base/overlay structure correct, no env leakage into base
[ ] ConfigMap/Secret separation correct; secrets not in plaintext in repo
[ ] External exposure (Service/Ingress/LB) reviewed, TLS enforced
[ ] HPA configured with sane min/max, resource requests present
[ ] PodDisruptionBudget present and consistent with HPA
[ ] Probes (liveness/readiness/startup) correctly reflect app + Kafka health
[ ] Security context hardened (non-root, dropped capabilities)
[ ] Rolling update strategy safe for Kafka consumer rebalancing
[ ] Observability (metrics/logs) wired up
[ ] No red flags identified (or flagged below)

Notes / Follow-ups:
_______________________________________________
_______________________________________________
```
