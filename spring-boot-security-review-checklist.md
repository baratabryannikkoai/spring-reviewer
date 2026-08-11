# Spring Boot Security Review Checklist
### Spring Security + OAuth2/JWT · Secrets Management · Dependency/CVE Scanning · OWASP Top 10:2025

Use this checklist to review the security posture of a Java Spring Boot codebase. It assumes **Spring Security 6.x+** (the `WebSecurityConfigurerAdapter`-based approach was removed in Security 6.0 — flag any code still extending it as an immediate finding).

---

## 1. Spring Security Configuration Fundamentals

- [ ] **No `WebSecurityConfigurerAdapter`.** Security config is a `SecurityFilterChain` `@Bean` built with the `HttpSecurity` lambda DSL. Code still extending the adapter is running against a class removed in Security 6.0 and needs migrating.
- [ ] **Multiple filter chains are intentional and ordered**, not accidental. If the app has separate rules for public/internal/actuator endpoints, each has its own `SecurityFilterChain` bean with an explicit `@Order` and a precise `securityMatcher`, not one broad chain trying to do everything.
- [ ] **Default-deny posture.** The chain ends with `.anyRequest().authenticated()` (or `.denyAll()`), and public endpoints are allow-listed explicitly — not the reverse. A newly added endpoint should be unreachable by default until someone deliberately opens it up.
- [ ] **CSRF handling matches session model.** CSRF protection is enabled for browser/cookie-session based apps and can be safely disabled *only* for genuinely stateless, token-authenticated APIs (`SessionCreationPolicy.STATELESS`) — disabling CSRF without also going stateless is a common misconfiguration.
- [ ] **Actuator endpoints are not wide open.** `/actuator/health` may be public; `/actuator/env`, `/actuator/beans`, `/actuator/heapdump`, etc. must require authentication (or be unexposed entirely) — these routinely leak secrets and internals when left open.
- [ ] **Method-level security enabled deliberately** (`@EnableMethodSecurity`) with `@PreAuthorize`/`@PostAuthorize` used on service-layer methods that touch sensitive data — not relying on URL-pattern matching alone, which is easy to bypass via unexpected routing.
- [ ] **CORS configuration is explicit and scoped**, not `*` for `Access-Control-Allow-Origin` on any endpoint that accepts credentials — an open CORS policy combined with cookie-based auth is a direct account-takeover vector.

---

## 2. OAuth2 Resource Server & JWT

### 2.1 Token validation

- [ ] **Algorithm is pinned and asymmetric.** JWTs are signed with RS256 (or another asymmetric algorithm) and the resource server explicitly rejects `alg: none` and unexpected algorithms — never trust the algorithm declared in the token header without constraint.
- [ ] **Signature verification via JWKS**, using `issuer-uri` (preferred, enables automatic key rotation via the `.well-known` discovery + JWKS endpoint) or an explicit `jwk-set-uri`, not a hardcoded static public key that has no rotation path.
- [ ] **Issuer (`iss`) claim validated** against the expected authorization server.
- [ ] **Audience (`aud`) claim validated** against this service's own identifier. This is the single most commonly missed check — without it, a token legitimately issued for a *different* service in your ecosystem will be accepted here too. Add a custom `OAuth2TokenValidator<Jwt>` for audience if Spring Security's defaults don't cover it for your provider.
- [ ] **Expiration (`exp`) and not-before (`nbf`) enforced** — default behavior in Spring's `JwtDecoder`, but confirm no custom decoder configuration has disabled or loosened it.
- [ ] **Custom claim validators composed via `DelegatingOAuth2TokenValidator`**, not by re-implementing the whole decoder from scratch — extend the defaults rather than replacing them.

```java
@Bean
JwtDecoder jwtDecoder(OAuth2ResourceServerProperties properties) {
    NimbusJwtDecoder decoder = NimbusJwtDecoder
        .withJwkSetUri(properties.getJwt().getJwkSetUri())
        .build();

    OAuth2TokenValidator<Jwt> withIssuer = JwtValidators.createDefaultWithIssuer(properties.getJwt().getIssuerUri());
    OAuth2TokenValidator<Jwt> withAudience = new JwtClaimValidator<List<String>>(
        "aud", aud -> aud != null && aud.contains("orders-service"));

    decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(withIssuer, withAudience));
    return decoder;
}
```

### 2.2 Authorization

- [ ] **Scope/role mapping is explicit.** A custom `JwtAuthenticationConverter` maps token claims (`scope`, `roles`, or a provider-specific claim like Keycloak's `realm_access.roles`) into Spring `GrantedAuthority` objects with a clear, documented prefix convention (e.g., `SCOPE_` vs `ROLE_`) — mismatched prefixes are a frequent source of "authorization silently does nothing" bugs.
- [ ] **`@PreAuthorize` expressions match the mapped authority prefix** used by the converter above; a code reviewer should be able to trace a claim in the token to the exact authority checked in the annotation.
- [ ] **No trust in client-supplied identity outside the token.** User ID, tenant ID, and role must come from the validated JWT claims, never from a request header/body field the caller could set arbitrarily (e.g., a spoofable `X-User-Id` header).

### 2.3 Token lifecycle

- [ ] **Short-lived access tokens** (minutes, not days) paired with refresh tokens where a full OAuth2 flow is in use — long-lived JWTs can't be revoked before they expire.
- [ ] **Revocation/logout story exists** for scenarios that need it (e.g., a token-introspection call to the authorization server for high-sensitivity operations, or a denylist for compromised tokens) since self-contained JWTs can't be revoked by the resource server alone.
- [ ] **Refresh tokens are not accepted at resource-server endpoints** — only access tokens should be valid there; mixing token types is a common flaw.
- [ ] **Tokens are never logged**, including in error/debug logs, request tracing spans, or exception stack traces.

### 2.4 Modern authorization-server setup (if you issue tokens)

- [ ] **Spring Authorization Server** (the current first-party project) or a maintained external IdP (Keycloak, Auth0, Okta) is used to issue tokens — not a hand-rolled `/login` endpoint minting JWTs with a symmetric secret, which is harder to secure and doesn't support standard rotation/discovery.
- [ ] If using Keycloak: confirm the integration uses Spring Security's built-in OAuth2 Resource Server support, **not** the deprecated `keycloak-spring-boot-starter`/`KeycloakWebSecurityConfigurerAdapter`, which were removed from Keycloak 20+.

---

## 3. Secrets Management

- [ ] **No secrets in source control.** API keys, DB credentials, signing keys, and third-party tokens are absent from `application.yml`/`.properties`, code, and commit history — check history, not just the current tree, since a secret committed once and later removed is still compromised.
- [ ] **Secrets sourced from a dedicated secrets manager** in every real environment — Vault, AWS Secrets Manager, GCP Secret Manager, or Azure Key Vault — not from plain environment variables baked into a container image or CI config, which are easier to leak via logs, process listings, or image layers.
- [ ] **Local/dev secrets are clearly separated from prod**, using placeholder or dummy values in checked-in dev config, with real values injected at runtime only in deployed environments.
- [ ] **Secrets are injected at runtime, not build time.** A secret baked into a Docker image layer persists in that layer forever, even if a later layer "removes" it, and is extractable by anyone with image access.
- [ ] **Rotation is possible without a code deploy.** Verify the app picks up rotated secrets (DB password, signing keys, API keys) via config refresh or restart-on-secret-change, not by requiring a code change to point at a new value.
- [ ] **Different secrets per environment.** Test/staging/prod never share an API key, DB credential, or signing key — a staging leak should not compromise production.
- [ ] **Access to the secrets store is itself access-controlled and audited** — not every service identity should be able to read every secret; scope IAM/Vault policies per service.
- [ ] **`.env` files, if used locally, are gitignored** and never committed, including accidentally via a squashed merge or force-push.

---

## 4. Dependency & CVE Scanning

- [ ] **Automated dependency scanning runs in CI on every build/PR**, not just periodically or manually — OWASP Dependency-Check, Snyk, or GitHub's built-in Dependabot/code-scanning are all viable; pick one and confirm it's wired into the actual pipeline, not just installed and ignored.
- [ ] **Build fails (or is flagged) on new critical/high CVEs** in direct or transitive dependencies — a scanner that only reports without gating a merge tends to get ignored in practice.
- [ ] **Transitive dependencies are covered**, not just direct ones declared in `pom.xml`/`build.gradle` — most real-world CVEs show up several levels deep in the dependency graph.
- [ ] **An SBOM (Software Bill of Materials) is generated** per build/release (CycloneDX or SPDX format via a Maven/Gradle plugin) — this is now expected practice given the OWASP Top 10:2025's new "Software Supply Chain Failures" category, and is often a compliance requirement for enterprise/regulated customers.
- [ ] **Dependency updates are automated and routine** (Dependabot/Renovate opening PRs for outdated dependencies) rather than relying on someone remembering to check — stale dependencies accumulate unpatched CVEs quietly.
- [ ] **Base container images are scanned too**, not just application dependencies — an outdated JDK base image or OS packages in the container are just as exploitable as a vulnerable Java library.
- [ ] **Plugin/build-tool supply chain is considered**, not only runtime dependencies — a compromised Maven plugin or npm-based frontend build tool executing during CI is itself a supply-chain risk.
- [ ] **A documented process exists for handling an unpatched/unfixable CVE** (e.g., a transitive dependency with no upstream fix yet) — accept-with-expiry-date, pin a patched fork, or isolate the affected code path — rather than leaving it flagged and ignored indefinitely.

---

## 5. OWASP Top 10:2025 — Applied to Spring Boot

The list changed meaningfully from 2021: two new categories (**Software Supply Chain Failures**, **Mishandling of Exceptional Conditions**), SSRF folded into Broken Access Control, and Security Misconfiguration jumped from #5 to #2. Review against the current ranking:

| # | Category | What to check in a Spring Boot app |
|---|---|---|
| **A01** | Broken Access Control *(now includes SSRF)* | `@PreAuthorize` coverage on sensitive endpoints/methods; no IDOR (e.g., `/orders/{id}` without an ownership check); outbound HTTP calls built from user-controlled URLs are validated/allow-listed against internal-network targets. |
| **A02** | Security Misconfiguration | Default-deny filter chain (Section 1); actuator endpoints locked down; verbose error responses/stack traces disabled in prod (`server.error.include-stacktrace=never`); unnecessary Spring Boot auto-configured endpoints not exposed; security headers set (`Content-Security-Policy`, `X-Content-Type-Options`, `Strict-Transport-Security` via `HttpSecurity.headers(...)`). |
| **A03** | Software Supply Chain Failures *(new)* | Section 4 in full: CI-gated scanning, SBOM generation, pinned/verified plugin versions, no unvetted third-party artifacts pulled from unofficial repositories. |
| **A04** | Cryptographic Failures | TLS enforced end-to-end (including internal service-to-service calls, not just the edge); no custom/home-grown crypto; passwords hashed with a modern algorithm (`BCryptPasswordEncoder`/Argon2, not MD5/SHA1); sensitive data encrypted at rest where required. |
| **A05** | Injection | Parameterized queries via JPA/`NamedParameterJdbcTemplate` everywhere — no string-concatenated SQL; input validation on all externally supplied values (`@Valid` + Bean Validation); no unsandboxed use of `ProcessBuilder`/shell invocation with user input. |
| **A06** | Insecure Design | Threat modeling done for sensitive flows (payments, auth, admin actions) before implementation, not retrofitted; rate limiting/abuse controls designed in for public-facing endpoints, not bolted on after an incident. |
| **A07** | Authentication Failures | Section 2 in full, plus: account lockout/backoff on repeated failed logins; MFA available for privileged accounts; no default/hardcoded credentials anywhere in the codebase or container images. |
| **A08** | Software or Data Integrity Failures | CI/CD pipeline artifacts are signed/verified before deploy; dependencies pulled only from trusted, pinned registries (not floating `latest` tags); deserialization of untrusted data uses safe, allow-listed types rather than unrestricted Java deserialization. |
| **A09** | Security Logging and Alerting Failures | Authentication failures, authorization denials, and sensitive actions (payment, admin, data export) are logged; logs feed into active alerting — not just passive storage nobody reviews; logs never contain tokens, passwords, or full card numbers (ties into A04). |
| **A10** | Mishandling of Exceptional Conditions *(new)* | Error handling never fails open — an exception during an access-control check must deny, not default to allow; global exception handlers (`@ControllerAdvice`) return generic error bodies to clients while logging full detail server-side; resource exhaustion (large payloads, deeply nested JSON, unbounded loops) is guarded against, not just happy-path exceptions. |

---

## 6. Summary — Highest-Priority Findings to Flag First

If time is limited, prioritize in this order — these map to where most real-world Spring Boot security incidents actually originate:

1. Missing or incorrect **audience validation** on JWTs (Section 2.1)
2. **Secrets in source control** or baked into container images (Section 3)
3. **Open actuator endpoints** or other default-deny violations (Sections 1, 5-A02)
4. **No CI-gated dependency scanning** — unpatched known CVEs shipping to prod (Section 4, 5-A03)
5. **Missing `@PreAuthorize`/ownership checks** enabling IDOR (Section 1, 5-A01)
6. **Fail-open error handling** on access-control or payment logic (Section 5-A10)
