# P1 Plan: TLS/HTTPS Enablement for Production Readiness

## 1. Objective

Add TLS/HTTPS support to the load balancer and provide a safe migration path from HTTP-only local development to production-grade encrypted traffic.

Primary goals:

- encrypt client-to-load-balancer traffic
- provide secure deployment baseline for real environments
- preserve local dev ergonomics without forcing certificates

## 2. Why This Is P1

- HTTPS is effectively mandatory in production.
- It protects traffic confidentiality/integrity and reduces operational risk.
- It significantly improves the project’s real-world credibility and showcase value.

## 3. Scope

In scope:

- HTTPS listener support in load balancer.
- Configurable TLS cert/key paths.
- Optional HTTP-to-HTTPS redirect mode.
- Security-focused defaults and startup validation.
- Documentation and sample TLS config.

Out of scope (this phase):

- Full mTLS on gossip plane.
- Automatic certificate issuance/rotation (ACME integration).
- Advanced WAF/rate-limit/security gateway features.

## 4. Target Behavior

Runtime modes:

1. HTTP mode (current default for local dev).
2. HTTPS mode (cert + key required).
3. Optional dual-listener:
   - HTTPS serves traffic
   - HTTP listener redirects to HTTPS (301/308).

Validation rules:

- If TLS mode is enabled and cert/key is missing, fail fast at startup.
- Log startup mode clearly (`http` vs `https`).

Security baseline:

- Enforce modern TLS versions (minimum TLS 1.2 or 1.3 target).
- Disable insecure defaults where configurable.

## 5. Implementation Plan

1. Extend `types.LBConfig` with TLS fields:
   - `TLSEnabled`
   - `TLSCertFile`
   - `TLSKeyFile`
   - `HTTPRedirectToHTTPS`
   - optional `HTTPRedirectAddr`
2. Add config-file mapping support for these fields in TOML loader.
3. Update load balancer startup flow:
   - `ListenAndServeTLS` when TLS enabled
   - keep existing `ListenAndServe` path for HTTP mode
4. Add redirect handler (if enabled) for plain HTTP listener.
5. Update scripts/docs with local self-signed cert workflow.
6. Keep backward compatibility: existing HTTP startup continues unchanged.

## 6. Testing Plan

Unit/behavior tests:

- TLS config validation fails for missing cert/key.
- HTTPS server boots with valid cert/key.
- Redirect mode returns expected redirect status + location.
- HTTP-only mode remains unchanged.

Integration checks:

- Generate self-signed cert for local test.
- Verify:
  - `curl https://...` succeeds with expected JSON
  - `curl http://...` redirects when redirect mode is enabled

Regression:

- Existing routing/failover behaviors remain intact under HTTPS mode.

## 7. Acceptance Criteria

- LB supports HTTPS startup via config/flags.
- Misconfigured TLS fails fast with clear error messages.
- Optional HTTP redirect works as documented.
- Existing test suite stays green; new TLS tests pass.

## 8. Risks and Mitigations

Risk: certificate management complexity for users  
Mitigation: include a simple local self-signed workflow + production notes.

Risk: accidental mixed-mode insecure deployment  
Mitigation: explicit mode logs and recommended secure defaults.

Risk: handshake/perf overhead concerns  
Mitigation: keep-alive and modern TLS defaults; measure via existing stress/failover tools.
