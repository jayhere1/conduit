# Security Policy

## Reporting a vulnerability

Please report security issues privately to the maintainer rather than opening a
public issue. Include a description, reproduction steps, and the affected
version or commit. You will get an acknowledgement within a few days.

Do not open public GitHub issues for undisclosed vulnerabilities.

## Scope

This document covers the Conduit API server (`conduit serve` /
`conduit-api`). The CLI-only workflows (`compile`, `run`, `plan`, `apply`)
operate on local files under the user's own account and are out of scope for
network threat modeling.

## Authentication & authorization model

Authentication is **opt-in** via `conduit serve --auth-enabled`. When enabled:

- **API keys** are the sole credential. A key is `cdt_` + 32 hex chars, shown
  once at creation. Only a salted SHA-256 hash and the 12-char public prefix
  are stored (`conduit-api/src/auth.rs`, `create_key`/`hash_key`). Keys are
  high-entropy random values, so a fast hash is appropriate — this is not
  password storage.
- **Three roles**, totally ordered: `Viewer < Operator < Admin`
  (`auth.rs`, `Role`). Reads require Viewer, mutations require Operator, API-key
  management requires Admin.
- **Enforcement is global.** The `auth_gate` middleware
  (`conduit-api/src/middleware.rs`) rejects anonymous requests on every route
  except the public allowlist (`/health`, `/info`, `/docs*`) and applies a
  coarse role gate *before* request bodies are parsed. Each mutating handler
  additionally checks a fine-grained `Permission` (defense in depth).
- **Constant-time key comparison.** `authenticate` pre-filters candidate keys
  by stored prefix, then compares the salted hash with `subtle::ConstantTimeEq`
  (`auth.rs`), so validation timing does not leak hash bytes.
- **Revocation** takes effect immediately — `authenticate` rejects revoked and
  expired keys on the next request (`auth.rs`, `is_valid`).

### When auth is disabled (the default)

`conduit serve` without `--auth-enabled` serves **all endpoints publicly**, by
design, for local/single-user development. Do not expose such an instance to an
untrusted network. The startup banner states which mode is active.

## Threat model & mitigations

| Threat | Mitigation | Evidence |
|---|---|---|
| Anonymous access to mutating endpoints | Global `auth_gate` middleware + per-handler permission checks | `middleware.rs` `auth_gate`; `tests/auth_enforcement_test.rs`; `tests/auth_redteam.rs::anonymous_cannot_mutate_anything` |
| Forged / malformed tokens | Bearer parsing + salted-hash validation; header-smuggling and injection strings rejected | `auth.rs` `extract_bearer`/`authenticate`; `tests/auth_redteam.rs::{forged_tokens_are_rejected,malformed_authorization_headers_are_rejected}` |
| Timing side-channel on key check | Constant-time comparison via `subtle` | `auth.rs` `authenticate`; PRD A2 |
| Revoked/expired key replay | Revocation + expiry checked on every request | `auth.rs` `is_valid`; `tests/auth_redteam.rs::{revoked_key_cannot_be_replayed,expired_key_is_rejected}` |
| Privilege escalation (Viewer/Operator → Admin) | Role gate on `/auth/keys*`; key management is Admin-only | `middleware.rs` `required_role_for`; `tests/auth_redteam.rs::{viewer_cannot_escalate_to_key_management,operator_cannot_manage_keys}` |
| Credential theft at rest | Only salted hashes + prefixes stored; plaintext shown once | `auth.rs` `create_key`/`hash_key` |
| Cross-origin browser attacks | Same-origin by default; explicit `--cors-origin` allow-list | `routes.rs` CORS block; `tests/cors_test.rs`; PRD A4 |
| Request flooding | Per-IP token-bucket rate limiting on API + WS | `conduit-api/src/rate_limit.rs` |
| Oversized request bodies | 10 MB body-size limit | `routes.rs` `RequestBodyLimitLayer` |
| Undetected auth abuse | Failed auths, role denials, and key create/revoke recorded as `AuthAudit` events | `middleware.rs`/`handlers/auth.rs` `audit_auth`; `tests/auth_audit_test.rs`; queryable via `GET /api/v1/events?event_type=authaudit` |

## Known limitations

- **WebSocket auth.** `/ws/events` is not gated by `auth_gate` (browsers cannot
  set an `Authorization` header on a WebSocket handshake). Do not stream
  sensitive events to untrusted networks; a query-token handshake is future
  work.
- **No per-key rate limiting.** Rate limiting is per-IP, not per-key; a valid
  key behind a shared IP shares the bucket.
- **SHA-256, not a password KDF.** Correct for high-entropy random keys; do not
  reuse this path for user-chosen passwords.
- **TLS termination is external.** The server speaks plain HTTP; run it behind a
  TLS-terminating proxy in production.

## Verifying the posture

```bash
cargo test -p conduit-api --test auth_redteam \
                          --test auth_enforcement_test \
                          --test auth_audit_test \
                          --test cors_test
```
