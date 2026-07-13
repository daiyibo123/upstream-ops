# Changelog

All notable changes are documented here. Releases use semantic versioning: `vMAJOR.MINOR.PATCH`.

- `MAJOR`: incompatible changes.
- `MINOR`: backwards-compatible features.
- `PATCH`: backwards-compatible fixes and small improvements.

Every release must update this file, `backend/global/version.go`, and the Dockerfile version argument before its matching Git tag is pushed.

## v0.22.0 - 2026-07-13

### Added

- Added cheaper-channel-first gateway routing: schedulable candidates now prefer public/charity groups first and then lower ratios before slower or failed candidates.
- Reworked available channels into OpenAI, Claude, and Grok columns with a unified OR filter area for search, format, ratio band, and public-key status.
- Added manual upstream group-key creation with URL, group, ratio, client format, and upstream Key fields.
- Added channel/group/ratio/upstream-Key-aware Gateway Key creation and editing, including allowed group binding.
- Added Gateway Key money-based balance limits, today/total cost tracking, today/total token usage, and automatic disabling after the configured balance is exhausted.
- Added Gateway Key concurrency limits with queued waiting instead of immediate failure when the limit is reached.
- Added `GET /api/gateway/keys/:id/usage` for querying a single Gateway Key's today tokens, today cost, total tokens, and total cost.
- Added login brute-force protection: more than five failed attempts from one IP locks that IP for five minutes.
- Added daily automatic cleanup for usage records only; channels, keys, groups, configs, and other logs are preserved.

### Changed

- One-click health checks now support selected group IDs and batch parallelism, so large group sets are tested in concurrent batches instead of serially misclassifying later groups.
- Health checks now use OpenAI, Claude, and Grok-specific probe requests and classify zero balance, rate limit, forbidden, non-generation payloads, auth failures, timeouts, network errors, model errors, invalid requests, and server errors separately.
- One-click group-key bootstrap now skips channels, group names, and group descriptions containing `图`, `img`, or `ban`.
- Codex direct URL usage no longer depends on ccswitch routing: Responses requests can fall back to chat/completions, chat SSE can be converted back into Responses SSE, missing `response.completed` is synthesized, slow response headers are allowed for long reasoning streams, and non-SSE JSON stream replies are wrapped as Responses SSE.
- Public Gateway Key display now shows a masked key without password, reveals/copies the full key through the eye/copy actions, supports clearing the public-key password, and improves the password dialog on mobile.
- Dashboard and monitor token displays now use compact M/B formatting and the home preview layout was simplified.
- Notification delivery now captures provider response bodies for email/WeCom/Feishu failures to make recent failure causes visible and actionable.
- Usage-log retention defaults to one day to prevent record growth.

## v0.21.2 - 2026-07-12

### Fixed

- The in-app update action now returns the updater setup error to the interface instead of the unhelpful `HTTP 400` message.
- Documented the one-time Compose upgrade required to start the Watchtower updater sidecar.

## v0.21.1 - 2026-07-12

### Changed

- Improved dashboard, gateway scheduling, health-check batching, key management, and settings interactions.
