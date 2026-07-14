# Changelog

All notable changes are documented here. Releases use semantic versioning: `vMAJOR.MINOR.PATCH`.

- `MAJOR`: incompatible changes.
- `MINOR`: backwards-compatible features.
- `PATCH`: backwards-compatible fixes and small improvements.

Every release must update this file, `backend/global/version.go`, and the Dockerfile version argument before its matching Git tag is pushed.

## v0.22.6 - 2026-07-14

### Fixed

- Fixed System Settings persistence for the homepage cheapest-channel switch; saving now retains the selected state in `config.yaml`. Login session TTL updates are covered by a save/load regression test.
- Refined the public homepage lowest-price list: it now keeps only one lowest-ratio OpenAI group per website, ranks the five cheapest websites, displays the website domain and ratio only, and loops with stable ranking.
- Improved upstream health checks for large channel sets: probes are batched in groups of ten, serialized per API Base URL, retried only for transient failures, and avoid marking protocol/model-limited routes as dead prematurely.
- Improved direct Responses compatibility by retaining safe pre-output fallback and terminal-event protections while recording model capability per upstream.

### Changed

- Improved streaming first-token delivery: SSE upstream requests explicitly request identity encoding to prevent compression buffering, while connection pooling and heartbeats remain in place.
- For candidates with otherwise equal routing priority and ratio, the scheduler now prefers the lower observed time-to-first-token (TTFT), then uses total latency as a tie-breaker. This reduces perceived response delay without overriding charity, priority, ratio, model support, or session affinity.

## v0.22.5 - 2026-07-14

### Fixed

- Improved automatic upstream protocol detection. OpenAI-format groups now probe native Responses and Chat Completions with a real low-token stream; Claude uses its native Messages contract and `x-api-key`; Grok uses its native OpenAI-compatible Chat contract.
- When the economical `gpt-5.4` health probe and its fallback model are unavailable, the gateway now reads the upstream model list and probes one advertised text model. This prevents model-limited but healthy upstreams, including `gpt-5.6`-only relays, from being marked as failed merely because the default probe model is absent.
- Protocol detection now runs after manual key creation or replacement, channel-format changes, group synchronization, and within OpenAI batch health checks. Request protocol is no longer manually selectable.
- Corrected dashboard health statistics to include OpenAI Chat-Completions compatibility groups as OpenAI channels, and split the former combined `403 / 非生成` metric into independent 403-rejection and non-generation counts.
- Restored manual channel creation in Available Channels. A manually added channel’s API Keys dialog can now add a bound upstream Key directly, edit an existing Key, reset stale runtime failures, and re-detect the upstream protocol.

## v0.22.4 - 2026-07-14

### Fixed

- Hardened direct Codex `/v1/responses` streaming: upstream preflight now completes before any downstream event is emitted, allowing safe failover before the first token while guaranteeing a single protocol terminal event for a started stream.
- Refined upstream cooldown and recovery handling so short-lived 503, timeout, network, and routing failures do not immediately disable an entire group; compatible healthy candidates can be selected before a user-visible failure.
- Improved health probes to send a minimal streaming `1+1=` generation request with a two-token limit, verify real generated output instead of treating connection establishment as success, and fall back from `gpt-5.4` when an upstream does not expose that model.
- Fixed stale automatic group records: a successful group sync now reconciles the complete upstream snapshot, including an empty result, and removes groups deleted upstream.
- Automatic group creation now excludes groups whose name or description contains `图`, `image`, `img`, `im2`, or `ban`; excluded legacy automatic groups are removed during the same sync.
- Manual channels and `manual:` group keys are excluded from automatic synchronization so they cannot be queried, overwritten, or deleted by the one-click action.
- Improved Gateway Key expiry, exhausted-balance, IP-ban, and public-IP concurrency handling, returning readable Responses-compatible messages to Codex clients instead of bare protocol disconnects.

### Changed

- Added per-Gateway-Key upstream multiplier limits: unlimited, `<= 0.05`, or `<= 0.1`.
- Expanded available-channel filtering, paging, format/status display, manual-channel controls, source display, public-key configuration, usage IP/latency columns, and homepage cheapest-OpenAI presentation settings.
- Changed the group action label to “覆盖同步分组 Key” and now reports preserved/updated, created, removed, skipped, and failed counts.

## v0.22.3 - 2026-07-14

### Changed

- Reworked available-channel display into one compact row per upstream group, ordered by multiplier from low to high; removed the nested per-channel group cards and hidden verbose raw health errors from the channel list.
- Added unified OR-based filtering for fuzzy search, format, multiplier band, public-key state, and the six status choices: all, alive, dead, zero balance, rate limited, and 403 forbidden.
- One-click and per-group health checks now accept OpenAI-format groups only, use `gpt-5.5`, and enforce that restriction in the API as well as the interface.
- Group-key bootstrap now excludes image/blocked groups containing `图`, `img`, `im2`, or `ban`; newly inferred `cc`, `cs`, `kiro`, `max`, and Claude aliases default to Claude format.
- One-click group-key bootstrap is now a safe reconciliation: it updates existing upstream groups, removes upstream-deleted or newly excluded automatic groups, and preserves manually added `manual:` group keys.
- Added usage-record request IP display and IP abuse controls: globally ban/unban callers, exempt a specific IP from the public-key limit, and queue public-key traffic to a maximum of five concurrent requests per IP by default.
- Routing now recognizes `prompt_cache_key` as an affinity key so Codex/OpenAI requests in the same prompt-cache family stay on the same upstream and retain provider-side cache eligibility.

### Fixed

- Hardened direct Codex URL streaming by normalizing upstream `response.done` terminal events into `response.completed` plus `[DONE]`, matching strict Responses clients.
- Responses stream intent is now detected from body `stream: true`, `?stream=1/true`, and `Accept: text/event-stream`; hinted stream requests also forward `stream: true` upstream and return SSE terminal failures instead of bare HTTP 503 JSON.
- Public gateway keys that are expired or over balance now return Codex-readable streamed messages, avoiding protocol-level disconnect errors.

## v0.22.2 - 2026-07-13

### Fixed

- Fixed direct Codex `/v1/responses` streaming without ccswitch routing: OpenAI-compatible upstreams now select the Chat Completions compatibility bridge by protocol rather than unreliable client headers, while retaining native Responses fallback when Chat is unavailable.
- Hardened Responses stream compatibility for direct URL use: Chat SSE and non-SSE JSON replies are converted into complete Responses lifecycle events; upstream EOF, bare `[DONE]`, and malformed terminal streams are closed with `response.completed` plus `[DONE]` rather than a premature disconnect.
- Added failover for model-not-found, unsupported-model, and model-access-denied upstream responses, so a requested model such as `gpt-5.6` automatically proceeds to the next eligible channel.
- Restored one-click health checks to OpenAI-format enabled groups only, preventing Claude/Grok formats from being tested with an incompatible probe.

### Changed

- Preserved request affinity and upstream cache-related payload fields through gateway forwarding so repeated Codex conversations remain pinned to the same upstream where possible and do not lose provider-side prompt-cache eligibility.

## v0.22.1 - 2026-07-13

### Fixed

- Fixed Codex direct URL streaming without ccswitch routing by preferring the chat-completions bridge for `/v1/responses` streaming requests on OpenAI-compatible upstreams, even when the client User-Agent does not explicitly identify Codex.
- Converted Responses API tool declarations and chat `tool_calls` stream chunks into Codex-compatible Responses function-call events when using the chat-completions bridge.
- Normalized data-only Responses SSE chunks by adding the required `event: response.*` lines, so Codex can reliably observe `response.output_text.delta`, `response.completed`, and the final `[DONE]`.
- Hardened Responses SSE termination: upstream `[DONE]`, EOF, idle timeout, or mid-stream error now produce a valid Responses terminal event (`response.completed` or `response.failed`) instead of closing the stream early.

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
