# Changelog

All notable changes are documented here. Releases use semantic versioning: `vMAJOR.MINOR.PATCH`.

- `MAJOR`: incompatible changes.
- `MINOR`: backwards-compatible features.
- `PATCH`: backwards-compatible fixes and small improvements.

Every release must update this file, `backend/global/version.go`, the Dockerfile version argument, and the frontend package version before its matching Git tag is pushed. Update any version-pinned README deployment command at the same time. The matching `vMAJOR.MINOR.PATCH` tag triggers the Docker build and GitHub Release workflow.

## v0.24.5 - 2026-07-15

### Fixed

- OpenAI health checks now probe `gpt-5.4` first and try `gpt-5.5` only when the first probe fails.
- Probes use the native Responses input-list shape with low reasoning effort, avoiding false deaths caused by rejected shorthand input.
- A successful 5.4 probe stops immediately; authentication, 403, rate-limit, and balance failures do not waste a second model probe.
- Compatibility model discovery remains available only after both primary probes fail, preserving support for relays that advertise other models.

## v0.24.4 - 2026-07-15

### Fixed

- Fixed manual OpenAI-compatible channels whose native Responses endpoint requires the official Codex input-list shape. Health checks and protocol detection now send a streamed `1+1=` request using `input` message/content blocks and `reasoning.effort=low` instead of the rejected shorthand string form.
- A manually selected request protocol is now tested exactly as configured. Health checking no longer reports a manual Responses channel alive merely because a hidden Chat fallback succeeded.
- Increased the tiny probe output allowance to 16 tokens so low-effort reasoning models can complete the math response without turning a healthy route into a false failure.

### Added

- Added a refresh icon to Usage Details. It reloads the current page of records without refreshing the whole browser page and shows a spinner while loading.

## v0.24.3 - 2026-07-15

### Fixed

- Successful usage records no longer use the misleading `estimated` status. Local token estimation remains an internal accounting detail while the request outcome is stored and displayed as `success` / “完成”; existing `estimated` rows are normalized during migration.
- Fixed automatic request-mode detection timing out before it could try Chat Completions or the alternate authentication header. Detection now has a realistic overall deadline and a bounded timeout per protocol attempt.
- Automatic protocol detection is serialized per upstream website, preventing several keys on the same relay from being probed simultaneously and triggering avoidable 5XX responses.
- Tightened health-check success recognition: an empty `response.completed` event or a Chat chunk containing only `finish_reason` no longer marks a channel alive. Actual text/reasoning generation is required, while completed Responses events containing real output remain supported.

## v0.24.2 - 2026-07-15

### Fixed

- Fixed Available Channels search after adding or renaming a channel. Searching now refreshes both channel management data and group-key data before filtering, then enriches every group with the channel's current name and URL.
- Channels that match the search but do not yet have a synchronized or manually bound group Key are now shown explicitly with an “尚无可用分组” state and an action to add a group Key, instead of incorrectly reporting that the channel cannot be found.

## v0.24.1 - 2026-07-15

### Added

- Added global and per-channel upstream response-content interception. A matching pre-output response is retried twice on the same upstream, then safely fails over to another channel without replaying content already delivered to the client.
- Added batch Gateway Key disabling with a custom Responses-compatible message that Codex can display directly on subsequent requests.

### Fixed

- Available-channel search now matches the current channel name, website/API URL, stored channel URL, group name, description and reference using fuzzy matching.
- Application branding is now sourced from System Settings. Hard-coded product branding was removed from the UI, backend defaults, generated upstream Key names, diagnostic headers and notification email content.

### Changed

- Redesigned notification email HTML with a clearer responsive layout, stronger contrast and application-title branding.
- Kept content interception inside the existing stream preflight window, so normal requests gain no extra buffering or first-token delay. Existing health-check batching, per-provider probing and sticky low-cost scheduling behavior remain covered by the full test suite.

## v0.24.0 - 2026-07-15

### Fixed

- Fixed the Responses-to-Chat compatibility bridge dropping tool calls. Chat-compatible upstreams now preserve function-call IDs, names and arguments in both streaming events and the final `response.completed` object, so Codex can execute edit, shell and other tools instead of receiving a text-only turn.
- Tool-only responses no longer receive a fabricated empty message before the function call. Tool and text outputs now retain their actual order and output indexes.
- Kept the same conversion for non-streaming responses and added regression coverage for streamed and non-streamed tool calls.

### Changed

- Usage details now show the upstream name above the effective ratio. Public gateway Key calls carry a compact公益标识, avoiding a separate crowded ratio-only column.
- Local usage and effective-ratio scheduling improvements from this release are included in the v0.24.0 release line; gateway and frontend production builds are verified before tagging.

## v0.23.1 - 2026-07-15

### Fixed

- Fixed false “no configured upstream supports requested model” errors: temporary 503/429, network, and provider-router responses that mention a model are no longer cached as a permanent model capability miss. Only explicit model-support/access rejections are cached.
- Clarified inconclusive health-check status as “待复测” and added it to Available Channels status filtering. This status remains schedulable and does not mean the channel is dead.
- Fixed overlapping controls in Available Channels. Format/request-method selects and priority/concurrency inputs now shrink and truncate safely on narrow layouts.

### Changed

- Added short-lived soft route affinity for requests without a conversation, response, or prompt-cache identity. It is scoped to the calling gateway Key, source IP, and model, keeping healthy channels stable to reduce reconnects and first-token variance while still failing over immediately on a real fault.

## v0.23.0 - 2026-07-14

### Fixed

- Fixed manual NewAPI/OpenAI-compatible upstream keys copied as `Bearer <key>`, `Authorization: Bearer <key>`, or `X-Api-Key: <key>`. They are normalized before storage and again before forwarding, so existing manual records no longer produce a duplicated `Bearer` prefix and `Invalid token` response.
- Added Codex-compatible default request identity for synthetic OpenAI health probes (`User-Agent: codex-cli` and `Originator: Codex CLI`) while preserving the exact headers from real inbound Codex clients.
- Hardened direct `/v1/responses` preflight failover. Lifecycle-only streams (`response.created` / `response.in_progress`) that end with EOF, a premature `[DONE]`, cancellation, incompleteness, or an error now move to the next healthy upstream before any downstream content is written.

### Changed

- Direct gateway forwarding remains single-request and cache-affine: once text, reasoning, or tool-call output has reached the client, it never replays the request on another upstream. The gateway instead sends exactly one protocol-compliant failure/cancellation terminal event if the active stream later breaks.
- Added regression coverage for legacy manual Key formats, Codex request-header preservation, lifecycle-only EOF failover, and Responses terminal-event integrity.

## v0.22.7 - 2026-07-14

### Fixed

- Restored per-key request-method configuration for manual channels. Administrators can keep automatic protocol detection or explicitly select Responses, Chat Completions, or Claude Messages; a manual correction is preserved during later synchronization and detection jobs.
- Fixed manual upstream keys that were usable upstream but became `403`, `Invalid token`, or unavailable after being added. Protocol and authentication capability are now re-detected per concrete key, including a safe Bearer / `X-Api-Key` fallback before any response bytes are sent.
- Manual health checks now retry the alternate authentication header for 401/403 results and keep an unresolved manual probe as `unknown` rather than cooling down a potentially healthy route. A real request still disables a genuinely invalid key only after both header contracts fail.
- Allowed multiple manual keys under the same visible upstream group. They retain independent secrets, request modes, authentication headers, health state, and scheduling records instead of overwriting one another.

### Changed

- Available Channels now shows the detected authentication-header contract for each upstream key, alongside its protocol.

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
