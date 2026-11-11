# Issue #119 Phase 1: synthesized design (contract for the implementer)

Inputs: design-A.md (tracer owns everything), design-B.md (pure ledger module). Grounding:
explore-request-path.md, explore-stream-and-trace.md, issue-119.md. All in this `.audit/` dir.

Goal: measurement only. No change to what clients receive, to retry/refresh/routing behavior, or to
the meaning of existing trace columns. One bounded per-request summary, persisted in traces.db,
surfaced on `GET /api/admin/traces`, proven through the real axum router for all four APIs.

## Synthesis decision

| Concern | Taken from | Decision |
| --- | --- | --- |
| Value types | B | New pure module `src/common/timing.rs`: `Phase`, `PhaseMarks`, `Span`/`SpanStat`, `Scale`, `Histogram`, `Chunk`, `EmitCounts`, `EmitKind`, `OutputTokens`, `Endpoint`, `Ledger`, `Summary` (+ `derived()` rates). No tokio/axum/reqwest/rusqlite. Inputs are `Micros` offsets so it is unit-testable without a clock. B lines 127-243 are the sketch. Drop `HttpVersion` (B never defined it; transport info is Phase 5). |
| Who owns live state | A | `RequestTracer` owns `parking_lot::Mutex<Ledger>` plus the origin `Instant`. NOT an `Arc<Ledger>` on `KeyContext` (B): compaction and websearch create more than one tracer per client request, and a shared mutable ledger would merge them. One writer chain per tracer. |
| Request origin | A | `KeyContext` gains `received_at: Instant` (stamped first thing in `auth_middleware`) and `endpoint: Endpoint`. Both immutable, `Copy`. Every `KeyContext` literal in tests gets `received_at: Instant::now()` via a constructor/helper, not by hand in 40 places if avoidable. |
| Endpoint label | neither | Set explicitly, not by URI string matching. Preferred: route-level so the middleware knows it (e.g. a per-route `Extension(Endpoint::Chat)` layer the middleware reads, or separate middleware wrappers). If that is awkward, each of the 4 entry handlers sets `key_ctx.endpoint` before forwarding. Verify `/cc/v1` nesting is labeled `ClaudeCode`. |
| Tracer start | A+B | Tracer construction sites stay where they are (no "one tracer at entry" restructure, no new Drop rule). `RequestTracer::new` takes the origin from `key_ctx.received_at`; handler marks `Phase::Prepared` at construction (conversion/serde/token-count/cache-metering done) and `Phase::BodyParsed` when available. Legacy `started_at`, `duration_ms`, `first_token_ms`, `mark_first_content`, `mark_first_token` keep their exact current meaning and code paths (history and UI thresholds stay comparable). |
| Provider phases | A+B | `TraceSink` gains a default no-op `on_span(Span, Duration)` and `on_upstream_call()` (headers received). `call_api_with_retry` reports: CredentialSelect, ProfileResolve, HeadersWait, ErrorBodyRead, Backoff, and the 403 force refresh as TokenRefresh. Refresh lock wait and refresh time are returned as values in `CallContext` (new field, e.g. `refresh: RefreshTiming { lock_wait, refresh }`) measured in `try_ensure_token`; no lock or await moves, no sink threaded into token_manager. |
| Body timing | A | Body-only wrapper (`TimedBody` or equivalent) around the upstream bytes stream in each decode loop: first body byte, receive gap (upstream wait), downstream wait (last yield -> next poll), bytes. Frames-per-chunk counted at the existing `decode_iter` site. Do NOT do B's `UpstreamEvents` refactor of the three decode loops (separate refactor PR later). Must be cancel-safe/Unpin inside the ping `select!`; must not change chunk delivery. |
| Emit timing | A+B | One `StreamSettlement::emit` (or equivalent single helper) records first/last Messages-level thinking, text, tool emit and emitted-content gaps. Pings excluded from content gaps. Structured-output hold: emit time is when it is actually yielded. |
| Adapter-level first yield (Chat/Responses) | A | Deferred to a follow-up. Trace row finalizes on the inner Messages stream before the adapter ends. Document in Summary as not measured; add a guard test that Chat/Responses still forward first text promptly. |
| Output-token provenance | A+B | `OutputTokens::{Exact, Estimated, Unavailable}` stored in new column `output_tokens_source` (`exact`/`estimated`/`unavailable`), independent of `usage_source`. Exact only when `metadataEvent.tokenUsage` arrived for every upstream call of the request. Exact-but-zero while content was emitted -> `unavailable` (missing field defaults to 0). |
| TPS | A | output_tokens / generation window; window = first content -> last content (any kind), summed per upstream call (web-search rounds do not count the pause between them). Excludes pre-content wait and post-content usage/trailer wait. Reported only with >= 2 content events and window >= 250 ms, always with its source. Text and thinking bytes/sec separately. Derived on read, never stored. |
| Persistence | B | Three nullable columns on `traces`: `endpoint`, `output_tokens_source`, `timing` (versioned JSON `Summary`, `v: 1`, bounded, no Vec/String). Added via `migrate` list and SCHEMA consistently with existing pattern. No trace_attempts change (spans are aggregated in Summary). Admin traces JSON gains `endpoint`, `outputTokensSource`, `timing` (parsed object) and `throughput` (derived). Old rows return nulls. |
| Path coverage | A table | stream, non-stream, websearch loop (provider spans, first byte, cadence; content marks only if cheap), compaction (each tracer timed from original receipt), provider failure, interrupted/abandoned. Pure web_search and early 4xx/5xx: record nothing (unchanged). Fix nothing else in passing; websearch `mark_first_token` per-chunk inconsistency is noted, not changed. |
| Privacy/bounds | issue | No prompts, bodies, credentials, no per-event writes, no unbounded arrays, no metric labels. One extra JSON column in the existing single insert. |

## Commit sequence (each commit builds and passes the suite)

1. `test(gateway): measure first content and cadence through the router for all four APIs` — ignored bench in gateway_tests.rs via `create_router_with_shared_provider`, real socket, all four APIs incl. Responses, stream; fixture with N text deltas at fixed spacing. Prints p50/p95 first text, total, emitted gap max. Run it BEFORE commit 2 and save numbers.
2. `feat(timing): add pure per-request timing ledger` — `src/common/timing.rs` + unit tests (histogram buckets, quantile bound, first-wins marks, span accumulation, web-search call fold, TPS rules, token source rules, Summary JSON round trip, every Phase has a summary field).
3. `feat(trace): persist timing summary and token source` — columns, migration (test from a frozen v0.9.8 schema incl. migrate twice), insert/query round trip, admin JSON.
4. `feat(timing): record phases and cadence for every inference request` — producers. Router tests: 4 APIs x stream/non-stream assert monotonic phase order and populated fields; thinking vs text separated; gated upstream gap counted as upstream wait; slow reader counted as downstream wait; estimated vs exact token source; upstream 5xx with spans; interrupted stream still writes timing.
5. `docs: document request timing summary` — section in docs/gateway-performance.md: field definitions, TPS definition, limitations (adapter-level yield deferred, live auth/platform not covered), before/after bench table + overhead.

## Overhead budget

Rerun commit-1 bench at commit 4. First-text p50 delta <= max(0.3 ms, 2%) vs commit 1, same machine, alternating runs. Report the number either way.
