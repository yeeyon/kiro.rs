# Gateway fixes and response latency

Measured locally on October 2, 2026. Baseline: commit `be0c042`. The baseline
and changed code used the same debug build profile and local HTTP fixture;
neither run called Kiro or used real credentials.

## Before and after

The upstream fixture sends assistant text after 20 ms, then waits another
200 ms before sending final usage and closing the response. Each endpoint
receives ten sequential streaming requests through the actual gateway handlers,
provider, HTTP client, and AWS event-stream decoder.

First-content timing includes request conversion and upstream dispatch. It
ignores message-start, role, metadata, and keepalive events. This fixture does
not include incoming HTTP routing, authentication middleware, TLS, or a reverse
proxy. P50 is the upper median of ten observations; P95 is the slowest observation.

| Endpoint | First content P50 before | First content P50 after | First content P95 before | First content P95 after | Total completion P50 before / after |
| --- | ---: | ---: | ---: | ---: | ---: |
| `/cc/v1/messages` | 235.42 ms | 31.53 ms | 247.47 ms | 39.41 ms | 235.45 / 234.55 ms |
| `/v1/chat/completions` | 234.65 ms | 31.17 ms | 246.99 ms | 39.37 ms | 234.65 / 234.58 ms |
| `/v1/messages` | 30.71 ms | 31.33 ms | 31.83 ms | 40.64 ms | 235.47 / 234.96 ms |

CC and Chat first-content latency fell approximately 87% in this fixture.
Completion time stays effectively unchanged: these fixes remove gateway
buffering; they do not accelerate upstream model generation. A slower upstream
will still produce a slower answer. This small controlled run demonstrates the
regression fix; it is not a production load or throughput benchmark.

For every endpoint, ten requests used **ten upstream TCP connections before**
and **one after**. Removing the forced `Connection: close` header allows the
existing reusable HTTP clients to pool connections. Actual savings over HTTPS
depend on upstream keepalive and network conditions.

## Changes

- CC uses the existing live Messages stream instead of collecting the entire
  answer. Initial token usage is estimated; final `message_delta` reports the
  provider usage when available.
- Chat Completions translates incoming Messages events immediately, including
  fragmented tool arguments and final usage. A failed stream emits an error
  without a successful finish or `[DONE]` marker.
- Streaming responses include `X-Accel-Buffering: no` for proxies that honor it.
- Exact upstream `MODEL_TEMPORARILY_UNAVAILABLE` server errors return **503**
  with `Retry-After`, instead of making three internal attempts and returning
  a generic 502. A short per-model cooldown defaults to five seconds and honors
  a valid upstream `Retry-After`. Other models remain available. This change
  exposes upstream overload promptly; it does not repair upstream availability.
- Connection and upstream read deadlines are separate from the existing
  720-second total request budget. Active reads reset the read deadline.
  A timeout while awaiting headers or reading an error response body returns
  **504** without repeating the wait. A timeout after streaming starts emits
  an SSE error, because the HTTP status has already been sent.
- Non-streaming responses decode frames incrementally, fixing the empty-success
  bug when the complete response exceeded the decoder's 16 MB buffer.
- Empty bodies, invalid frames, truncated frames, malformed event payloads,
  upstream error events, and disconnects now produce explicit failures instead
  of normal completion. Full error cause chains are retained in request traces.
- Trace first-token timing starts with emitted text, thinking, or a tool call,
  rather than counting an upstream metadata chunk as content.

## Configuration

Existing configuration files receive these defaults automatically:

```json
{
  "upstreamConnectTimeoutSecs": 15,
  "upstreamReadTimeoutSecs": 120,
  "upstreamTotalTimeoutSecs": 720
}
```

The read setting limits upstream silence while awaiting headers or the next
body read. It does not limit an active answer to 120 seconds. Zero connect/read
settings are clamped to one second. Runtime credentials and configuration files
were not changed.

`upstreamTotalTimeoutSecs` is the deadline for a whole request, body included.
An active stream cannot reset it, so it also caps how long an answer may be: a
request still delivering tokens when it expires dies with a half-written body,
and every token already generated is paid for and lost. It defaults to 0 (disabled),
the value previously hardcoded in the API client, so behaviour is unchanged
unless you set it.

Raise it if long agentic turns are being truncated at twelve minutes, or set it
to 0 to remove the ceiling entirely — a stalled upstream is still caught by
`upstreamReadTimeoutSecs`, which measures silence rather than total work.

## TDD and verification

The initial six regression tests failed on the baseline: buffered CC, buffered
Chat, hidden CC disconnect, forced connection closure, generic overload mapping
with repeated attempts, and an empty response above the decoder buffer limit.
Additional failing tests reproduced damaged/empty streams, lost `Retry-After`
headers, stalled headers/body reads, and metadata being recorded as a first token.
The fixes make these tests pass. Coverage also verifies complete tool arguments,
final usage, and an active stream surviving past its idle timeout.

Final validation: **768 tests passed, zero failed**, with the latency measurement
ignored in the normal suite and run separately afterward. The production
compilation check also passed. This adds 18 regression/configuration tests to
the original 750-test suite, plus the opt-in latency measurement.

Run the full suite and production compilation check:

```powershell
cargo test --locked --offline
cargo check --locked --offline
```

Run the deterministic gateway regressions or repeat the latency measurement:

```powershell
cargo test --locked --offline anthropic::gateway_tests
cargo test --locked --offline gateway_latency_measurement -- --ignored --nocapture
```

Captured local evidence is in `target/gateway-before.txt`,
`target/gateway-after-final.txt`, `target/gateway-final-tests.txt`, and the
`target/gateway-*-red.txt` files. These build-directory logs are not committed.

## Request timing summary

Each traced inference request stores one timing summary in `traces.db`. It
answers where the time before the first response went and how evenly the
stream arrived. It records measurements only: clients receive the same bytes,
and retries, token refresh and routing are unchanged.

The summary covers `/v1/messages`, `/cc/v1/messages`, `/v1/chat/completions`
and `/v1/responses`, stream and non-stream. It stores no prompt, body or
credential, and its size is fixed.

### Where to read it

`GET /api/admin/traces` returns four more fields per record. Records written
before this version return `null` for all four.

| Field | Content |
| --- | --- |
| `endpoint` | `messages`, `cc`, `chat`, `responses`, or `other` when a handler was called without its route |
| `outputTokensSource` | `exact`, `estimated` or `unavailable` |
| `timing` | the stored summary, `v: 1` |
| `throughput` | rates and gap quantiles computed from `timing` when the record is read |

In SQL the summary is the JSON column `timing`, for example
`json_extract(timing, '$.phases.firstEmitTextUs')`.

All times are microseconds. Every offset counts from the moment the auth
middleware first saw the request, before the body was read. The existing
`durationMs` and `firstTokenMs` keep their meaning: they count from the later
point where the request tracer is created.

### `timing.phases`

Each phase is recorded once. `null` means it did not happen.

| Field | Moment |
| --- | --- |
| `bodyParsedUs` | The inference handler took over: auth, body read and JSON parsing are done. For Chat and Responses this includes the translation to a Messages request. |
| `preparedUs` | Conversion to the Kiro request, serialization, token counting and cache metering are done. |
| `upstreamHeadersUs` | The first upstream call returned a 2xx response head. |
| `firstBodyBytesUs` | First non-empty upstream body chunk. |
| `firstDecodedContentUs` | First upstream event with thinking, text or tool data, before any buffering in the gateway. |
| `firstEmitThinkingUs`, `firstEmitTextUs`, `firstEmitToolUs` | First Messages event of that kind handed to the response body. Stream requests only. |

`firstEmitTextUs - firstDecodedContentUs` is the time the gateway held the
content. With `output_config.format` it is the whole answer, because the
stream is rewritten at the end.

### `timing.spans`

Steps of the provider call that can repeat (retries, web-search rounds). Each
has `totalUs`, `count` and `maxUs`.

| Field | Step |
| --- | --- |
| `credentialSelect` | Choosing a credential, without the two refresh steps. A failed selection reports its whole wait here. |
| `refreshLockWait` | Waiting for the token refresh lock. |
| `tokenRefresh` | Refreshing the token while holding the lock, and the forced refresh after a 403. |
| `profileResolve` | Resolving the profile ARN. |
| `headersWait` | Request written to response head received, once per attempt. |
| `errorBodyRead` | Reading the body of a failed attempt. |
| `backoff` | Sleeping between retries. |

### `timing.stream`

Present when an upstream call returned a response head.

| Field | Content |
| --- | --- |
| `upstreamCalls`, `chunks`, `bytes` | Upstream calls, body chunks and body bytes. |
| `upstreamWaitUs` | Time spent waiting for upstream body chunks. |
| `downstreamWait` | Time between handing a batch to the response body and asking the upstream for more. This is the response writer and the client. Zero for non-stream requests. |
| `receiveGaps` | Histogram of the time between two upstream chunks of one call. |
| `emitGaps` | Histogram of the time between two content batches handed to the response body. Keep-alive pings are not content. |
| `framesPerChunk` | Histogram of frames decoded per upstream chunk. |
| `contentEmits` | Content batches handed to the response body. |
| `lastContentUs`, `finalUsageUs`, `upstreamEosUs` | Last decoded content, last `metadataEvent.tokenUsage`, clean end of the last upstream body. |
| `generation`, `thinking`, `text`, `tool` | `windowUs` (first to last decoded event), `events` and `bytes`, for all content and per kind. |

A histogram has `counts` (12 buckets), `n`, `sum` and `max`. A value goes into
the first bucket whose upper bound is not below it; the last bucket is open.
Gap bounds in ms: 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500. Frame
bounds: 0, 1, 2, 3, 4, 6, 8, 12, 16, 32, 64.

Delivery is pull-based, so a long receive gap can come from either side. Read
it with the two sums: a large `upstreamWaitUs` share points at the upstream or
the network to it, and a large `downstreamWait` points at the client side.

A request with several upstream calls (the web-search loop) adds each call's
windows together. The pause between two calls is not a gap and not generation
time.

### Output tokens and throughput

`outputTokensSource` says where `outputTokens` came from. It is separate from
`usageSource`, which describes the input and cache split.

| Value | Meaning |
| --- | --- |
| `exact` | Every upstream call of the request sent `metadataEvent.tokenUsage`. |
| `estimated` | Counted locally from the content. |
| `unavailable` | The request failed or was cancelled before usage was known, or the upstream reported zero although content arrived. |

`throughput.outputTps` is `outputTokens` divided by `generation.windowUs`. The
window runs from the first to the last decoded content of each upstream call.
It excludes the wait before the first content and the wait for usage after the
last content. The value is `null` unless the window is at least 250 ms and has
two or more content events, and `throughput.outputTokensSource` always names
the token source next to it. The token count covers thinking, text and tool
arguments together, and the tokens of the first event were generated before
the window opens, so short answers read slightly high.

`textBytesPerSec` and `thinkingBytesPerSec` use each kind's own window and the
same two conditions. `emitGapP50Us`, `emitGapP95Us`, `receiveGapP50Us` and
`receiveGapP95Us` are the upper bound of the histogram bucket that holds the
quantile, not exact values.

### Limitations

- Emit marks are taken where the Messages stream hands a batch to the response
  body. Chat and Responses translate that stream afterwards, and their own
  first event is not timed. Chat drops thinking, so `firstEmitThinkingUs` on a
  `chat` record is content the client never received.
- Handed to the response body is not delivered. The client may read later.
- Content kinds are counted at decode time. A `<thinking>` tag inside
  assistant text counts as text there.
- The mixed web-search loop records provider spans, upstream cadence and
  decoded content, but no emit marks, and its tracer is created before
  conversion, so `preparedUs` is early. Its `firstTokenMs` still means first
  upstream chunk.
- Remote compaction (Responses) creates its tracer after conversion, so
  `bodyParsedUs` and `preparedUs` are close together.
- A pure web-search request, and a request rejected before any provider call,
  still writes no trace record.
- The upstream fixture uses API-key credentials, so the tests do not exercise
  the refresh spans. Live authentication and platform behaviour are not
  covered by any test here.
- Connection reuse is not recorded.

### Measured overhead

`gateway_router_cadence_measurement` sends 200 streaming requests per API
through the production router on a real socket. The upstream fixture sends 8
text deltas 5 ms apart, one frame per chunk. Tracing is on in both columns.
Before is commit `e4e99a6` (the bench alone), after is `da62c60` (timing
recorded). Each value is the median of three runs that alternated between the
two commits, debug build, Docker Linux on a Windows host, November 11, 2026.

| API | First text P50 before | First text P50 after | Total P50 before | Total P50 after |
| --- | ---: | ---: | ---: | ---: |
| `/v1/messages` | 2.66 ms | 2.72 ms | 53.77 ms | 54.01 ms |
| `/cc/v1/messages` | 3.31 ms | 2.73 ms | 54.85 ms | 54.17 ms |
| `/v1/chat/completions` | 3.20 ms | 2.67 ms | 53.97 ms | 53.91 ms |
| `/v1/responses` | 3.19 ms | 3.11 ms | 54.18 ms | 54.33 ms |

The differences are below the spread between runs of one commit, which
reached 2.9 ms for first text P50 (`/v1/chat/completions` before: 2.66 to
5.53 ms). The run shows no overhead larger than that spread. It cannot prove
a difference below it.

The bench also prints the gateway's own view of one request per API. In this
fixture the longest gap between text chunks at the client was about 43 ms,
although the deltas are sent 5 ms apart. The summary places it on the upstream
side. In the two sampled requests that showed the gap, `receiveGaps.max` was
41 and 42 ms and `downstreamWait.maxUs` was 0.2 ms; in the other two the
client gap and `receiveGaps.max` were both 7 ms. The gap is on the hop between
the local fixture and the gateway, not in the gateway's response. It says
nothing about the real Kiro upstream.

```powershell
cargo test --locked gateway_router_cadence_measurement -- --ignored --nocapture
cargo test --locked router_timing
```

## TCP_NODELAY on client sockets

**Finding.** The 40 ms gap above came from the listener. The gateway did not
set `TCP_NODELAY` on accepted client sockets, so Nagle's algorithm plus the
client's delayed ACK held small SSE writes for about 40 ms. With the fixture
sending 8 text events 5 ms apart, the P50 of the longest client-side gap was:

| Fixture socket | Gateway socket | Gap max P50 | Total P50 |
| --- | --- | ---: | ---: |
| default | default | 43 ms | about 54 ms |
| `TCP_NODELAY` | default | 42 ms | about 54 ms |
| default | `TCP_NODELAY` | 42 ms | about 54 ms |
| `TCP_NODELAY` | `TCP_NODELAY` | 7 ms | about 54 ms |

Both sockets need the option in this fixture, because each hop stalls on its
own. Total time did not change; only the evenness of the chunks did.

**Change.** `common::net::nodelay_listener` wraps the `TcpListener` and sets
`TCP_NODELAY` on every accepted socket. A failure is logged as a warning and the
connection is kept. `main.rs` serves through it, with connect info still in
place for the client IP fallback. The router tests serve through the same
function, and the upstream fixture listener sets the option too, so the bench
measures the gateway rather than the fixture. `router_streams_small_events_without_nagle_stalls`
asserts the median longest chunk gap stays under 30 ms for a 10 ms cadence on
all four APIs.

**After.** Same bench, 200 requests per API, debug build, Docker Linux on a Windows host,
November 11, 2026, one run:

| API | Gap max P50 | Gap max P95 | Total P50 |
| --- | ---: | ---: | ---: |
| `/v1/messages` | 7.01 ms | 7.72 ms | 55.08 ms |
| `/cc/v1/messages` | 6.89 ms | 7.53 ms | 55.20 ms |
| `/v1/chat/completions` | 7.08 ms | 7.72 ms | 57.95 ms |
| `/v1/responses` | 7.67 ms | 8.98 ms | 56.42 ms |

The longest gap fell from about 43 ms to about 7 ms on every API. Total time is
within run-to-run spread of the earlier 54 ms. With the option removed, the new
test failed with medians near 42 ms.

**Limitations.** Measured on a loopback Linux fixture only. The effect on a real
network is unmeasured. Windows delays ACKs by up to 200 ms by default, so the
stall may be larger there, which is also unmeasured. The gateway's connection to
the real Kiro upstream is a separate hop and is not changed here.
