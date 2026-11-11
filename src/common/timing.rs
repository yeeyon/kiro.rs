//! Per-request timing ledger.
//!
//! Pure value types: no clock, no I/O. Callers pass offsets in microseconds since the
//! request was received, so every rule here is testable without sleeping.
//!
//! One [`Ledger`] covers one trace row. It can span several upstream calls (the
//! web-search loop), and folds each call so that the pause between two calls is never
//! counted as a receive gap or as generation time.

use serde::{Deserialize, Serialize};

/// Microseconds since the request was received by the auth middleware.
pub type Micros = u64;

/// Instants that happen at most once per request. The first write wins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// The inference handler was entered: auth, body read and JSON parsing are done.
    /// For Chat and Responses this also includes the adapter's translation to Messages.
    BodyParsed,
    /// Conversion, serialization, token counting and cache metering are done.
    Prepared,
    /// The first upstream call returned a 2xx response head.
    UpstreamHeaders,
    /// First non-empty upstream body chunk.
    FirstBodyBytes,
    /// First upstream event that carries thinking, text or tool data, before any hold.
    FirstDecodedContent,
    /// First Messages-format event of that kind handed to the response body stream.
    /// Handed over is not delivered: the client may read it later.
    FirstEmitThinking,
    FirstEmitText,
    FirstEmitTool,
}

impl Phase {
    pub const ALL: [Phase; 8] = [
        Phase::BodyParsed,
        Phase::Prepared,
        Phase::UpstreamHeaders,
        Phase::FirstBodyBytes,
        Phase::FirstDecodedContent,
        Phase::FirstEmitThinking,
        Phase::FirstEmitText,
        Phase::FirstEmitTool,
    ];
}

/// Durations that can happen several times per request (retries, web-search rounds).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Span {
    /// Credential selection, minus the two refresh spans below. When selection fails
    /// the whole wait is reported here, because no refresh split is available.
    CredentialSelect,
    /// Waiting for the token refresh lock.
    RefreshLockWait,
    /// Token refresh while holding the lock, and the forced refresh after a 403.
    TokenRefresh,
    /// Resolving the profile ARN.
    ProfileResolve,
    /// Request written to response head received. One per attempt.
    HeadersWait,
    /// Reading the body of a non-2xx attempt.
    ErrorBodyRead,
    /// Retry sleeps.
    Backoff,
}

impl Span {
    pub const ALL: [Span; 7] = [
        Span::CredentialSelect,
        Span::RefreshLockWait,
        Span::TokenRefresh,
        Span::ProfileResolve,
        Span::HeadersWait,
        Span::ErrorBodyRead,
        Span::Backoff,
    ];
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpanStat {
    pub total_us: u64,
    pub count: u32,
    pub max_us: u64,
}

impl SpanStat {
    fn add(&mut self, elapsed_us: u64) {
        self.total_us = self.total_us.saturating_add(elapsed_us);
        self.count = self.count.saturating_add(1);
        self.max_us = self.max_us.max(elapsed_us);
    }
}

const BUCKETS: usize = 12;

/// Bucket upper bounds belong to the scale, not to the histogram instance. A value
/// lands in the first bucket whose bound is not below it; the last bucket is open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scale {
    /// Upper bounds in ms: 1 2 5 10 25 50 100 250 500 1000 2500, then open.
    GapMicros,
    /// Upper bounds in frames: 0 1 2 3 4 6 8 12 16 32 64, then open.
    Frames,
}

impl Scale {
    const fn bounds(self) -> &'static [u64; BUCKETS - 1] {
        match self {
            Scale::GapMicros => &[
                1_000, 2_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
                1_000_000, 2_500_000,
            ],
            Scale::Frames => &[0, 1, 2, 3, 4, 6, 8, 12, 16, 32, 64],
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Histogram {
    pub counts: [u32; BUCKETS],
    pub n: u32,
    pub sum: u64,
    pub max: u64,
}

impl Histogram {
    pub fn record(&mut self, scale: Scale, value: u64) {
        let bucket = scale
            .bounds()
            .iter()
            .position(|bound| value <= *bound)
            .unwrap_or(BUCKETS - 1);
        self.counts[bucket] = self.counts[bucket].saturating_add(1);
        self.n = self.n.saturating_add(1);
        self.sum = self.sum.saturating_add(value);
        self.max = self.max.max(value);
    }

    /// Upper bound of the bucket that holds quantile `q`, capped at the largest value
    /// seen. `None` when empty.
    pub fn quantile_upper_bound(&self, scale: Scale, q: f64) -> Option<u64> {
        if self.n == 0 {
            return None;
        }
        let rank = ((q.clamp(0.0, 1.0) * f64::from(self.n)).ceil() as u32).max(1);
        let mut seen = 0u32;
        for (bucket, count) in self.counts.iter().enumerate() {
            seen = seen.saturating_add(*count);
            if seen >= rank {
                let bound = scale.bounds().get(bucket).copied().unwrap_or(self.max);
                return Some(bound.min(self.max));
            }
        }
        Some(self.max)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContentKind {
    Thinking,
    Text,
    Tool,
}

impl ContentKind {
    pub const ALL: [ContentKind; 3] = [ContentKind::Thinking, ContentKind::Text, ContentKind::Tool];
}

/// Content events and bytes per kind in one batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ContentTally {
    events: [u32; ContentKind::ALL.len()],
    bytes: [u32; ContentKind::ALL.len()],
}

impl ContentTally {
    pub fn add(&mut self, kind: ContentKind, bytes: usize) {
        let slot = kind as usize;
        self.events[slot] = self.events[slot].saturating_add(1);
        self.bytes[slot] = self.bytes[slot].saturating_add(u32::try_from(bytes).unwrap_or(u32::MAX));
    }

    pub fn is_empty(&self) -> bool {
        self.events.iter().all(|count| *count == 0)
    }

    fn events(&self, kind: ContentKind) -> u32 {
        self.events[kind as usize]
    }

    fn bytes(&self, kind: ContentKind) -> u32 {
        self.bytes[kind as usize]
    }
}

/// One upstream body chunk as the reader saw it.
#[derive(Clone, Copy, Debug, Default)]
pub struct Arrival {
    /// When the reader started waiting for this chunk.
    pub poll_started: Micros,
    pub at: Micros,
    pub bytes: usize,
}

/// What one upstream chunk decoded to.
#[derive(Clone, Copy, Debug, Default)]
pub struct Decoded {
    pub frames: u32,
    pub content: ContentTally,
    /// The chunk carried `metadataEvent.tokenUsage`.
    pub final_usage: bool,
}

/// Where the reported output token count came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputTokens {
    /// Every upstream call of the request sent `metadataEvent.tokenUsage`.
    Exact(u64),
    /// Counted locally from the content.
    Estimated(u64),
    /// No usable count: the request failed or was cancelled before usage was known,
    /// or the upstream reported zero although content arrived.
    Unavailable,
}

impl OutputTokens {
    pub const fn source(self) -> &'static str {
        match self {
            OutputTokens::Exact(_) => "exact",
            OutputTokens::Estimated(_) => "estimated",
            OutputTokens::Unavailable => "unavailable",
        }
    }

    /// Rebuild from the persisted source column and token count.
    pub fn from_stored(source: &str, tokens: u64) -> Option<Self> {
        match source {
            "exact" => Some(OutputTokens::Exact(tokens)),
            "estimated" => Some(OutputTokens::Estimated(tokens)),
            "unavailable" => Some(OutputTokens::Unavailable),
            _ => None,
        }
    }

    const fn count(self) -> Option<u64> {
        match self {
            OutputTokens::Exact(n) | OutputTokens::Estimated(n) => Some(n),
            OutputTokens::Unavailable => None,
        }
    }
}

/// Which client-facing API a request entered through.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Endpoint {
    Messages,
    ClaudeCode,
    Chat,
    Responses,
    #[default]
    Other,
}

impl Endpoint {
    pub const fn as_str(self) -> &'static str {
        match self {
            Endpoint::Messages => "messages",
            Endpoint::ClaudeCode => "cc",
            Endpoint::Chat => "chat",
            Endpoint::Responses => "responses",
            Endpoint::Other => "other",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PhaseMarks([Option<Micros>; Phase::ALL.len()]);

impl PhaseMarks {
    fn mark(&mut self, phase: Phase, at: Micros) {
        self.0[phase as usize].get_or_insert(at);
    }

    fn get(&self, phase: Phase) -> Option<Micros> {
        self.0[phase as usize]
    }
}

/// First-to-last time of a kind of content, summed over upstream calls.
#[derive(Clone, Copy, Debug, Default)]
struct Window {
    open: Option<(Micros, Micros)>,
    closed_us: u64,
    events: u32,
    bytes: u64,
}

impl Window {
    fn touch(&mut self, at: Micros, events: u32, bytes: u32) {
        if events == 0 {
            return;
        }
        let first = self.open.map_or(at, |(first, _)| first);
        self.open = Some((first, at));
        self.events = self.events.saturating_add(events);
        self.bytes = self.bytes.saturating_add(u64::from(bytes));
    }

    fn close(&mut self) {
        self.closed_us = self.total_us();
        self.open = None;
    }

    fn total_us(&self) -> u64 {
        let open = self.open.map_or(0, |(first, last)| last.saturating_sub(first));
        self.closed_us.saturating_add(open)
    }

    fn stat(&self) -> WindowStat {
        WindowStat {
            window_us: self.total_us(),
            events: self.events,
            bytes: self.bytes,
        }
    }
}

/// Cursors that restart with every upstream call.
#[derive(Clone, Copy, Debug, Default)]
struct CallCursor {
    last_chunk: Option<Micros>,
    /// Last batch handed to the response body that the reader has not waited past yet.
    handed: Option<Micros>,
    last_content_emit: Option<Micros>,
    usage_seen: bool,
}

/// All timing state of one trace row. Fixed size, no heap.
#[derive(Clone, Debug, Default)]
pub struct Ledger {
    marks: PhaseMarks,
    spans: [SpanStat; Span::ALL.len()],
    calls: u32,
    calls_with_usage: u32,
    call: CallCursor,
    chunks: u32,
    bytes: u64,
    upstream_wait_us: u64,
    downstream_wait: SpanStat,
    receive_gaps: Histogram,
    frames_per_chunk: Histogram,
    emit_gaps: Histogram,
    content_emits: u32,
    generation: Window,
    kinds: [Window; ContentKind::ALL.len()],
    last_content: Option<Micros>,
    final_usage: Option<Micros>,
    eos: Option<Micros>,
}

impl Ledger {
    pub fn mark(&mut self, phase: Phase, at: Micros) {
        self.marks.mark(phase, at);
    }

    pub fn span(&mut self, span: Span, elapsed_us: u64) {
        self.spans[span as usize].add(elapsed_us);
    }

    /// An upstream call returned a response head.
    pub fn upstream_call(&mut self, at: Micros) {
        self.marks.mark(Phase::UpstreamHeaders, at);
        self.calls = self.calls.saturating_add(1);
        self.call = CallCursor::default();
        self.generation.close();
        for window in &mut self.kinds {
            window.close();
        }
    }

    pub fn upstream_chunk(&mut self, arrival: Arrival) {
        if arrival.bytes == 0 {
            return;
        }
        self.marks.mark(Phase::FirstBodyBytes, arrival.at);
        self.chunks = self.chunks.saturating_add(1);
        self.bytes = self.bytes.saturating_add(arrival.bytes as u64);
        self.upstream_wait_us = self
            .upstream_wait_us
            .saturating_add(arrival.at.saturating_sub(arrival.poll_started));
        if let Some(handed) = self.call.handed.take() {
            self.downstream_wait.add(arrival.poll_started.saturating_sub(handed));
        }
        if let Some(last) = self.call.last_chunk {
            self.receive_gaps.record(Scale::GapMicros, arrival.at.saturating_sub(last));
        }
        self.call.last_chunk = Some(arrival.at);
    }

    pub fn decoded(&mut self, at: Micros, decoded: &Decoded) {
        self.frames_per_chunk.record(Scale::Frames, u64::from(decoded.frames));
        if decoded.final_usage {
            self.final_usage = Some(at);
            if !self.call.usage_seen {
                self.call.usage_seen = true;
                self.calls_with_usage = self.calls_with_usage.saturating_add(1);
            }
        }
        if decoded.content.is_empty() {
            return;
        }
        self.marks.mark(Phase::FirstDecodedContent, at);
        self.last_content = Some(at);
        let (mut events, mut bytes) = (0u32, 0u32);
        for kind in ContentKind::ALL {
            let (kind_events, kind_bytes) = (decoded.content.events(kind), decoded.content.bytes(kind));
            self.kinds[kind as usize].touch(at, kind_events, kind_bytes);
            events = events.saturating_add(kind_events);
            bytes = bytes.saturating_add(kind_bytes);
        }
        self.generation.touch(at, events, bytes);
    }

    /// A non-empty batch was handed to the response body stream.
    pub fn emitted(&mut self, at: Micros, content: &ContentTally) {
        self.call.handed = Some(at);
        if content.is_empty() {
            return;
        }
        for (kind, phase) in [
            (ContentKind::Thinking, Phase::FirstEmitThinking),
            (ContentKind::Text, Phase::FirstEmitText),
            (ContentKind::Tool, Phase::FirstEmitTool),
        ] {
            if content.events(kind) > 0 {
                self.marks.mark(phase, at);
            }
        }
        if let Some(last) = self.call.last_content_emit {
            self.emit_gaps.record(Scale::GapMicros, at.saturating_sub(last));
        }
        self.call.last_content_emit = Some(at);
        self.content_emits = self.content_emits.saturating_add(1);
    }

    /// The upstream body of the current call ended cleanly.
    pub fn upstream_eos(&mut self, at: Micros) {
        self.eos = Some(at);
    }

    /// `reported` is the request's output token count, or `None` when the request
    /// ended without a usage snapshot.
    pub fn output_tokens(&self, reported: Option<u64>) -> OutputTokens {
        let Some(tokens) = reported else {
            return OutputTokens::Unavailable;
        };
        if self.calls == 0 || self.calls_with_usage < self.calls {
            return OutputTokens::Estimated(tokens);
        }
        // A missing `outputTokens` field deserializes as 0.
        if tokens == 0 && self.generation.events > 0 {
            return OutputTokens::Unavailable;
        }
        OutputTokens::Exact(tokens)
    }

    pub fn summarize(&self, now: Micros) -> Summary {
        let phase = |phase| self.marks.get(phase);
        let span = |span: Span| self.spans[span as usize];
        let kind = |kind: ContentKind| self.kinds[kind as usize].stat();
        Summary {
            v: Summary::VERSION,
            total_us: now,
            phases: PhaseSummary {
                body_parsed_us: phase(Phase::BodyParsed),
                prepared_us: phase(Phase::Prepared),
                upstream_headers_us: phase(Phase::UpstreamHeaders),
                first_body_bytes_us: phase(Phase::FirstBodyBytes),
                first_decoded_content_us: phase(Phase::FirstDecodedContent),
                first_emit_thinking_us: phase(Phase::FirstEmitThinking),
                first_emit_text_us: phase(Phase::FirstEmitText),
                first_emit_tool_us: phase(Phase::FirstEmitTool),
            },
            spans: SpanSummary {
                credential_select: span(Span::CredentialSelect),
                refresh_lock_wait: span(Span::RefreshLockWait),
                token_refresh: span(Span::TokenRefresh),
                profile_resolve: span(Span::ProfileResolve),
                headers_wait: span(Span::HeadersWait),
                error_body_read: span(Span::ErrorBodyRead),
                backoff: span(Span::Backoff),
            },
            stream: (self.calls > 0).then(|| StreamSummary {
                upstream_calls: self.calls,
                chunks: self.chunks,
                bytes: self.bytes,
                upstream_wait_us: self.upstream_wait_us,
                downstream_wait: self.downstream_wait,
                receive_gaps: self.receive_gaps,
                frames_per_chunk: self.frames_per_chunk,
                emit_gaps: self.emit_gaps,
                content_emits: self.content_emits,
                last_content_us: self.last_content,
                final_usage_us: self.final_usage,
                upstream_eos_us: self.eos,
                generation: self.generation.stat(),
                thinking: kind(ContentKind::Thinking),
                text: kind(ContentKind::Text),
                tool: kind(ContentKind::Tool),
            }),
        }
    }
}

/// The persisted per-request shape. Bounded: no `Vec`, no `String`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub v: u8,
    /// Request received to trace row written.
    pub total_us: u64,
    pub phases: PhaseSummary,
    pub spans: SpanSummary,
    /// `None` when no upstream call returned a response head.
    pub stream: Option<StreamSummary>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PhaseSummary {
    pub body_parsed_us: Option<Micros>,
    pub prepared_us: Option<Micros>,
    pub upstream_headers_us: Option<Micros>,
    pub first_body_bytes_us: Option<Micros>,
    pub first_decoded_content_us: Option<Micros>,
    pub first_emit_thinking_us: Option<Micros>,
    pub first_emit_text_us: Option<Micros>,
    pub first_emit_tool_us: Option<Micros>,
}

/// Exhaustive on purpose: a new phase without a summary field does not compile.
#[cfg(test)]
impl PhaseSummary {
    fn get(&self, phase: Phase) -> Option<Micros> {
        match phase {
            Phase::BodyParsed => self.body_parsed_us,
            Phase::Prepared => self.prepared_us,
            Phase::UpstreamHeaders => self.upstream_headers_us,
            Phase::FirstBodyBytes => self.first_body_bytes_us,
            Phase::FirstDecodedContent => self.first_decoded_content_us,
            Phase::FirstEmitThinking => self.first_emit_thinking_us,
            Phase::FirstEmitText => self.first_emit_text_us,
            Phase::FirstEmitTool => self.first_emit_tool_us,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpanSummary {
    pub credential_select: SpanStat,
    pub refresh_lock_wait: SpanStat,
    pub token_refresh: SpanStat,
    pub profile_resolve: SpanStat,
    pub headers_wait: SpanStat,
    pub error_body_read: SpanStat,
    pub backoff: SpanStat,
}

#[cfg(test)]
impl SpanSummary {
    fn get(&self, span: Span) -> SpanStat {
        match span {
            Span::CredentialSelect => self.credential_select,
            Span::RefreshLockWait => self.refresh_lock_wait,
            Span::TokenRefresh => self.token_refresh,
            Span::ProfileResolve => self.profile_resolve,
            Span::HeadersWait => self.headers_wait,
            Span::ErrorBodyRead => self.error_body_read,
            Span::Backoff => self.backoff,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowStat {
    /// First to last decoded event of this kind, summed over upstream calls.
    pub window_us: u64,
    pub events: u32,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamSummary {
    pub upstream_calls: u32,
    pub chunks: u32,
    pub bytes: u64,
    /// Time the gateway spent waiting for upstream body chunks.
    pub upstream_wait_us: u64,
    /// Time between handing a batch to the response body and asking the upstream for
    /// more: the response writer and the client. Zero for non-stream requests.
    pub downstream_wait: SpanStat,
    /// Time between consecutive upstream chunks of one call ([`Scale::GapMicros`]).
    /// Delivery is pull-based, so a gap contains downstream wait as well.
    pub receive_gaps: Histogram,
    /// Frames decoded per upstream chunk ([`Scale::Frames`]).
    pub frames_per_chunk: Histogram,
    /// Time between consecutive content-bearing batches handed to the response body
    /// ([`Scale::GapMicros`]). Pings are not content.
    pub emit_gaps: Histogram,
    pub content_emits: u32,
    /// Last decoded content.
    pub last_content_us: Option<Micros>,
    /// Last `metadataEvent.tokenUsage`.
    pub final_usage_us: Option<Micros>,
    /// Clean end of the last upstream body.
    pub upstream_eos_us: Option<Micros>,
    /// All content kinds together.
    pub generation: WindowStat,
    pub thinking: WindowStat,
    pub text: WindowStat,
    pub tool: WindowStat,
}

/// Rates and gap quantiles derived from a summary on read. Never stored.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Throughput {
    /// Upper bound of the histogram bucket that holds the quantile, not an exact value.
    pub emit_gap_p50_us: Option<u64>,
    pub emit_gap_p95_us: Option<u64>,
    pub receive_gap_p50_us: Option<u64>,
    pub receive_gap_p95_us: Option<u64>,
    /// Output tokens per second of generation window. The token count covers thinking,
    /// text and tool arguments together. Tokens of the first event were generated
    /// before the window opens, so short responses read slightly high.
    pub output_tps: Option<f64>,
    /// Provenance of the token count behind `output_tps`.
    pub output_tokens_source: &'static str,
    pub text_bytes_per_sec: Option<f64>,
    pub thinking_bytes_per_sec: Option<f64>,
}

impl Summary {
    pub const VERSION: u8 = 1;
    /// Below this a window is dominated by chunking, not by generation speed.
    const MIN_RATE_WINDOW_US: u64 = 250_000;

    pub fn throughput(&self, tokens: OutputTokens) -> Throughput {
        let rate = |amount: u64, window: WindowStat| {
            (window.events >= 2 && window.window_us >= Self::MIN_RATE_WINDOW_US)
                .then(|| amount as f64 * 1_000_000.0 / window.window_us as f64)
        };
        let stream = self.stream.as_ref();
        let gap = |gaps: fn(&StreamSummary) -> &Histogram, q: f64| {
            stream.and_then(|stream| gaps(stream).quantile_upper_bound(Scale::GapMicros, q))
        };
        Throughput {
            emit_gap_p50_us: gap(|stream| &stream.emit_gaps, 0.5),
            emit_gap_p95_us: gap(|stream| &stream.emit_gaps, 0.95),
            receive_gap_p50_us: gap(|stream| &stream.receive_gaps, 0.5),
            receive_gap_p95_us: gap(|stream| &stream.receive_gaps, 0.95),
            output_tps: stream
                .zip(tokens.count())
                .and_then(|(stream, count)| rate(count, stream.generation)),
            output_tokens_source: tokens.source(),
            text_bytes_per_sec: stream.and_then(|stream| rate(stream.text.bytes, stream.text)),
            thinking_bytes_per_sec: stream
                .and_then(|stream| rate(stream.thinking.bytes, stream.thinking)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(bytes: usize) -> Decoded {
        let mut content = ContentTally::default();
        content.add(ContentKind::Text, bytes);
        Decoded { frames: 1, content, final_usage: false }
    }

    fn usage() -> Decoded {
        Decoded { frames: 1, content: ContentTally::default(), final_usage: true }
    }

    fn chunk(ledger: &mut Ledger, poll_started: Micros, at: Micros) {
        ledger.upstream_chunk(Arrival { poll_started, at, bytes: 10 });
    }

    #[test]
    fn histogram_places_edge_values_in_the_bounded_bucket() {
        let mut gaps = Histogram::default();
        for value in [0, 1_000, 1_001, 2_500_000, 2_500_001] {
            gaps.record(Scale::GapMicros, value);
        }
        assert_eq!(gaps.counts, [2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1]);
        assert_eq!((gaps.n, gaps.max), (5, 2_500_001));

        let mut frames = Histogram::default();
        for value in [0, 1, 4, 5, 64, 65] {
            frames.record(Scale::Frames, value);
        }
        assert_eq!(frames.counts, [1, 1, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1]);
    }

    #[test]
    fn histogram_quantile_is_an_upper_bound_capped_at_max() {
        let mut gaps = Histogram::default();
        assert_eq!(gaps.quantile_upper_bound(Scale::GapMicros, 0.5), None);
        for value in [300, 400, 4_000, 40_000] {
            gaps.record(Scale::GapMicros, value);
        }
        assert_eq!(gaps.quantile_upper_bound(Scale::GapMicros, 0.5), Some(1_000));
        assert_eq!(gaps.quantile_upper_bound(Scale::GapMicros, 0.75), Some(5_000));
        assert_eq!(gaps.quantile_upper_bound(Scale::GapMicros, 1.0), Some(40_000));
        gaps.record(Scale::GapMicros, 9_000_000);
        assert_eq!(gaps.quantile_upper_bound(Scale::GapMicros, 1.0), Some(9_000_000));
    }

    #[test]
    fn every_phase_is_first_wins_and_reaches_the_summary() {
        let mut ledger = Ledger::default();
        for (index, phase) in Phase::ALL.into_iter().enumerate() {
            ledger.mark(phase, 100 + index as u64);
            ledger.mark(phase, 9_999);
        }
        let summary = ledger.summarize(10_000);
        for (index, phase) in Phase::ALL.into_iter().enumerate() {
            assert_eq!(summary.phases.get(phase), Some(100 + index as u64), "{phase:?}");
        }
    }

    #[test]
    fn every_span_accumulates_and_reaches_the_summary() {
        let mut ledger = Ledger::default();
        for (index, span) in Span::ALL.into_iter().enumerate() {
            ledger.span(span, 10 + index as u64);
            ledger.span(span, 500);
        }
        let summary = ledger.summarize(0);
        for (index, span) in Span::ALL.into_iter().enumerate() {
            let expected = SpanStat { total_us: 510 + index as u64, count: 2, max_us: 500 };
            assert_eq!(summary.spans.get(span), expected, "{span:?}");
        }
    }

    #[test]
    fn no_upstream_call_means_no_stream_summary() {
        let mut ledger = Ledger::default();
        ledger.span(Span::HeadersWait, 40);
        assert_eq!(ledger.summarize(50).stream, None);
    }

    #[test]
    fn chunk_waits_split_into_upstream_and_downstream() {
        let mut ledger = Ledger::default();
        ledger.upstream_call(1_000);
        chunk(&mut ledger, 1_000, 1_400);
        ledger.decoded(1_410, &text(5));
        let mut emitted = ContentTally::default();
        emitted.add(ContentKind::Text, 5);
        ledger.emitted(1_420, &emitted);
        chunk(&mut ledger, 1_720, 2_020);
        ledger.decoded(2_030, &text(5));
        ledger.emitted(2_040, &emitted);
        ledger.upstream_eos(2_100);

        let summary = ledger.summarize(2_200);
        let derived = summary.throughput(OutputTokens::Unavailable);
        assert_eq!((derived.receive_gap_p50_us, derived.emit_gap_p95_us), (Some(620), Some(620)));
        let stream = summary.stream.unwrap();
        assert_eq!((stream.chunks, stream.bytes), (2, 20));
        assert_eq!(stream.upstream_wait_us, 400 + 300);
        assert_eq!(stream.downstream_wait, SpanStat { total_us: 300, count: 1, max_us: 300 });
        assert_eq!((stream.receive_gaps.n, stream.receive_gaps.max), (1, 620));
        assert_eq!((stream.emit_gaps.n, stream.emit_gaps.max), (1, 620));
        assert_eq!(stream.content_emits, 2);
        assert_eq!(stream.upstream_eos_us, Some(2_100));
        assert_eq!(stream.last_content_us, Some(2_030));
    }

    #[test]
    fn emit_marks_separate_kinds_and_ignore_batches_without_content() {
        let mut ledger = Ledger::default();
        ledger.upstream_call(0);
        ledger.emitted(10, &ContentTally::default());
        let mut thinking = ContentTally::default();
        thinking.add(ContentKind::Thinking, 4);
        ledger.emitted(20, &thinking);
        let mut text_and_tool = ContentTally::default();
        text_and_tool.add(ContentKind::Text, 4);
        text_and_tool.add(ContentKind::Tool, 0);
        ledger.emitted(50, &text_and_tool);

        let summary = ledger.summarize(60);
        assert_eq!(summary.phases.first_emit_thinking_us, Some(20));
        assert_eq!(summary.phases.first_emit_text_us, Some(50));
        assert_eq!(summary.phases.first_emit_tool_us, Some(50));
        let stream = summary.stream.unwrap();
        assert_eq!((stream.content_emits, stream.emit_gaps.n), (2, 1));
    }

    #[test]
    fn pause_between_upstream_calls_is_not_a_gap_or_generation_time() {
        let mut ledger = Ledger::default();
        ledger.upstream_call(100);
        chunk(&mut ledger, 100, 200);
        ledger.decoded(200, &text(10));
        chunk(&mut ledger, 200, 500_200);
        ledger.decoded(500_200, &text(10));

        ledger.upstream_call(9_000_000);
        chunk(&mut ledger, 9_000_000, 9_000_100);
        ledger.decoded(9_000_100, &text(10));
        chunk(&mut ledger, 9_000_100, 9_300_100);
        ledger.decoded(9_300_100, &text(10));

        let summary = ledger.summarize(9_400_000);
        assert_eq!(summary.phases.upstream_headers_us, Some(100));
        let stream = summary.stream.unwrap();
        assert_eq!(stream.upstream_calls, 2);
        assert_eq!((stream.receive_gaps.n, stream.receive_gaps.max), (2, 500_000));
        assert_eq!(stream.generation, WindowStat { window_us: 800_000, events: 4, bytes: 40 });
        assert_eq!(stream.text, stream.generation);
        assert_eq!(stream.thinking, WindowStat::default());
    }

    #[test]
    fn output_tokens_are_exact_only_when_every_call_reported_usage() {
        let mut ledger = Ledger::default();
        assert_eq!(ledger.output_tokens(None), OutputTokens::Unavailable);
        assert_eq!(ledger.output_tokens(Some(7)), OutputTokens::Estimated(7));

        ledger.upstream_call(0);
        ledger.decoded(10, &text(5));
        assert_eq!(ledger.output_tokens(Some(7)), OutputTokens::Estimated(7));
        ledger.decoded(20, &usage());
        ledger.decoded(30, &usage());
        assert_eq!(ledger.output_tokens(Some(7)), OutputTokens::Exact(7));
        assert_eq!(ledger.output_tokens(Some(0)), OutputTokens::Unavailable);
        assert_eq!(ledger.output_tokens(None), OutputTokens::Unavailable);
        assert_eq!(ledger.summarize(40).stream.unwrap().final_usage_us, Some(30));

        ledger.upstream_call(100);
        assert_eq!(ledger.output_tokens(Some(7)), OutputTokens::Estimated(7));
    }

    #[test]
    fn exact_zero_without_content_stays_exact() {
        let mut ledger = Ledger::default();
        ledger.upstream_call(0);
        ledger.decoded(10, &usage());
        assert_eq!(ledger.output_tokens(Some(0)), OutputTokens::Exact(0));
    }

    fn summary_with_text_window(window_us: u64, events: u32) -> Summary {
        let mut ledger = Ledger::default();
        ledger.upstream_call(0);
        let mut thinking = ContentTally::default();
        thinking.add(ContentKind::Thinking, 1_000);
        ledger.decoded(1_000, &Decoded { frames: 1, content: thinking, final_usage: false });
        for index in 0..events {
            let at = 2_000 + window_us * u64::from(index) / u64::from(events.max(2) - 1);
            ledger.decoded(at, &text(500));
        }
        // The trailer after the last content must not stretch the window.
        ledger.decoded(60_000_000, &usage());
        ledger.summarize(60_000_000)
    }

    #[test]
    fn throughput_uses_the_generation_window_and_names_its_source() {
        let summary = summary_with_text_window(999_000, 2);
        let exact = summary.throughput(OutputTokens::Exact(100));
        assert_eq!(exact.output_tps, Some(100.0));
        assert_eq!(exact.output_tokens_source, "exact");
        assert_eq!(exact.text_bytes_per_sec, Some(1_000.0 * 1_000_000.0 / 999_000.0));
        assert_eq!(exact.thinking_bytes_per_sec, None);

        let estimated = summary.throughput(OutputTokens::Estimated(50));
        assert_eq!((estimated.output_tps, estimated.output_tokens_source), (Some(50.0), "estimated"));

        let unavailable = summary.throughput(OutputTokens::Unavailable);
        assert_eq!((unavailable.output_tps, unavailable.output_tokens_source), (None, "unavailable"));
        assert!(unavailable.text_bytes_per_sec.is_some());
    }

    #[test]
    fn throughput_is_withheld_for_short_windows_and_single_events() {
        let short = summary_with_text_window(200_000, 3);
        assert_eq!(short.throughput(OutputTokens::Exact(100)).text_bytes_per_sec, None);
        // thinking + text still span 201 ms in total: below the 250 ms floor.
        assert_eq!(short.throughput(OutputTokens::Exact(100)).output_tps, None);

        let single = summary_with_text_window(0, 1);
        assert_eq!(single.throughput(OutputTokens::Exact(100)).text_bytes_per_sec, None);

        let no_stream = Ledger::default().summarize(5);
        assert_eq!(no_stream.throughput(OutputTokens::Exact(100)).output_tps, None);
    }

    #[test]
    fn summary_round_trips_through_json_with_stable_field_names() {
        let mut ledger = Ledger::default();
        ledger.mark(Phase::BodyParsed, 120);
        ledger.span(Span::HeadersWait, 900);
        ledger.upstream_call(1_000);
        chunk(&mut ledger, 1_000, 1_500);
        ledger.decoded(1_500, &text(3));
        let summary = ledger.summarize(2_000);

        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(json["v"], 1);
        assert_eq!(json["totalUs"], 2_000);
        assert_eq!(json["phases"]["bodyParsedUs"], 120);
        assert_eq!(json["phases"]["firstEmitTextUs"], serde_json::Value::Null);
        assert_eq!(json["spans"]["headersWait"]["totalUs"], 900);
        assert_eq!(json["stream"]["receiveGaps"]["counts"].as_array().unwrap().len(), 12);
        assert_eq!(json["stream"]["text"]["bytes"], 3);
        assert_eq!(serde_json::from_value::<Summary>(json).unwrap(), summary);
    }

    #[test]
    fn output_token_source_round_trips_through_its_stored_name() {
        for tokens in [OutputTokens::Exact(9), OutputTokens::Estimated(9), OutputTokens::Unavailable] {
            assert_eq!(OutputTokens::from_stored(tokens.source(), 9), Some(tokens));
        }
        assert_eq!(OutputTokens::from_stored("provider", 9), None);
    }
}
