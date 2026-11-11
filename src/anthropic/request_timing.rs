//! The clock around [`Ledger`]: turns `Instant`s into offsets from request receipt.
//!
//! One `RequestTiming` belongs to one `RequestTracer`. Its writers (the handler, the
//! provider through the trace sink, the upstream body reader and the response stream)
//! run one after another on the request's own future chain, so the mutex is never
//! contended and never held across an await.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::Stream;

use crate::common::timing::{
    Arrival, ContentKind, ContentTally, Decoded, Ledger, Micros, OutputTokens, Phase, Span, Summary,
};
use crate::kiro::model::events::Event;

use super::stream::SseEvent;

pub(crate) struct RequestTiming {
    origin: Instant,
    ledger: parking_lot::Mutex<Ledger>,
}

fn micros(elapsed: Duration) -> Micros {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

impl RequestTiming {
    /// `received_at` is the origin of every offset. `entered_at` is when the inference
    /// handler took over; the call itself marks the end of request preparation.
    pub(crate) fn start(received_at: Instant, entered_at: Instant) -> Arc<Self> {
        let timing = Self {
            origin: received_at,
            ledger: parking_lot::Mutex::new(Ledger::default()),
        };
        let entered = timing.offset(entered_at);
        timing.record(|ledger, now| {
            ledger.mark(Phase::BodyParsed, entered);
            ledger.mark(Phase::Prepared, now);
        });
        Arc::new(timing)
    }

    fn offset(&self, at: Instant) -> Micros {
        micros(at.saturating_duration_since(self.origin))
    }

    fn record(&self, write: impl FnOnce(&mut Ledger, Micros)) {
        let now = self.offset(Instant::now());
        write(&mut self.ledger.lock(), now);
    }

    pub(crate) fn span(&self, span: Span, elapsed: Duration) {
        self.ledger.lock().span(span, micros(elapsed));
    }

    pub(crate) fn upstream_call(&self) {
        self.record(|ledger, now| ledger.upstream_call(now));
    }

    pub(crate) fn decoded(&self, decoded: &Decoded) {
        self.record(|ledger, now| ledger.decoded(now, decoded));
    }

    pub(crate) fn emitted(&self, content: &ContentTally) {
        self.record(|ledger, now| ledger.emitted(now, content));
    }

    pub(crate) fn upstream_eos(&self) {
        self.record(|ledger, now| ledger.upstream_eos(now));
    }

    /// Snapshot for the trace row. `reported_output_tokens` is `None` when the request
    /// ended without a usage snapshot.
    pub(crate) fn snapshot(&self, reported_output_tokens: Option<u64>) -> (Summary, OutputTokens) {
        let now = self.offset(Instant::now());
        let ledger = self.ledger.lock();
        (ledger.summarize(now), ledger.output_tokens(reported_output_tokens))
    }
}

/// Upstream body stream that reports each chunk's arrival and how long the reader
/// waited for it. It forwards every item unchanged.
pub(crate) struct TimedBody<S> {
    inner: S,
    timing: Option<Arc<RequestTiming>>,
    /// Survives a dropped `next()` future, so a keep-alive ping that wins the
    /// `select!` does not restart the wait.
    waiting_since: Option<Instant>,
}

impl<S> TimedBody<S> {
    pub(crate) fn new(inner: S, timing: Option<Arc<RequestTiming>>) -> Self {
        Self { inner, timing, waiting_since: None }
    }
}

impl<S, E> Stream for TimedBody<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(timing) = &this.timing else {
            return Pin::new(&mut this.inner).poll_next(cx);
        };
        let poll_started = *this.waiting_since.get_or_insert_with(Instant::now);
        let polled = Pin::new(&mut this.inner).poll_next(cx);
        if let Poll::Ready(item) = &polled {
            this.waiting_since = None;
            if let Some(Ok(chunk)) = item {
                let poll_started = timing.offset(poll_started);
                let bytes = chunk.len();
                timing.record(|ledger, at| ledger.upstream_chunk(Arrival { poll_started, at, bytes }));
            }
        }
        polled
    }
}

/// Add one upstream event to the tally of its chunk. A tool event always counts as
/// content, with or without input, like a `tool_use` block start.
pub(crate) fn tally_event(decoded: &mut Decoded, event: &Event) {
    decoded.frames = decoded.frames.saturating_add(1);
    match event {
        Event::AssistantResponse(response) if !response.content.is_empty() => {
            decoded.content.add(ContentKind::Text, response.content.len());
        }
        Event::ReasoningContent(reasoning) => {
            if let Some(text) = reasoning.text.as_deref().filter(|text| !text.is_empty()) {
                decoded.content.add(ContentKind::Thinking, text.len());
            }
        }
        Event::ToolUse(tool_use) => decoded.content.add(ContentKind::Tool, tool_use.input.len()),
        Event::Metadata(metadata) if metadata.token_usage.is_some() => decoded.final_usage = true,
        _ => {}
    }
}

/// Content in a batch of Messages events, by the rules that decide the first token:
/// a `tool_use` block start, or a non-empty text, thinking or tool-argument delta.
pub(crate) fn tally_sse(events: &[SseEvent]) -> ContentTally {
    let mut content = ContentTally::default();
    for event in events {
        match event.event.as_str() {
            "content_block_start" => {
                let block_type = event.data.pointer("/content_block/type");
                if block_type.and_then(serde_json::Value::as_str) == Some("tool_use") {
                    content.add(ContentKind::Tool, 0);
                }
            }
            "content_block_delta" => {
                for (field, kind) in [
                    ("text", ContentKind::Text),
                    ("thinking", ContentKind::Thinking),
                    ("partial_json", ContentKind::Tool),
                ] {
                    let value = event.data.get("delta").and_then(|delta| delta.get(field));
                    if let Some(value) = value.and_then(serde_json::Value::as_str)
                        && !value.is_empty()
                    {
                        content.add(kind, value.len());
                    }
                }
            }
            _ => {}
        }
    }
    content
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sse(event: &str, data: serde_json::Value) -> SseEvent {
        SseEvent::new(event, data)
    }

    #[test]
    fn sse_tally_counts_what_the_first_token_rule_counts() {
        let events = [
            sse("message_start", serde_json::json!({"message": {}})),
            sse("content_block_start", serde_json::json!({"content_block": {"type": "text"}})),
            sse("content_block_delta", serde_json::json!({"delta": {"text": ""}})),
            sse("ping", serde_json::json!({})),
        ];
        assert!(tally_sse(&events).is_empty());

        let mut expected = ContentTally::default();
        expected.add(ContentKind::Tool, 0);
        expected.add(ContentKind::Thinking, 2);
        expected.add(ContentKind::Text, 3);
        expected.add(ContentKind::Tool, 4);
        let events = [
            sse("content_block_start", serde_json::json!({"content_block": {"type": "tool_use"}})),
            sse("content_block_delta", serde_json::json!({"delta": {"thinking": "hm"}})),
            sse("content_block_delta", serde_json::json!({"delta": {"text": "abc"}})),
            sse("content_block_delta", serde_json::json!({"delta": {"partial_json": "{\"a\""}})),
        ];
        assert_eq!(tally_sse(&events), expected);
    }
}
