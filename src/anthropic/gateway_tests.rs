//! Real HTTP fixtures for gateway regressions and repeatable latency measurements.
use super::{
    handlers::{post_messages, post_messages_cc},
    middleware::{AppState, KeyContext},
    openai::post_chat_completions,
    responses::post_responses,
};
use crate::{
    admin::trace_db::{TraceKeySource, TraceQuery, TraceStore},
    kiro::{
        endpoint::{KiroEndpoint, RequestContext},
        model::credentials::KiroCredentials,
        parser::crc::crc32,
        provider::KiroProvider,
        token_manager::MultiTokenManager,
    },
    model::config::{Config, ToolCompatibilityMode},
};
use axum::{
    Extension, Json, Router,
    body::{Body, to_bytes},
    extract::ConnectInfo,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::post,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use parking_lot::Mutex;
use serde_json::json;

#[path = "chat_stream_gateway_tests.rs"]
mod chat_stream_tests;
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
enum Mode {
    ReviewSuspended,
    ReviewRepetition,
    ReviewEndTurn,
    Normal(Duration),
    Interrupted,
    Overloaded,
    Large,
    Truncated,
    InvalidFrame,
    Empty,
    Tools,
    GatedChatText,
    GatedToolArguments,
    CompactionRetry,
    Filtered,
    Stalled,
    SlowHeaders,
    SlowErrorBody,
    MetadataFirst,
    Active,
    /// 上游内容过滤拦截：只有 reasoning，`metadataEvent.stopReason = CONTENT_FILTERED`，
    /// 随后 contextUsage 100%（验证拒答不被上下文溢出覆盖）。帧序列取自线上抓包。
    Refused,
    RefusedWithSearch,
    RefusedWithPartialTool,
    RefusedWithInvalidTool,
    RefusedWithEmptyTool,
    RefusedBare,
    /// 上游按给定片段依次返回 assistantResponseEvent，随后 metadataEvent（用量 13/17）。
    Reply(&'static [&'static str]),
    ReplyWithCredits(&'static [&'static str]),
    /// 同 `Reply`，但先返回一段原生 reasoning。
    ReplyWithReasoning(&'static [&'static str]),
    /// 正文以字面 `<thinking>` 开头（L7：extractThinking=false 时流式不能拆出思考块）。
    ThinkingText,
    /// 正文之后上游下发 ThrottlingException 异常帧（M3）。
    Throttled,
    /// web_search 循环里工具参数是半截 JSON（L10）。
    SearchInvalidToolJson,
    /// 只有正文、没有 metadataEvent.tokenUsage：线上 Kiro 对 claude-opus/sonnet-5.5 的实际形态
    /// （traces.db 里 usage_source 全是 none），缓存用量只能来自本地 CacheMeter。
    NoProviderUsage,
    /// `events` text deltas, one frame per chunk, `every` apart, then metadataEvent.
    Cadence { events: usize, every: Duration },
}

struct Fixture {
    state: AppState,
    calls: Arc<AtomicUsize>,
    connections: Arc<Mutex<HashSet<SocketAddr>>>,
    close_headers: Arc<AtomicUsize>,
    /// 上游收到的 Kiro 请求体（JSON）。
    bodies: Arc<Mutex<Vec<serde_json::Value>>>,
    release_tool_arguments: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}

#[tokio::test]
async fn tool_mode_admin_switch_updates_gateway_and_preserves_in_flight_snapshot() {
    use crate::admin::{
        AdminService, AdminState, ClientKeyManager, GroupManager, UsageAggregator,
        create_admin_router,
    };

    let path = std::env::temp_dir().join(format!("tool-mode-api-{}.json", uuid::Uuid::new_v4()));
    std::fs::write(&path, r#"{"toolCompatibilityMode":"claude-code"}"#).unwrap();
    let fixture = fixture_with_config(Mode::Reply(&["ok"]), Config::load(&path).unwrap()).await;
    let manager = fixture.state.kiro_provider.as_ref().unwrap().token_manager();
    let state = AdminState::new(
        "fixture-admin-only",
        AdminService::new(manager.clone(), vec!["fixture".into()]),
        Arc::new(ClientKeyManager::new()),
        Arc::new(UsageAggregator::new()),
        Arc::new(TraceStore::open_in_memory().unwrap()),
        Arc::new(GroupManager::new()),
    );
    let app = create_admin_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/config/tool-compatibility", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    assert_eq!(client.get(&url).send().await.unwrap().status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        client.put(&url).json(&json!({"mode":"raw"})).send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let initial: serde_json::Value = client.get(&url).header("x-api-key", "fixture-admin-only")
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(initial, json!({"mode":"claude-code"}));
    for invalid in [json!({"mode":"invalid"}), json!({}), json!({"mode":"raw","extra":true})] {
        let response = client.put(&url).header("x-api-key", "fixture-admin-only")
            .json(&invalid).send().await.unwrap();
        assert!(response.status().is_client_error());
        assert_eq!(manager.tool_compatibility_mode(), ToolCompatibilityMode::ClaudeCode);
    }
    let in_flight = fixture.state.clone().snapshot_tool_compatibility_mode();
    for (mode, name) in [("raw", "Write"), ("claude-code", "fs_write"), ("raw", "Write")] {
        let response = client.put(&url).header("x-api-key", "fixture-admin-only")
            .json(&json!({"mode":mode})).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.json::<serde_json::Value>().await.unwrap(), json!({"mode":mode}));
        for endpoint in ["messages", "cc", "chat", "responses"] {
            for streaming in [false, true] {
                let mut payload = json!({
                    "model":"gpt-5.6-sol", "max_tokens":64, "stream":streaming,
                    "messages":[{"role":"user","content":"hello"}],
                    "input":[{"role":"user","content":"hello"}],
                    "tools":[{"name":"Write","description":"fixture", "input_schema":{
                        "type":"object", "properties":{"file_path":{"type":"string"},"content":{"type":"string"}}
                    }}]
                });
                let parameters = payload["tools"][0]["input_schema"].clone();
                let response = if endpoint == "chat" {
                    payload["tools"] = json!([{"type":"function", "function":{
                        "name":"Write", "description":"fixture", "parameters":parameters
                    }}]);
                    post_chat_completions(axum::extract::State(fixture.state.clone()), Extension(key()), HeaderMap::new(), Json(serde_json::from_value(payload).unwrap())).await
                } else if endpoint == "responses" {
                    payload["tools"] = json!([{
                        "type":"function", "name":"Write", "description":"fixture", "parameters":parameters
                    }]);
                    post_responses(axum::extract::State(fixture.state.clone()), Extension(key()), HeaderMap::new(), Json(serde_json::from_value(payload).unwrap())).await
                } else if endpoint == "cc" {
                    post_messages_cc(axum::extract::State(fixture.state.clone()), Extension(key()), Json(serde_json::from_value(payload).unwrap())).await
                } else {
                    post_messages(axum::extract::State(fixture.state.clone()), Extension(key()), Json(serde_json::from_value(payload).unwrap())).await
                };
                assert_eq!(response.status(), StatusCode::OK);
                to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
                let bodies = fixture.bodies.lock();
                let tools = &bodies.last().unwrap()["conversationState"]["currentMessage"]["userInputMessage"]["userInputMessageContext"]["tools"];
                assert_eq!(tools[0]["toolSpecification"]["name"], name, "{tools}");
            }
        }
    }
    assert_eq!(in_flight.tool_compatibility_mode, ToolCompatibilityMode::ClaudeCode);
    assert_eq!(Config::load(&path).unwrap().tool_compatibility_mode, ToolCompatibilityMode::Raw);
    std::fs::write(&path, "invalid json").unwrap();
    let response = client.put(&url).header("x-api-key", "fixture-admin-only")
        .json(&json!({"mode":"claude-code"})).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(manager.tool_compatibility_mode(), ToolCompatibilityMode::Raw);
    server.abort();
    std::fs::remove_file(&path).unwrap();
}

#[tokio::test]
async fn integration_repeat_guard_records_error() {
    let mut fixture = fixture(Mode::ReviewRepetition).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    fixture.state = fixture.state.clone().with_trace_store(Some(store.clone()));
    let response = request(fixture.state.clone(), "messages", true).await;
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("repeated-token loop"));
    let mut records = Vec::new();
    for _ in 0..50 {
        records = store.query_paged(&TraceQuery { limit: 10, ..Default::default() }).0;
        if !records.is_empty() { break; }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].final_status, "error");
}

async fn assert_ungrouped_key_usage(is_system: bool) {
    use crate::admin::client_keys::ClientKeyManager;

    let fixture = fixture(Mode::ReplyWithCredits(&["hello"])).await;
    let path = std::env::temp_dir().join(format!("client-key-usage-{}.json", uuid::Uuid::new_v4()));
    let manager = Arc::new(ClientKeyManager::load(&path).unwrap());
    let plaintext = if is_system {
        manager.sync_system_key("system".into(), None, "fixture-system-key".into());
        "fixture-system-key"
    } else {
        manager.create_with_key("client".into(), None, None, "fixture-client-key".into());
        "fixture-client-key"
    };
    let key_id = manager.list()[0].id;
    assert_eq!(key_id == 0, is_system);
    assert_eq!(manager.group_of(key_id), None);

    let app = super::router::create_router_with_shared_provider(
        fixture.state.kiro_provider.clone(),
        false,
        ToolCompatibilityMode::Raw,
        Some(manager.clone()),
        None,
        None,
        None,
        None,
        None,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let mut calls = 0;
    for endpoint in [
        "/v1/messages",
        "/cc/v1/messages",
        "/v1/chat/completions",
        "/v1/responses",
    ] {
        for streaming in [false, true] {
            let payload = json!({
                "model": "gpt-5.6-sol",
                "max_tokens": 64,
                "stream": streaming,
                "messages": [{"role": "user", "content": "hello"}],
                "input": [{"role": "user", "content": "hello"}]
            });
            let response = client
                .post(format!("http://{address}{endpoint}"))
                .header("x-api-key", plaintext)
                .json(&payload)
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "{endpoint}, stream={streaming}"
            );
            let body = response.text().await.unwrap();
            assert!(
                body.contains("hello"),
                "{endpoint}, stream={streaming}: {body}"
            );
            calls += 1;
            let reloaded = ClientKeyManager::load(&path).unwrap();
            for snapshot in [manager.list(), reloaded.list()] {
                let entry = snapshot.iter().find(|entry| entry.id == key_id).unwrap();
                assert_eq!(entry.group, None);
                assert_eq!(entry.total_calls, calls);
                assert_eq!(
                    (
                        entry.total_input_tokens,
                        entry.total_output_tokens,
                        entry.total_credits,
                    ),
                    (13 * calls, 17 * calls, 0.25 * calls as f64),
                    "key={key_id}, {endpoint}, stream={streaming}"
                );
                assert_eq!(entry.total_cache_read_tokens, 3 * calls);
                assert_eq!(entry.total_cache_creation_tokens, 2 * calls);
            }
        }
    }
    assert_eq!(fixture.calls.load(Ordering::Relaxed), calls as usize);
    server.abort();
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn gateway_ungrouped_system_key_records_and_persists_usage() {
    assert_ungrouped_key_usage(true).await;
}

#[tokio::test]
async fn gateway_ungrouped_client_key_records_and_persists_usage() {
    assert_ungrouped_key_usage(false).await;
}

#[tokio::test]
async fn integration_native_end_turn_matches_streaming_and_non_streaming() {
    let fixture = fixture(Mode::ReviewEndTurn).await;
    for streaming in [true, false] {
        let response = request(fixture.state.clone(), "messages", streaming).await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("\"stop_reason\":\"end_turn\""), "streaming={streaming}: {body}");
    }
}

#[tokio::test]
async fn integration_cc_websearch_receives_custom_headers() {
    use crate::model::custom_headers::{CustomHeaderRule, CustomHeadersManager};
    let fixture = fixture(Mode::Normal(Duration::ZERO)).await;
    let state = fixture.state.clone().with_custom_headers(Some(CustomHeadersManager::new(vec![
        CustomHeaderRule { key: "X-Review-Trace".to_string(), value: "{trace_id}".to_string(), enabled: true },
    ])));
    for streaming in [true, false] {
        let payload = json!({"model":"gpt-5.6-sol","max_tokens":64,"stream":streaming,
            "messages":[{"role":"user","content":"search example"}],
            "tools":[{"type":"web_search_20250305","name":"web_search"}]});
        let response = post_messages_cc(axum::extract::State(state.clone()), Extension(key()), Json(serde_json::from_value(payload).unwrap())).await;
        assert!(response.headers().get("x-review-trace").is_some());
    }
}

#[tokio::test]
async fn integration_cached_model_alias_is_not_rejected_by_preflight() {
    let fixture = fixture(Mode::Normal(Duration::ZERO)).await;
    let manager = fixture.state.kiro_provider.as_ref().unwrap().token_manager();
    manager.set_test_models_fetcher(Arc::new(|_| Box::pin(async {
        Ok(serde_json::from_value(json!({"models": [{"modelId": "claude-sonnet-4.6"}]})).unwrap())
    })));
    manager.get_available_models_for(1).await.unwrap();
    let payload = json!({"model":"claude-sonnet-4-6-thinking", "max_tokens":64,
        "messages":[{"role":"user","content":"hello"}]});
    let response = post_messages(axum::extract::State(fixture.state.clone()), Extension(key()), Json(serde_json::from_value(payload).unwrap())).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(fixture.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn integration_no_credentials_after_upstream_failure_does_not_create_trace() {
    for streaming in [true, false] {
        let mut fixture = fixture(Mode::ReviewSuspended).await;
        let store = Arc::new(TraceStore::open_in_memory().unwrap());
        fixture.state = fixture.state.clone().with_trace_store(Some(store.clone()));
        let response = request(fixture.state.clone(), "messages", streaming).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let (_, total) = store.query_paged(&TraceQuery { limit: 10, ..Default::default() });
        assert_eq!(total, 0);
    }
}

#[tokio::test]
async fn integration_mixed_websearch_no_credentials_does_not_create_trace() {
    for streaming in [true, false] {
        let mut fixture = fixture(Mode::ReviewSuspended).await;
        let store = Arc::new(TraceStore::open_in_memory().unwrap());
        fixture.state = fixture.state.clone().with_trace_store(Some(store.clone()));
        let payload = json!({"model":"gpt-5.6-sol","max_tokens":64,"stream":streaming,
            "messages":[{"role":"user","content":"hello"}],
            "tools":[{"type":"web_search_20250305","name":"web_search"},
                {"name":"run","description":"run","input_schema":{"type":"object"}}]});
        let response = post_messages(axum::extract::State(fixture.state.clone()), Extension(key()), Json(serde_json::from_value(payload).unwrap())).await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("No available credentials"));
        let (_, total) = store.query_paged(&TraceQuery { limit: 10, ..Default::default() });
        assert_eq!(total, 0);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct LocalEndpoint(String);
impl KiroEndpoint for LocalEndpoint {
    fn name(&self) -> &'static str {
        "fixture"
    }
    fn api_url(&self, _: &RequestContext<'_>) -> String {
        self.0.clone()
    }
    fn mcp_url(&self, _: &RequestContext<'_>) -> String {
        self.0.clone()
    }
    fn decorate_api(
        &self,
        req: reqwest::RequestBuilder,
        _: &RequestContext<'_>,
    ) -> reqwest::RequestBuilder {
        req
    }
    fn decorate_mcp(
        &self,
        req: reqwest::RequestBuilder,
        _: &RequestContext<'_>,
    ) -> reqwest::RequestBuilder {
        req
    }
    fn transform_api_body(&self, body: &str, _: &RequestContext<'_>) -> String {
        body.to_owned()
    }
}

fn exception_frame(exception_type: &str, payload: &str) -> Bytes {
    let mut headers = Vec::new();
    for (name, value) in [(":message-type", "exception"), (":exception-type", exception_type)] {
        headers.push(name.len() as u8);
        headers.extend_from_slice(name.as_bytes());
        headers.push(7);
        headers.extend_from_slice(&(value.len() as u16).to_be_bytes());
        headers.extend_from_slice(value.as_bytes());
    }
    let mut out = Vec::new();
    out.extend_from_slice(&((16 + headers.len() + payload.len()) as u32).to_be_bytes());
    out.extend_from_slice(&(headers.len() as u32).to_be_bytes());
    out.extend_from_slice(&crc32(&out).to_be_bytes());
    out.extend(headers);
    out.extend_from_slice(payload.as_bytes());
    out.extend_from_slice(&crc32(&out).to_be_bytes());
    Bytes::from(out)
}

fn frame(event: &str, payload: serde_json::Value) -> Bytes {
    let mut headers = Vec::new();
    for (name, value) in [(":message-type", "event"), (":event-type", event)] {
        headers.push(name.len() as u8);
        headers.extend_from_slice(name.as_bytes());
        headers.push(7);
        headers.extend_from_slice(&(value.len() as u16).to_be_bytes());
        headers.extend_from_slice(value.as_bytes());
    }
    let payload = serde_json::to_vec(&payload).unwrap();
    let mut out = Vec::new();
    out.extend_from_slice(&((16 + headers.len() + payload.len()) as u32).to_be_bytes());
    out.extend_from_slice(&(headers.len() as u32).to_be_bytes());
    out.extend_from_slice(&crc32(&out).to_be_bytes());
    out.extend(headers);
    out.extend(payload);
    out.extend_from_slice(&crc32(&out).to_be_bytes());
    Bytes::from(out)
}

async fn fixture(mode: Mode) -> Fixture {
    fixture_with_config(mode, Config::default()).await
}

async fn fixture_with_config(mode: Mode, config: Config) -> Fixture {
    let calls = Arc::new(AtomicUsize::new(0));
    let connections = Arc::new(Mutex::new(HashSet::new()));
    let close_headers = Arc::new(AtomicUsize::new(0));
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let seen_bodies = bodies.clone();
    let release_tool_arguments = Arc::new(tokio::sync::Notify::new());
    let release_arguments = release_tool_arguments.clone();
    let (seen_calls, seen_connections, seen_close) =
        (calls.clone(), connections.clone(), close_headers.clone());
    let app = Router::new().route("/", post(move |ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, body: Bytes| {
        let (calls, connections, close) = (seen_calls.clone(), seen_connections.clone(), seen_close.clone());
        let seen_bodies = seen_bodies.clone();
        let release_arguments = release_arguments.clone();
        async move {
            calls.fetch_add(1, Ordering::Relaxed);
            if let Ok(value) = serde_json::from_slice(&body) { seen_bodies.lock().push(value); }
            connections.lock().insert(peer);
            if headers.get("connection").is_some_and(|v| v == "close") { close.fetch_add(1, Ordering::Relaxed); }
            if matches!(mode, Mode::Overloaded) {
                return Response::builder().status(500).body(Body::from(r#"{"message":"high load","reason":"MODEL_TEMPORARILY_UNAVAILABLE"}"#)).unwrap();
            }
            if matches!(mode, Mode::ReviewSuspended) {
                return Response::builder().status(403).body(Body::from(r#"{"reason":"TEMPORARILY_SUSPENDED"}"#)).unwrap();
            }
            if matches!(mode, Mode::SlowHeaders) { tokio::time::sleep(Duration::from_secs(4)).await; }
            if matches!(mode, Mode::SlowErrorBody) {
                return Response::builder().status(500).body(Body::from_stream(stream::pending::<Result<Bytes, Infallible>>())).unwrap();
            }
            if matches!(mode, Mode::Empty) { return Response::new(Body::empty()); }
            if let Mode::Reply(pieces) | Mode::ReplyWithReasoning(pieces) | Mode::ReplyWithCredits(pieces) = mode {
                let mut bytes = Vec::new();
                if matches!(mode, Mode::ReplyWithReasoning(_)) {
                    bytes.extend_from_slice(&frame("reasoningContentEvent", json!({"text":"Let me think about the goal."})));
                }
                for piece in pieces { bytes.extend_from_slice(&frame("assistantResponseEvent", json!({"content": piece}))); }
                bytes.extend_from_slice(&frame("metadataEvent", json!({"tokenUsage":{"uncachedInputTokens":13,"outputTokens":17,"cacheReadInputTokens":3,"cacheWriteInputTokens":2}})));
                if matches!(mode, Mode::ReplyWithCredits(_)) {
                    bytes.extend_from_slice(&frame("meteringEvent", json!({"unit":"credit","unitPlural":"credits","usage":0.25})));
                }
                return Response::new(Body::from(bytes));
            }
            if matches!(mode, Mode::Filtered) {
                let mut bytes = Vec::new();
                for part in [
                    frame("reasoningContentEvent", json!({"text":"I should summarize the result."})),
                    frame("metadataEvent", json!({"stopReason":"CONTENT_FILTERED","stopDetails":{"detail":"private provider detail"},"tokenUsage":{"uncachedInputTokens":13,"outputTokens":17,"cacheReadInputTokens":3,"cacheWriteInputTokens":2}})),
                ] { bytes.extend_from_slice(&part); }
                let chunks: Vec<_> = bytes.chunks(7).map(Bytes::copy_from_slice).collect();
                return Response::new(Body::from_stream(stream::iter(chunks.into_iter().map(Ok::<_, Infallible>))));
            }
            if matches!(mode, Mode::Large) {
                let frames = (0..18000).map(|_| frame("assistantResponseEvent", json!({"content": "x".repeat(1024)})));
                return Response::new(Body::from_stream(stream::iter(frames.map(Ok::<_, Infallible>))));
            }
            if matches!(mode, Mode::Truncated | Mode::InvalidFrame) {
                let mut bytes = frame("assistantResponseEvent", json!({"content": "damaged"})).to_vec();
                if matches!(mode, Mode::Truncated) { bytes.truncate(bytes.len() - 4); }
                else { let last = bytes.len() - 1; bytes[last] ^= 1; }
                return Response::new(Body::from(bytes));
            }
            if matches!(mode, Mode::Throttled) {
                let mut bytes = frame("assistantResponseEvent", json!({"content": "partial answer"})).to_vec();
                bytes.extend_from_slice(&exception_frame("ThrottlingException", r#"{"message":"Rate exceeded for account 123456789012"}"#));
                return Response::new(Body::from(bytes));
            }
            if matches!(mode, Mode::SearchInvalidToolJson) {
                let bytes = frame("toolUseEvent", json!({"name":"run","toolUseId":"run_1","input":"{\"command\":\"ls","stop":true})).to_vec();
                return Response::new(Body::from(bytes));
            }
            if matches!(mode, Mode::ThinkingText) {
                let mut bytes = frame("assistantResponseEvent", json!({"content": "<thinking>先想想</thinking>\n\n答案"})).to_vec();
                bytes.extend_from_slice(&frame("metadataEvent", json!({"tokenUsage":{"uncachedInputTokens":5,"outputTokens":3,"cacheReadInputTokens":0,"cacheWriteInputTokens":0}})));
                return Response::new(Body::from(bytes));
            }
            if matches!(mode, Mode::ReviewRepetition | Mode::ReviewEndTurn) {
                let content = if matches!(mode, Mode::ReviewRepetition) { "count\n".repeat(40) } else { "complete".to_string() };
                let mut bytes = frame("assistantResponseEvent", json!({"content": content})).to_vec();
                bytes.extend_from_slice(&frame("metadataEvent", json!({"stopReason": "END_TURN"})));
                if matches!(mode, Mode::ReviewEndTurn) {
                    bytes.extend_from_slice(&frame("contextUsageEvent", json!({"contextUsagePercentage": 100.0})));
                }
                return Response::new(Body::from(bytes));
            }
            if matches!(mode, Mode::GatedChatText) {
                let first = frame("assistantResponseEvent", json!({"content": chat_stream_tests::first_text()}));
                let second = frame("assistantResponseEvent", json!({"content": chat_stream_tests::second_text()}));
                let metadata = frame("metadataEvent", json!({"tokenUsage": {
                    "uncachedInputTokens":13,"outputTokens":17,"cacheReadInputTokens":3,"cacheWriteInputTokens":2
                }}));
                let second_gate = release_arguments.clone();
                let chunks = stream::once(async move { Ok::<_, Infallible>(first) })
                    .chain(stream::once(async move {
                        second_gate.notified().await;
                        Ok::<_, Infallible>(second)
                    }))
                    .chain(stream::once(async move {
                        release_arguments.notified().await;
                        Ok::<_, Infallible>(metadata)
                    }));
                return Response::new(Body::from_stream(chunks));
            }
            if matches!(mode, Mode::GatedToolArguments) {
                let first = frame("toolUseEvent", json!({
                    "name":"Write", "toolUseId":"call_1",
                    "input":"{\"file_path\":\"/not-created\",\"content\":\"first", "stop":false
                }));
                let last = frame("toolUseEvent", json!({
                    "name":"Write", "toolUseId":"call_1", "input":" second\"}", "stop":true
                }));
                let chunks = stream::once(async move { Ok::<_, Infallible>(first) })
                    .chain(stream::once(async move {
                        release_arguments.notified().await;
                        Ok::<_, Infallible>(last)
                    }));
                return Response::new(Body::from_stream(chunks));
            }
            if matches!(mode, Mode::Tools)
                || (matches!(mode, Mode::CompactionRetry) && calls.load(Ordering::Relaxed) == 1)
            {
                let mut bytes = Vec::new();
                for part in [
                    frame("toolUseEvent", json!({"name":"run", "toolUseId":"call_1", "input":"{\"command\":", "stop":false})),
                    frame("toolUseEvent", json!({"name":"run", "toolUseId":"call_1", "input":"\"hello\"}", "stop":true})),
                    frame("metadataEvent", json!({"tokenUsage":{"uncachedInputTokens":13,"outputTokens":17,"cacheReadInputTokens":3,"cacheWriteInputTokens":2}})),
                ] { bytes.extend_from_slice(&part); }
                return Response::new(Body::from(bytes));
            }
            if let Mode::Cadence { events, every } = mode {
                let chunks = stream::unfold(0, move |step| async move {
                    if step > events { return None; }
                    if step > 0 { tokio::time::sleep(every).await; }
                    let chunk = if step == events {
                        frame("metadataEvent", json!({"tokenUsage":{"uncachedInputTokens":13,"outputTokens":17,"cacheReadInputTokens":3,"cacheWriteInputTokens":2}}))
                    } else {
                        frame("assistantResponseEvent", json!({"content": format!("{CADENCE_TEXT}{step} ")}))
                    };
                    Some((Ok::<_, Infallible>(chunk), step + 1))
                });
                return Response::new(Body::from_stream(chunks));
            }
            if matches!(mode, Mode::NoProviderUsage) {
                return Response::new(Body::from(frame("assistantResponseEvent", json!({"content": "ok"}))));
            }
            if matches!(mode, Mode::Refused | Mode::RefusedWithSearch | Mode::RefusedWithPartialTool | Mode::RefusedWithInvalidTool | Mode::RefusedWithEmptyTool | Mode::RefusedBare) {
                let mut bytes = Vec::new();
                if matches!(mode, Mode::RefusedWithSearch) {
                    bytes.extend_from_slice(&frame("toolUseEvent", json!({"name":"web_search","toolUseId":"search_1","input":"{\"query\":\"example\"}","stop":true})));
                }
                if matches!(mode, Mode::RefusedWithPartialTool) {
                    bytes.extend_from_slice(&frame("toolUseEvent", json!({"name":"run","toolUseId":"run_1","input":"{\"command\":","stop":false})));
                }
                if matches!(mode, Mode::RefusedWithInvalidTool) {
                    bytes.extend_from_slice(&frame("toolUseEvent", json!({"name":"run","toolUseId":"run_1","input":"{\"command\":","stop":true})));
                }
                if matches!(mode, Mode::RefusedWithEmptyTool) {
                    bytes.extend_from_slice(&frame("toolUseEvent", json!({"name":"run","toolUseId":"run_1","input":"","stop":false})));
                }
                if matches!(mode, Mode::RefusedBare) {
                    bytes.extend_from_slice(&frame("metadataEvent", json!({"stopReason":"CONTENT_FILTERED"})));
                    return Response::new(Body::from(bytes));
                }
                for part in [
                    frame("reasoningContentEvent", json!({"text":"The"})),
                    frame("metadataEvent", json!({"stopReason":"CONTENT_FILTERED","stopDetails":{"refusal":{"category":"CYBER","explanation":"The selected model cannot continue this conversation."}}})),
                    frame("contextUsageEvent", json!({"contextUsagePercentage":100.0})),
                ] { bytes.extend_from_slice(&part); }
                return Response::new(Body::from(bytes));
            }
            let first = frame("assistantResponseEvent", json!({"content": "early content ".repeat(64)}));
            let metadata = frame("metadataEvent", json!({"tokenUsage": {"uncachedInputTokens": 13, "outputTokens": 17, "cacheReadInputTokens": 3, "cacheWriteInputTokens": 2}}));
            let chunks = stream::unfold(0, move |step| {
                let first = first.clone(); let metadata = metadata.clone();
                async move {
                    match step {
                        0 => { tokio::time::sleep(Duration::from_millis(20)).await; Some((Ok::<_, std::io::Error>(if matches!(mode, Mode::MetadataFirst) { metadata } else { first }), 1)) }
                        1 if matches!(mode, Mode::MetadataFirst) => { tokio::time::sleep(Duration::from_millis(100)).await; Some((Ok(first), 2)) }
                        1..=5 if matches!(mode, Mode::Active) => { tokio::time::sleep(Duration::from_millis(350)).await; Some((Ok(if step == 5 { metadata } else { first }), step + 1)) }
                        6 if matches!(mode, Mode::Active) => None,
                        1 if matches!(mode, Mode::Interrupted) => { tokio::time::sleep(Duration::from_millis(50)).await; Some((Err(std::io::Error::other("fixture disconnected")), 2)) }
                        1 if matches!(mode, Mode::Stalled) => futures::future::pending().await,
                        1 => { if let Mode::Normal(gap) = mode { tokio::time::sleep(gap).await; } Some((Ok(metadata), 2)) }
                        _ => None,
                    }
                }
            });
            Response::new(Body::from_stream(chunks))
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let creds: KiroCredentials = serde_json::from_value(json!({"id": 1, "kiroApiKey": "fixture-only", "machineId": "0000000000000000000000000000000000000000000000000000000000000000"})).unwrap();
    let manager = Arc::new(MultiTokenManager::new(config, vec![creds], None, None, false).unwrap());
    let endpoints = HashMap::from([(
        "fixture".to_owned(),
        Arc::new(LocalEndpoint(url)) as Arc<dyn KiroEndpoint>,
    )]);
    let provider = KiroProvider::new(manager, endpoints, "fixture".to_owned());
    let state = AppState::new(false, ToolCompatibilityMode::Raw)
        .with_shared_kiro_provider(Arc::new(provider));
    Fixture {
        state,
        calls,
        connections,
        close_headers,
        bodies,
        release_tool_arguments,
        task,
    }
}

fn key() -> KeyContext {
    KeyContext::master()
}
async fn request(state: AppState, endpoint: &str, streaming: bool) -> Response {
    request_with_key(state, endpoint, streaming, key()).await
}

async fn request_with_key(state: AppState, endpoint: &str, streaming: bool, key_ctx: KeyContext) -> Response {
    let req = json!({"model": "gpt-5.6-sol", "max_tokens": 64, "stream": streaming, "messages": [{"role": "user", "content": "hello"}], "input": [{"role":"user", "content":"hello"}]});
    if endpoint == "chat" {
        post_chat_completions(
            axum::extract::State(state),
            Extension(key_ctx),
            HeaderMap::new(),
            Json(serde_json::from_value(req).unwrap()),
        )
        .await
    } else if endpoint == "responses" {
        post_responses(
            axum::extract::State(state),
            Extension(key_ctx),
            HeaderMap::new(),
            Json(serde_json::from_value(req).unwrap()),
        )
        .await
    } else if endpoint == "cc" {
        post_messages_cc(
            axum::extract::State(state),
            Extension(key_ctx),
            Json(serde_json::from_value(req).unwrap()),
        )
        .await
    } else {
        post_messages(
            axum::extract::State(state),
            Extension(key_ctx),
            Json(serde_json::from_value(req).unwrap()),
        )
        .await
    }
}

async fn first_content(
    body: &mut (impl futures::Stream<Item = Result<Bytes, axum::Error>> + Unpin),
) {
    while let Some(chunk) = body.next().await {
        if String::from_utf8_lossy(&chunk.unwrap()).contains("early content") {
            return;
        }
    }
    panic!("stream ended without content");
}

#[tokio::test]
async fn effective_call_billing_counts_completed_requests_across_protocols() {
    use crate::admin::client_keys::{BillingMode, ClientKeyManager, KeyAuth};

    for endpoint in ["messages", "cc", "chat", "responses"] {
        for streaming in [false, true] {
            let fixture = fixture(Mode::Normal(Duration::ZERO)).await;
            let manager = Arc::new(ClientKeyManager::new());
            let entry = manager.create("billable".into(), None, None);
            manager.update_meta(entry.id, None, None, None, Some((BillingMode::Calls, Some(1))));
            let state = fixture.state.clone().with_usage(Some(manager.clone()), None, None);
            assert_eq!(manager.verify_and_touch_ex(&entry.key), KeyAuth::Ok(entry.id));
            let key_ctx = KeyContext { key_id: entry.id, key_source: TraceKeySource::ClientKey, ..key() };
            let response = request_with_key(state, endpoint, streaming, key_ctx).await;
            assert_eq!(response.status(), StatusCode::OK, "{endpoint} streaming={streaming}");
            to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let snapshot = manager.list().remove(0);
            assert_eq!(snapshot.total_successful_calls, 1, "{endpoint} streaming={streaming}");
            assert_eq!(snapshot.total_input_tokens, 13);
            assert_eq!(snapshot.total_output_tokens, 17);
            assert_eq!(manager.verify_and_touch_ex(&entry.key), KeyAuth::CallsOverLimit {
                id: entry.id, used: 1, limit: 1,
            });
        }
    }
}

#[tokio::test]
async fn effective_call_billing_does_not_charge_failed_or_interrupted_requests() {
    use crate::admin::client_keys::ClientKeyManager;

    for endpoint in ["messages", "cc", "chat", "responses"] {
        for mode in [Mode::Overloaded, Mode::Interrupted, Mode::Empty, Mode::InvalidFrame] {
            for streaming in [false, true] {
                let fixture = fixture(mode).await;
                let manager = Arc::new(ClientKeyManager::new());
                let entry = manager.create("billable".into(), None, None);
                let state = fixture.state.clone().with_usage(Some(manager.clone()), None, None);
                let key_ctx = KeyContext { key_id: entry.id, key_source: TraceKeySource::ClientKey, ..key() };
                let response = request_with_key(state, endpoint, streaming, key_ctx).await;
                let status = response.status();
                let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
                assert!(status.is_client_error() || status.is_server_error() || String::from_utf8_lossy(&body).contains("error"));
                assert_eq!(manager.list()[0].total_successful_calls, 0, "{endpoint} streaming={streaming}");
            }
        }
    }
}

#[tokio::test]
async fn effective_call_billing_does_not_charge_cancelled_streams() {
    use crate::admin::client_keys::ClientKeyManager;

    for endpoint in ["messages", "cc", "chat", "responses"] {
        let fixture = fixture(Mode::Stalled).await;
        let manager = Arc::new(ClientKeyManager::new());
        let entry = manager.create("billable".into(), None, None);
        let state = fixture.state.clone().with_usage(Some(manager.clone()), None, None);
        let key_ctx = KeyContext { key_id: entry.id, key_source: TraceKeySource::ClientKey, ..key() };
        let mut body = request_with_key(state, endpoint, true, key_ctx).await.into_body().into_data_stream();
        tokio::time::timeout(Duration::from_secs(2), first_content(&mut body)).await.unwrap();
        drop(body);
        assert_eq!(manager.list()[0].total_successful_calls, 0, "{endpoint}");
    }
}

#[tokio::test]
async fn effective_call_billing_counts_system_key() {
    use crate::admin::client_keys::ClientKeyManager;

    let fixture = fixture(Mode::Normal(Duration::ZERO)).await;
    let manager = Arc::new(ClientKeyManager::new());
    manager.sync_system_key("system".into(), None, "fixture-system-key".into());
    let state = fixture.state.clone().with_usage(Some(manager.clone()), None, None);
    let response = request(state, "messages", false).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(manager.list()[0].total_successful_calls, 1);
}

#[tokio::test]
async fn effective_call_billing_http_quota_and_non_generation_routes() {
    use crate::admin::client_keys::{BillingMode, ClientKeyManager};
    use super::middleware::auth_middleware;

    let fixture = fixture(Mode::Normal(Duration::ZERO)).await;
    let manager = Arc::new(ClientKeyManager::new());
    let entry = manager.create("billable".into(), None, None);
    manager.update_meta(entry.id, None, None, None, Some((BillingMode::Calls, Some(1))));
    let state = fixture.state.clone().with_usage(Some(manager.clone()), None, None);
    let app = Router::new()
        .route("/messages", post(post_messages))
        .route("/count_tokens", post(super::handlers::count_tokens))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth_middleware))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let payload = json!({"model": "gpt-5.6-sol", "max_tokens": 64, "messages": [{"role": "user", "content": "hello"}]});
    let response = client.post(format!("{base}/count_tokens")).header("x-api-key", &entry.key).json(&payload).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(manager.list()[0].total_successful_calls, 0);
    let response = client.post(format!("{base}/messages")).header("x-api-key", &entry.key).json(&json!({})).send().await.unwrap();
    assert!(response.status().is_client_error());
    assert_eq!(manager.list()[0].total_successful_calls, 0);
    let response = client.post(format!("{base}/messages")).header("x-api-key", &entry.key).json(&payload).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.bytes().await.unwrap();
    let response = client.post(format!("{base}/messages")).header("x-api-key", &entry.key).json(&payload).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let error: serde_json::Value = response.json().await.unwrap();
    assert_eq!(error["error"]["type"], "rate_limit_error");
    assert!(error["error"]["message"].as_str().unwrap().contains("有效调用次数"));
    assert_eq!(fixture.calls.load(Ordering::Relaxed), 1);
    assert_eq!(manager.list()[0].total_successful_calls, 1);
    task.abort();
}

#[tokio::test]
async fn effective_call_billing_compaction_counts_final_success_not_internal_rounds() {
    use crate::admin::client_keys::ClientKeyManager;

    for streaming in [false, true] {
        for (mode, expected_calls) in [(Mode::CompactionRetry, 1), (Mode::Tools, 0)] {
            let fixture = fixture(mode).await;
            let manager = Arc::new(ClientKeyManager::new());
            let entry = manager.create("billable".into(), None, None);
            let state = fixture.state.clone().with_usage(Some(manager.clone()), None, None);
            let key_ctx = KeyContext { key_id: entry.id, key_source: TraceKeySource::ClientKey, ..key() };
            let payload = json!({"model": "gpt-5.6-sol", "stream": streaming, "input": [
                {"role": "user", "content": "Summarize our work"}, {"type": "compaction_trigger"}
            ]});
            let response = post_responses(axum::extract::State(state), Extension(key_ctx), HeaderMap::new(), Json(serde_json::from_value(payload).unwrap())).await;
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let body = String::from_utf8_lossy(&body);
            assert!(body.contains(if expected_calls == 1 { "completed" } else { "compaction_error" }), "{body}");
            assert_eq!(fixture.calls.load(Ordering::Relaxed), 2);
            assert_eq!(manager.list()[0].total_successful_calls, expected_calls);
            assert_eq!(manager.list()[0].total_input_tokens, 26);
        }
    }
}

#[tokio::test]
async fn gateway_raw_tool_arguments_arrive_before_upstream_finishes() {
    let fixture = fixture(Mode::GatedToolArguments).await;
    let payload = json!({
        "model":"gpt-5.6-sol", "max_tokens":64, "stream":true,
        "messages":[{"role":"user", "content":"Write the requested content."}],
        "tools":[{
            "name":"Write", "description":"Write exactly the supplied content.",
            "input_schema":{
                "type":"object",
                "properties":{"file_path":{"type":"string"},"content":{"type":"string"}},
                "required":["file_path","content"]
            }
        }]
    });
    let response = post_messages(
        axum::extract::State(fixture.state.clone()),
        Extension(key()),
        Json(serde_json::from_value(payload).unwrap()),
    ).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let mut received = String::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(chunk) = body.next().await {
            received.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
            if received.contains("input_json_delta") {
                return;
            }
        }
        panic!("stream ended before the first tool argument fragment");
    }).await.expect("tool arguments were buffered while upstream waited for the client");
    let early_events = parse_sse(&received);
    let start = early_events.iter().find(|(_, event)| event["content_block"]["type"] == "tool_use").unwrap();
    assert_eq!(start.1["content_block"]["name"], "Write");
    assert_eq!(start.1["content_block"]["id"], "call_1");
    let tool_index = start.1["index"].clone();
    assert!(!early_events.iter().any(|(kind, event)| kind == "content_block_stop" && event["index"] == tool_index));
    assert!(!received.contains("message_stop"));
    fixture.release_tool_arguments.notify_one();
    while let Some(chunk) = body.next().await {
        received.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    let events = parse_sse(&received);
    let fragments: Vec<_> = events.iter()
        .filter(|(_, event)| event["delta"]["type"] == "input_json_delta")
        .map(|(_, event)| event["delta"]["partial_json"].as_str().unwrap())
        .collect();
    assert_eq!(fragments.len(), 2);
    let input: serde_json::Value = serde_json::from_str(&fragments.concat()).unwrap();
    assert_eq!(input, json!({"file_path":"/not-created","content":"first second"}));
    assert_eq!(events.iter().filter(|(kind, event)| kind == "content_block_stop" && event["index"] == tool_index).count(), 1);
    assert!(events.iter().any(|(kind, event)| kind == "message_delta" && event["delta"]["stop_reason"] == "tool_use"));
    assert_eq!(events.last().unwrap().0, "message_stop");
}

#[tokio::test]
async fn gateway_cc_emits_content_before_upstream_finishes() {
    let f = fixture(Mode::Normal(Duration::from_millis(400))).await;
    let mut body = request(f.state.clone(), "cc", true)
        .await
        .into_body()
        .into_data_stream();
    tokio::time::timeout(Duration::from_millis(250), first_content(&mut body))
        .await
        .expect("CC buffered content until EOF");
    let mut rest = String::new();
    while let Some(chunk) = body.next().await {
        rest.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    assert!(rest.contains("\"input_tokens\":13"));
    assert!(rest.contains("\"output_tokens\":17"));
}

#[tokio::test]
async fn gateway_chat_emits_content_before_upstream_finishes() {
    let f = fixture(Mode::Normal(Duration::from_millis(400))).await;
    let started = Instant::now();
    let response = request(f.state.clone(), "chat", true).await;
    let mut body = response.into_body().into_data_stream();
    first_content(&mut body).await;
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "Chat buffered content until EOF"
    );
}

#[tokio::test]
async fn gateway_cc_disconnect_is_an_error_not_message_stop() {
    let f = fixture(Mode::Interrupted).await;
    let response = request(f.state.clone(), "cc", true).await;
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(
        text.contains("event: error"),
        "disconnect was hidden: {text}"
    );
    assert!(!text.contains("event: message_stop"));
}

#[tokio::test]
async fn gateway_reuses_upstream_connection() {
    let f = fixture(Mode::Normal(Duration::ZERO)).await;
    for _ in 0..3 {
        let response = request(f.state.clone(), "messages", false).await;
        assert_eq!(response.status(), StatusCode::OK);
        to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    }
    assert_eq!(f.close_headers.load(Ordering::Relaxed), 0);
    assert_eq!(f.connections.lock().len(), 1);
}

#[tokio::test]
async fn gateway_model_overload_does_not_retry_three_times() {
    let f = fixture(Mode::Overloaded).await;
    let response = request(f.state.clone(), "cc", true).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().contains_key("retry-after"));
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("overloaded_error"));
    assert_eq!(f.calls.load(Ordering::Relaxed), 1);
    // 不做模型级冷却：一次过载只影响本次请求，下一个请求照常发往上游。
    let second = request(f.state.clone(), "cc", true).await;
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        f.calls.load(Ordering::Relaxed),
        2,
        "model overload must not block later requests without trying upstream"
    );
}

#[tokio::test]
async fn gateway_non_stream_decodes_responses_larger_than_decoder_buffer() {
    let f = fixture(Mode::Large).await;
    let response = request(f.state.clone(), "messages", false).await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 32 * 1024 * 1024)
        .await
        .unwrap();
    let response: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        response["content"][0]["text"].as_str().unwrap().len(),
        18000 * 1024
    );
}

#[tokio::test]
async fn gateway_damaged_frames_are_errors_not_success() {
    for mode in [Mode::Truncated, Mode::InvalidFrame, Mode::Empty] {
        let f = fixture(mode).await;
        for endpoint in ["cc", "messages"] {
            let response = request(f.state.clone(), endpoint, true).await;
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let text = String::from_utf8_lossy(&bytes);
            assert!(
                text.contains("event: error"),
                "corrupt stream was accepted: {text}"
            );
            assert!(!text.contains("event: message_stop"));
        }
        let response = request(f.state.clone(), "messages", false).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }
}

#[tokio::test]
async fn gateway_content_filter_is_reported_instead_of_output_limit_or_success() {
    // 只有 reasoning + CONTENT_FILTERED（带 tokenUsage、无 refusal 详情）：
    // 必须报告为拒答，不得伪造 max_tokens / 成功结束，也不泄露上游私有字段。
    let f = fixture(Mode::Filtered).await;
    for (endpoint, streaming, expected) in [
        ("messages", false, r#""stop_reason":"refusal""#),
        ("messages", true, r#""stop_reason":"refusal""#),
        ("cc", true, r#""stop_reason":"refusal""#),
        ("chat", true, r#""finish_reason":"content_filter""#),
        ("responses", true, r#""reason":"content_filter""#),
    ] {
        let response = request(f.state.clone(), endpoint, streaming).await;
        assert_eq!(response.status(), StatusCode::OK, "{endpoint}");
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains(expected), "{endpoint}: {body}");
        assert!(!body.contains("max_tokens"), "{endpoint}: {body}");
        assert!(!body.contains("model_context_window_exceeded"), "{endpoint}: {body}");
        assert!(!body.contains("private provider detail"), "{endpoint}: {body}");
    }
}

#[tokio::test]
async fn gateway_chat_disconnect_has_no_success_finish_or_done() {
    let f = fixture(Mode::Interrupted).await;
    let response = request(f.state.clone(), "chat", true).await;
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(text.contains("\"error\""));
    assert!(!text.contains("[DONE]"));
    assert!(!text.contains("\"finish_reason\":\"stop\""));
}

#[tokio::test]
async fn gateway_chat_preserves_overload_retry_after() {
    let f = fixture(Mode::Overloaded).await;
    let response = request(f.state.clone(), "chat", true).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get("retry-after").unwrap(), "5");
}

#[tokio::test]
async fn gateway_responses_preserves_overload_retry_after() {
    let f = fixture(Mode::Overloaded).await;
    let response = request(f.state.clone(), "responses", true).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get("retry-after").unwrap(), "5");
}

#[tokio::test]
async fn gateway_chat_tool_arguments_and_final_usage_are_complete() {
    let f = fixture(Mode::Tools).await;
    let response = request(f.state.clone(), "chat", true).await;
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let text = String::from_utf8_lossy(&bytes);
    let chunks: Vec<serde_json::Value> = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|line| *line != "[DONE]")
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let mut arguments = String::new();
    for chunk in &chunks {
        if let Some(args) = chunk
            .pointer("/choices/0/delta/tool_calls/0/function/arguments")
            .and_then(|v| v.as_str())
        {
            arguments.push_str(args);
        }
    }
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&arguments).unwrap(),
        json!({"command":"hello"})
    );
    assert!(
        chunks
            .iter()
            .any(|chunk| chunk.pointer("/choices/0/finish_reason") == Some(&json!("tool_calls")))
    );
    let usage = &chunks.last().unwrap()["usage"];
    assert_eq!(usage["prompt_tokens"], 18);
    assert_eq!(usage["completion_tokens"], 17);
    assert!(text.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn gateway_content_filter_refusal_is_not_a_successful_end_turn() {
    let f = fixture(Mode::Refused).await;
    async fn text(response: Response) -> String {
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    let response = request(f.state.clone(), "messages", false).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&text(response).await).unwrap();
    assert_eq!(body["stop_reason"], "refusal", "{body}");

    for endpoint in ["cc", "messages"] {
        let sse = text(request(f.state.clone(), endpoint, true).await).await;
        assert!(sse.contains(r#""stop_reason":"refusal""#), "{endpoint}: {sse}");
        assert!(!sse.contains("model_context_window_exceeded"), "{endpoint}: {sse}");
    }

    let chat = text(request(f.state.clone(), "chat", true).await).await;
    assert!(chat.contains(r#""finish_reason":"content_filter""#), "{chat}");
    assert!(!chat.contains(r#""finish_reason":"stop""#), "{chat}");

    let responses = text(request(f.state.clone(), "responses", true).await).await;
    assert!(responses.contains("event: response.incomplete"), "{responses}");
    assert!(responses.contains(r#""reason":"content_filter""#), "{responses}");
}

#[tokio::test]
async fn gateway_refusal_nonstream_compatibility_and_bare_metadata() {
    for mode in [Mode::Refused, Mode::RefusedBare] {
        let f = fixture(mode).await;
        for (endpoint, field, expected) in [
            ("messages", "/stop_reason", "refusal"),
            ("chat", "/choices/0/finish_reason", "content_filter"),
            ("responses", "/incomplete_details/reason", "content_filter"),
        ] {
            let response = request(f.state.clone(), endpoint, false).await;
            let status = response.status();
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(status, StatusCode::OK, "{endpoint}: {body}");
            assert_eq!(
                body.pointer(field).and_then(|v| v.as_str()),
                Some(expected),
                "{endpoint}: {body}"
            );
        }
    }
}

#[tokio::test]
async fn gateway_refused_round_does_not_execute_search() {
    let mut failures = Vec::new();
    for streaming in [false, true] {
        let f = fixture(Mode::RefusedWithSearch).await;
        let payload = json!({
            "model":"gpt-5.6-sol","max_tokens":64,"stream":streaming,
            "messages":[{"role":"user","content":"hello"}],
            "tools":[
                {"type":"web_search_20250305","name":"web_search"},
                {"name":"run","description":"test command","input_schema":{"type":"object","properties":{"command":{"type":"string"}}}}
            ]
        });
        let response = post_messages(
            axum::extract::State(f.state.clone()),
            Extension(key()),
            Json(serde_json::from_value(payload).unwrap()),
        )
        .await;
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes);
        let calls = f.calls.load(Ordering::Relaxed);
        if status != StatusCode::OK
            || calls != 1
            || !body.contains("\"refusal\"")
            || body.contains("server_tool_use")
            || body.contains("web_search_tool_result")
        {
            failures.push(format!(
                "stream={streaming}, calls={calls}, status={status}, body={body}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn gateway_partial_tool_before_refusal_preserves_terminal_reason() {
    let mut failures = Vec::new();
    for mode in [
        Mode::RefusedWithPartialTool,
        Mode::RefusedWithInvalidTool,
        Mode::RefusedWithEmptyTool,
    ] {
        for streaming in [false, true] {
            let f = fixture(mode).await;
            let payload = json!({
                "model":"gpt-5.6-sol","max_tokens":64,"stream":streaming,
                "messages":[{"role":"user","content":"hello"}],
                "tools":[{"name":"run","description":"test command","input_schema":{"type":"object","properties":{"command":{"type":"string"}}}}]
            });
            let response = post_messages(
                axum::extract::State(f.state.clone()),
                Extension(key()),
                Json(serde_json::from_value(payload).unwrap()),
            )
            .await;
            let status = response.status();
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let body = String::from_utf8_lossy(&bytes);
            if streaming && matches!(mode, Mode::RefusedWithPartialTool) {
                let events = parse_sse(&body);
                assert_eq!(status, StatusCode::OK);
                assert_eq!(events.last().unwrap().1["error"]["type"], "permission_error");
                assert!(body.contains("refusal"));
                assert!(!body.contains("message_stop"));
                assert!(!body.contains("upstream_tool_json_error"));
                continue;
            }
            if status != StatusCode::OK
                || !body.contains("\"refusal\"")
                || body.contains("upstream_tool_json_error")
                || body.contains("\"type\":\"tool_use\"")
            {
                failures.push(format!("stream={streaming}, status={status}, body={body}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn gateway_stalled_body_times_out_without_normal_completion() {
    let config: Config = serde_json::from_value(json!({"upstreamReadTimeoutSecs":1})).unwrap();
    let f = fixture_with_config(Mode::Stalled, config).await;
    let response = request(f.state.clone(), "cc", true).await;
    let bytes = tokio::time::timeout(
        Duration::from_secs(2),
        to_bytes(response.into_body(), 1024 * 1024),
    )
    .await
    .expect("idle stream did not time out")
    .unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(text.contains("event: error"));
    assert!(!text.contains("event: message_stop"));
}

#[tokio::test]
async fn gateway_header_timeout_does_not_repeat_long_waits() {
    let config: Config = serde_json::from_value(json!({"upstreamReadTimeoutSecs":1})).unwrap();
    let f = fixture_with_config(Mode::SlowHeaders, config).await;
    let response =
        tokio::time::timeout(Duration::from_secs(2), request(f.state.clone(), "cc", true))
            .await
            .expect("header deadline was ignored or retried");
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(f.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn gateway_error_body_timeout_does_not_repeat_long_waits() {
    let config: Config = serde_json::from_value(json!({"upstreamReadTimeoutSecs":1})).unwrap();
    let f = fixture_with_config(Mode::SlowErrorBody, config).await;
    let response =
        tokio::time::timeout(Duration::from_secs(2), request(f.state.clone(), "cc", true))
            .await
            .expect("error response body deadline was ignored or retried");
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(f.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn gateway_first_token_trace_ignores_metadata() {
    let mut f = fixture(Mode::MetadataFirst).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    f.state = f.state.clone().with_trace_store(Some(store.clone()));
    let response = request(f.state.clone(), "cc", true).await;
    to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let (records, count) = store.query_paged(&TraceQuery {
        limit: 10,
        ..Default::default()
    });
    assert_eq!(count, 1);
    assert!(
        records[0].first_token_ms.unwrap() >= 90,
        "metadata was counted as the first token: {:?}",
        records[0].first_token_ms
    );
}

#[tokio::test]
async fn gateway_local_rpm_limit_is_traced_apart_from_upstream_429() {
    // 2026-10-30 线上：单账号 RPM 打满后网关自己回的 429 记成 transient / "upstream rate limited"、
    // 凭据 0，看起来像上游限流（8 小时 1101 次，同期上游真正的 429 只有 38 次）。
    let config: Config =
        serde_json::from_value(json!({"accountRpmLimitEnabled": true, "accountRpmLimit": 1}))
            .unwrap();
    let mut f = fixture_with_config(Mode::Normal(Duration::ZERO), config).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    f.state = f.state.clone().with_trace_store(Some(store.clone()));

    let first = request(f.state.clone(), "messages", false).await;
    assert_eq!(first.status(), StatusCode::OK);
    to_bytes(first.into_body(), 1024 * 1024).await.unwrap();

    let second = request(f.state.clone(), "messages", false).await;
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(second.headers().get("retry-after").is_some());
    let body = to_bytes(second.into_body(), 1024 * 1024).await.unwrap();
    assert!(String::from_utf8_lossy(&body).contains("Gateway per-account rate limit"));
    assert_eq!(f.calls.load(Ordering::Relaxed), 1, "被本地限流的请求不应发到上游");

    let mut rejected = Vec::new();
    for _ in 0..50 {
        let (records, _) = store.query_paged(&TraceQuery {
            limit: 10,
            ..Default::default()
        });
        rejected = records
            .into_iter()
            .filter(|r| r.final_status != "success")
            .collect();
        if !rejected.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].error_type.as_deref(), Some("local_rate_limited"));
    assert!(
        rejected[0]
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("local per-account RPM limit"),
        "{:?}",
        rejected[0].error_message
    );
}

#[tokio::test]
async fn gateway_active_stream_can_exceed_idle_deadline() {
    let config: Config = serde_json::from_value(json!({"upstreamReadTimeoutSecs":1})).unwrap();
    let f = fixture_with_config(Mode::Active, config).await;
    let started = Instant::now();
    let response = request(f.state.clone(), "cc", true).await;
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(started.elapsed() > Duration::from_secs(1));
    assert!(text.contains("event: message_stop"));
    assert!(!text.contains("event: error"));
}

#[tokio::test]
#[ignore = "controlled latency measurement; run with --ignored --nocapture"]
async fn gateway_latency_measurement() {
    for endpoint in ["cc", "chat", "messages"] {
        let f = fixture(Mode::Normal(Duration::from_millis(200))).await;
        let mut first = Vec::new();
        let mut total = Vec::new();
        for _ in 0..10 {
            let start = Instant::now();
            let mut body = request(f.state.clone(), endpoint, true)
                .await
                .into_body()
                .into_data_stream();
            first_content(&mut body).await;
            first.push(start.elapsed().as_secs_f64() * 1000.0);
            while let Some(chunk) = body.next().await {
                chunk.unwrap();
            }
            total.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        first.sort_by(f64::total_cmp);
        total.sort_by(f64::total_cmp);
        println!(
            "LATENCY endpoint={endpoint} first_p50_ms={:.2} first_p95_ms={:.2} total_p50_ms={:.2} connections={} requests={}",
            first[5],
            first[9],
            total[5],
            f.connections.lock().len(),
            f.calls.load(Ordering::Relaxed)
        );
    }
}

const CADENCE_TEXT: &str = "cadence-";
const ROUTED_KEY: &str = "fixture-system-key";
const ROUTED_APIS: [&str; 4] = [
    "/v1/messages",
    "/cc/v1/messages",
    "/v1/chat/completions",
    "/v1/responses",
];

/// The production router on a real socket, in front of the upstream fixture.
struct Routed {
    address: SocketAddr,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Routed {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// One request body that every routed API accepts.
fn routed_payload(streaming: bool) -> serde_json::Value {
    json!({
        "model": "gpt-5.6-sol",
        "max_tokens": 64,
        "stream": streaming,
        "messages": [{"role": "user", "content": "hello"}],
        "input": [{"role": "user", "content": "hello"}]
    })
}

impl Routed {
    async fn send(&self, api: &str, payload: serde_json::Value) -> reqwest::Response {
        self.client
            .post(format!("http://{}{api}", self.address))
            .header("x-api-key", ROUTED_KEY)
            .json(&payload)
            .send()
            .await
            .unwrap()
    }

    async fn post(&self, api: &str, streaming: bool) -> reqwest::Response {
        self.send(api, routed_payload(streaming)).await
    }
}

async fn routed(
    fixture: &Fixture,
    store: Option<Arc<TraceStore>>,
    extract_thinking: bool,
) -> Routed {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    routed_on(listener, fixture, store, extract_thinking)
}

fn routed_on(
    listener: tokio::net::TcpListener,
    fixture: &Fixture,
    store: Option<Arc<TraceStore>>,
    extract_thinking: bool,
) -> Routed {
    let keys = Arc::new(crate::admin::client_keys::ClientKeyManager::new());
    keys.sync_system_key("system".into(), None, ROUTED_KEY.into());
    let app = super::router::create_router_with_shared_provider(
        fixture.state.kiro_provider.clone(),
        extract_thinking,
        ToolCompatibilityMode::Raw,
        Some(keys),
        None,
        None,
        None,
        store,
        None,
    );
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    Routed { address, client, server }
}

/// `GET /traces` on the admin router, backed by `store`.
async fn admin_traces(fixture: &Fixture, store: Arc<TraceStore>) -> Vec<serde_json::Value> {
    use crate::admin::{
        AdminService, AdminState, ClientKeyManager, GroupManager, UsageAggregator,
        create_admin_router,
    };
    let manager = fixture.state.kiro_provider.as_ref().unwrap().token_manager();
    let app = create_admin_router(AdminState::new(
        "fixture-admin-only",
        AdminService::new(manager.clone(), vec!["fixture".into()]),
        Arc::new(ClientKeyManager::new()),
        Arc::new(UsageAggregator::new()),
        store,
        Arc::new(GroupManager::new()),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/traces", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let body: serde_json::Value = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(&url)
        .header("x-api-key", "fixture-admin-only")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    server.abort();
    body["records"].as_array().unwrap().clone()
}

#[tokio::test]
async fn admin_traces_api_exposes_timing_and_leaves_old_rows_null() {
    use crate::admin::trace_db::TraceRecord;
    use crate::common::timing::{Arrival, ContentKind, ContentTally, Decoded, Ledger};

    let mut old: TraceRecord = serde_json::from_value(json!({
        "traceId": "old", "ts": "2026-11-01T00:00:00+00:00", "keyId": 0,
        "keySource": "masterApiKey", "model": "m", "isStream": true,
        "finalStatus": "success", "finalCredentialId": 1, "totalAttempts": 1,
        "durationMs": 900, "outputTokens": 120, "attempts": []
    }))
    .unwrap();
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    store.insert(&old);

    let mut ledger = Ledger::default();
    ledger.upstream_call(1_000);
    let mut content = ContentTally::default();
    content.add(ContentKind::Text, 40);
    for at in [2_000, 602_000] {
        ledger.upstream_chunk(Arrival { poll_started: at, at, bytes: 60 });
        ledger.decoded(at, &Decoded { frames: 1, content, final_usage: false });
    }
    old.trace_id = "new".into();
    old.ts = "2026-11-02T00:00:00+00:00".into();
    old.endpoint = Some("chat".into());
    old.output_tokens_source = Some("exact".into());
    old.timing = Some(ledger.summarize(700_000));
    store.insert(&old);

    let f = fixture(Mode::Reply(&["ok"])).await;
    let records = admin_traces(&f, store).await;
    let (new, old) = (&records[0], &records[1]);
    assert_eq!((&new["traceId"], &old["traceId"]), (&json!("new"), &json!("old")));
    assert_eq!(new["endpoint"], "chat");
    assert_eq!(new["outputTokensSource"], "exact");
    assert_eq!(new["timing"]["v"], 1);
    assert_eq!(new["timing"]["totalUs"], 700_000);
    assert_eq!(new["timing"]["phases"]["upstreamHeadersUs"], 1_000);
    assert_eq!(new["timing"]["stream"]["receiveGaps"]["counts"].as_array().unwrap().len(), 12);
    assert_eq!(new["throughput"]["outputTps"], 200.0);
    assert_eq!(new["throughput"]["outputTokensSource"], "exact");
    assert_eq!(old["durationMs"], 900);
    for field in ["endpoint", "outputTokensSource", "timing", "throughput"] {
        assert!(old[field].is_null(), "{field}: {}", old[field]);
    }
}

/// The trace row of the request that just ended.
async fn next_trace(
    store: &TraceStore,
    seen: &mut HashSet<String>,
) -> crate::admin::trace_db::TraceRecord {
    for _ in 0..250 {
        let (records, _) = store.query_paged(&TraceQuery { limit: 100, ..Default::default() });
        if let Some(record) = records.into_iter().find(|record| !seen.contains(&record.trace_id)) {
            seen.insert(record.trace_id.clone());
            return record;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the request wrote no trace row");
}

async fn read_until(
    body: &mut (impl futures::Stream<Item = reqwest::Result<Bytes>> + Unpin),
    received: &mut Vec<u8>,
    needle: &str,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !String::from_utf8_lossy(received).contains(needle) {
            let chunk = body.next().await.expect("stream ended before the expected text");
            received.extend_from_slice(&chunk.unwrap());
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{needle:?} did not reach the client"));
}

#[tokio::test]
async fn router_timing_covers_all_apis_stream_and_non_stream() {
    const EVENTS: usize = 3;
    let every = Duration::from_millis(30);
    let f = fixture(Mode::Cadence { events: EVENTS, every }).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    let routed = routed(&f, Some(store.clone()), false).await;
    let mut seen = HashSet::new();
    let text_bytes: usize = (0..EVENTS).map(|step| format!("{CADENCE_TEXT}{step} ").len()).sum();

    for (api, label) in ROUTED_APIS.into_iter().zip(["messages", "cc", "chat", "responses"]) {
        for streaming in [true, false] {
            let case = format!("{api} stream={streaming}");
            let response = routed.post(api, streaming).await;
            assert_eq!(response.status(), StatusCode::OK, "{case}");
            let body = response.text().await.unwrap();
            assert!(body.contains(&format!("{CADENCE_TEXT}{}", EVENTS - 1)), "{case}: {body}");

            let record = next_trace(&store, &mut seen).await;
            assert_eq!(record.endpoint.as_deref(), Some(label), "{case}");
            assert_eq!(record.is_stream, streaming, "{case}");
            assert_eq!(record.first_token_ms.is_some(), streaming, "{case}");
            assert_eq!(record.output_tokens, 17, "{case}");
            assert_eq!(record.output_tokens_source.as_deref(), Some("exact"), "{case}");

            let timing = record.timing.unwrap_or_else(|| panic!("{case}: no timing"));
            let phases = &timing.phases;
            let stream = timing.stream.as_ref().unwrap_or_else(|| panic!("{case}: no stream"));
            let mut order = vec![
                phases.body_parsed_us,
                phases.prepared_us,
                phases.upstream_headers_us,
                phases.first_body_bytes_us,
                phases.first_decoded_content_us,
            ];
            if streaming {
                order.push(phases.first_emit_text_us);
            } else {
                assert_eq!(phases.first_emit_text_us, None, "{case}");
            }
            order.extend([
                stream.last_content_us,
                stream.final_usage_us,
                stream.upstream_eos_us,
                Some(timing.total_us),
            ]);
            assert!(order.iter().all(Option::is_some), "{case}: {order:?}");
            assert!(order.is_sorted(), "{case}: {order:?}");
            assert_eq!((phases.first_emit_thinking_us, phases.first_emit_tool_us), (None, None), "{case}");

            assert_eq!(stream.upstream_calls, 1, "{case}");
            assert_eq!(
                (stream.text.events as usize, stream.text.bytes as usize),
                (EVENTS, text_bytes),
                "{case}"
            );
            assert_eq!(stream.thinking.events + stream.tool.events, 0, "{case}");
            assert_eq!(stream.frames_per_chunk.sum as usize, EVENTS + 1, "{case}");
            assert_eq!(stream.content_emits > 0, streaming, "{case}");
            let spaced = (every * (EVENTS as u32 - 1)).as_micros() as u64;
            assert!(stream.generation.window_us >= spaced * 3 / 4, "{case}: {stream:?}");
            assert!(stream.upstream_wait_us >= spaced, "{case}: {stream:?}");

            let spans = &timing.spans;
            assert_eq!(
                (spans.credential_select.count, spans.headers_wait.count, spans.backoff.count),
                (1, 1, 0),
                "{case}"
            );
        }
    }
}

#[tokio::test]
async fn router_timing_separates_thinking_from_text() {
    const REASONING: &str = "Let me think about the goal.";
    let f = fixture(Mode::ReplyWithReasoning(&["answer"])).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    let routed = routed(&f, Some(store.clone()), true).await;
    let mut seen = HashSet::new();
    for streaming in [true, false] {
        let mut payload = routed_payload(streaming);
        payload["thinking"] = json!({"type": "enabled", "budget_tokens": 2000});
        let body = routed.send("/v1/messages", payload).await.text().await.unwrap();
        assert!(body.contains(REASONING) && body.contains("answer"), "{body}");

        let timing = next_trace(&store, &mut seen).await.timing.unwrap();
        let stream = timing.stream.unwrap();
        assert_eq!((stream.thinking.events, stream.thinking.bytes as usize), (1, REASONING.len()));
        assert_eq!((stream.text.events, stream.text.bytes), (1, 6));
        assert_eq!(stream.generation.events, 2);
        let (thinking, text) = (timing.phases.first_emit_thinking_us, timing.phases.first_emit_text_us);
        if streaming {
            assert!(thinking.is_some() && thinking <= text, "{thinking:?} {text:?}");
        } else {
            assert_eq!((thinking, text), (None, None));
        }
    }
}

/// Also guards the Responses adapter: the first text must reach the client while the
/// upstream is still holding the rest.
#[tokio::test]
async fn router_timing_counts_a_gated_upstream_as_upstream_wait() {
    let gate = Duration::from_millis(250);
    let f = fixture(Mode::GatedChatText).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    let routed = routed(&f, Some(store.clone()), false).await;

    let mut body = routed.post("/v1/responses", true).await.bytes_stream();
    let mut received = Vec::new();
    read_until(&mut body, &mut received, "first streamed paragraph").await;
    assert!(!String::from_utf8_lossy(&received).contains("second streamed paragraph"));
    tokio::time::sleep(gate).await;
    f.release_tool_arguments.notify_one();
    read_until(&mut body, &mut received, "second streamed paragraph").await;
    f.release_tool_arguments.notify_one();
    while let Some(chunk) = body.next().await {
        chunk.unwrap();
    }

    let timing = next_trace(&store, &mut HashSet::new()).await.timing.unwrap();
    let stream = timing.stream.unwrap();
    let gate_us = gate.as_micros() as u64;
    assert!(stream.receive_gaps.max >= gate_us, "{stream:?}");
    assert!(stream.emit_gaps.max >= gate_us, "{stream:?}");
    assert!(stream.upstream_wait_us >= gate_us, "{stream:?}");
    assert!(stream.downstream_wait.total_us < gate_us / 2, "{stream:?}");
}

#[tokio::test]
async fn router_timing_counts_a_slow_reader_as_downstream_wait() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let pause = Duration::from_millis(1500);
    let f = fixture(Mode::Large).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());

    // Small socket buffers on both ends, so the pause pushes back on the gateway
    // instead of being absorbed by the kernel.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_send_buffer_size(4096).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let routed = routed_on(socket.listen(16).unwrap(), &f, Some(store.clone()), false);
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let mut client = socket.connect(routed.address).await.unwrap();
    let payload = routed_payload(true).to_string();
    let request = format!(
        "POST /v1/messages HTTP/1.1\r\nhost: gateway\r\nx-api-key: {ROUTED_KEY}\r\n\
         content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
        payload.len()
    );
    client.write_all(request.as_bytes()).await.unwrap();
    let mut buffer = vec![0u8; 64 * 1024];
    assert!(client.read(&mut buffer).await.unwrap() > 0);
    tokio::time::sleep(pause).await;
    // Read past everything that was buffered, so the gateway asks upstream for more
    // and the wait is recorded. Then hang up: the rest of the body adds nothing.
    let mut resumed = 0;
    while resumed < 4 * 1024 * 1024 {
        resumed += client.read(&mut buffer).await.unwrap();
    }
    drop(client);

    let timing = next_trace(&store, &mut HashSet::new()).await.timing.unwrap();
    let stream = timing.stream.unwrap();
    // One long wait, not many short ones: only a stalled reader produces it.
    assert!(
        stream.downstream_wait.max_us >= pause.as_micros() as u64 / 3,
        "{stream:?}"
    );
}

#[tokio::test]
async fn router_timing_names_an_estimated_token_source() {
    let f = fixture(Mode::NoProviderUsage).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    let routed = routed(&f, Some(store.clone()), false).await;
    let mut seen = HashSet::new();
    for (api, streaming) in [("/v1/messages", true), ("/v1/chat/completions", false)] {
        assert_eq!(routed.post(api, streaming).await.status(), StatusCode::OK);
        let record = next_trace(&store, &mut seen).await;
        assert_eq!(record.output_tokens_source.as_deref(), Some("estimated"), "{api}");
        assert_eq!(record.timing.unwrap().stream.unwrap().final_usage_us, None, "{api}");
    }

    let records = admin_traces(&f, store).await;
    assert_eq!(records.len(), 2);
    for record in &records {
        assert_eq!(record["outputTokensSource"], "estimated");
        assert_eq!(record["throughput"]["outputTokensSource"], "estimated");
        assert!(record["throughput"]["outputTps"].is_null(), "{record}");
        assert_eq!(record["timing"]["stream"]["text"]["bytes"], 2);
    }
    let mut endpoints: Vec<_> = records.iter().map(|record| record["endpoint"].as_str().unwrap()).collect();
    endpoints.sort_unstable();
    assert_eq!(endpoints, ["chat", "messages"]);
}

#[tokio::test]
async fn router_timing_keeps_provider_spans_when_the_upstream_fails() {
    let f = fixture(Mode::Overloaded).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    let routed = routed(&f, Some(store.clone()), false).await;
    let mut seen = HashSet::new();
    for streaming in [true, false] {
        let response = routed.post("/v1/messages", streaming).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let record = next_trace(&store, &mut seen).await;
        assert_eq!(record.final_status, "error");
        assert_eq!(record.output_tokens_source.as_deref(), Some("unavailable"));
        let timing = record.timing.unwrap();
        assert_eq!(timing.stream, None);
        assert_eq!(timing.phases.upstream_headers_us, None);
        assert!(timing.phases.prepared_us.is_some());
        let spans = &timing.spans;
        assert_eq!(
            (spans.credential_select.count, spans.headers_wait.count, spans.error_body_read.count),
            (1, 1, 1)
        );
        assert!(spans.headers_wait.total_us > 0);
    }
}

#[tokio::test]
async fn router_timing_is_written_for_interrupted_and_abandoned_streams() {
    let f = fixture(Mode::Interrupted).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    let routed_gateway = routed(&f, Some(store.clone()), false).await;
    let body = routed_gateway.post("/cc/v1/messages", true).await.text().await.unwrap();
    assert!(body.contains("early content") && body.contains("event: error"), "{body}");
    let record = next_trace(&store, &mut HashSet::new()).await;
    assert_eq!(record.error_type.as_deref(), Some("stream_interrupted"));
    assert_eq!(record.output_tokens_source.as_deref(), Some("estimated"));
    let timing = record.timing.unwrap();
    assert!(timing.phases.first_emit_text_us.is_some());
    let stream = timing.stream.unwrap();
    assert_eq!((stream.chunks, stream.upstream_eos_us), (1, None));

    let f = fixture(Mode::Stalled).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    let routed_gateway = routed(&f, Some(store.clone()), false).await;
    let mut body = routed_gateway.post("/v1/messages", true).await.bytes_stream();
    read_until(&mut body, &mut Vec::new(), "early content").await;
    drop(body);
    let record = next_trace(&store, &mut HashSet::new()).await;
    assert_eq!(record.final_status, "interrupted");
    let timing = record.timing.unwrap();
    assert!(timing.phases.first_emit_text_us.is_some());
    assert_eq!(timing.stream.unwrap().upstream_eos_us, None);
}

fn percentile(samples: &mut [f64], q: f64) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[((samples.len() - 1) as f64 * q).round() as usize]
}

/// Client-side view of one streamed request: ms to the first text chunk, ms to
/// the end of the body, and the longest wait between two text chunks.
async fn measure_stream(routed: &Routed, api: &str) -> (f64, f64, f64) {
    let start = Instant::now();
    let mut body = routed.post(api, true).await.bytes_stream();
    let mut first_text = None;
    let mut last_text = start;
    let mut gap_max = Duration::ZERO;
    while let Some(chunk) = body.next().await {
        if !String::from_utf8_lossy(&chunk.unwrap()).contains(CADENCE_TEXT) {
            continue;
        }
        let now = Instant::now();
        if first_text.is_some() {
            gap_max = gap_max.max(now - last_text);
        }
        first_text.get_or_insert(now - start);
        last_text = now;
    }
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    (
        ms(first_text.expect("stream carried no text")),
        ms(start.elapsed()),
        ms(gap_max),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "controlled latency measurement; run with --ignored --nocapture"]
async fn gateway_router_cadence_measurement() {
    const WARMUP: usize = 10;
    const REQUESTS: usize = 200;
    const EVENTS: usize = 8;
    const EVERY: Duration = Duration::from_millis(5);
    let f = fixture(Mode::Cadence { events: EVENTS, every: EVERY }).await;
    let store = Arc::new(TraceStore::open_in_memory().unwrap());
    let routed = routed(&f, Some(store.clone()), false).await;
    let mut seen = HashSet::new();
    for api in ROUTED_APIS {
        for _ in 0..WARMUP {
            measure_stream(&routed, api).await;
        }
        let (mut first, mut total, mut gap) = (Vec::new(), Vec::new(), Vec::new());
        for _ in 0..REQUESTS {
            let (f, t, g) = measure_stream(&routed, api).await;
            first.push(f);
            total.push(t);
            gap.push(g);
        }
        println!(
            "CADENCE api={api} requests={REQUESTS} events={EVENTS} every_ms={} \
             first_text_p50_ms={:.3} first_text_p95_ms={:.3} total_p50_ms={:.3} \
             total_p95_ms={:.3} gap_max_p50_ms={:.3} gap_max_p95_ms={:.3}",
            EVERY.as_millis(),
            percentile(&mut first, 0.5),
            percentile(&mut first, 0.95),
            percentile(&mut total, 0.5),
            percentile(&mut total, 0.95),
            percentile(&mut gap, 0.5),
            percentile(&mut gap, 0.95),
        );
        // The gateway's own view of one request, to place the client-side gap.
        measure_stream(&routed, api).await;
        seen.extend(
            store
                .query_paged(&TraceQuery { limit: 1000, ..Default::default() })
                .0
                .into_iter()
                .map(|record| record.trace_id),
        );
        let (_, _, client_gap) = measure_stream(&routed, api).await;
        let timing = next_trace(&store, &mut seen).await.timing.unwrap();
        let stream = timing.stream.unwrap();
        println!(
            "CADENCE_TRACE api={api} client_gap_max_ms={client_gap:.3} \
             upstream_receive_gap_max_ms={:.3} emit_gap_max_ms={:.3} \
             downstream_wait_max_ms={:.3} first_emit_text_ms={:.3}",
            stream.receive_gaps.max as f64 / 1000.0,
            stream.emit_gaps.max as f64 / 1000.0,
            stream.downstream_wait.max_us as f64 / 1000.0,
            timing.phases.first_emit_text_us.unwrap() as f64 / 1000.0,
        );
    }
}

// ---------------------------------------------------------------------------
// output_config.format = json_schema（Claude Code goal check 形态）
// ---------------------------------------------------------------------------

const OPENING_CONDITION: &[&str] = &[
    "The condition is not met yet. ",
    "{\"ok\":false,",
    "\"reason\":\"file missing\"}",
];
const OPENING_CHECKING: &[&str] = &[
    "I'm checking the repository state.\n\n",
    "{\"ok\":true,\"reason\":\"all tests pass\",\"impossible\":false}",
];
const OPENING_CORE: &[&str] = &[
    "The core con",
    "dition holds.\n```json\n{\"ok\":true,",
    "\"reason\":\"done\"}\n```\nDone.",
];
const PURE_JSON: &[&str] = &["{\"ok\":true,", "\"reason\":\"fine\"}"];
const NO_JSON: &[&str] = &["I'm checking the files, ", "nothing to report {yet}."];
const NONCONFORMING_THEN_OK: &[&str] = &[
    "First attempt: {\"ok\":\"yes\",\"reason\":1} then ",
    "{\"ok\":false,\"reason\":\"second\"} and {\"ok\":true,\"reason\":\"third\"}",
];

fn goal_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "ok": {"type": "boolean"},
            "reason": {"type": "string"},
            "impossible": {"type": "boolean"}
        },
        "required": ["ok", "reason"],
        "additionalProperties": false
    })
}

fn goal_output_config() -> serde_json::Value {
    json!({"format": {"type": "json_schema", "schema": goal_schema()}})
}

async fn goal_request(
    state: AppState,
    endpoint: &str,
    streaming: bool,
    thinking: bool,
    output_config: Option<serde_json::Value>,
) -> Response {
    goal_request_for_model(state, "claude-haiku-5-5", endpoint, streaming, thinking, output_config)
        .await
}

async fn goal_request_for_model(
    state: AppState,
    model: &str,
    endpoint: &str,
    streaming: bool,
    thinking: bool,
    output_config: Option<serde_json::Value>,
) -> Response {
    let mut req = json!({
        "model": model,
        "max_tokens": 256,
        "stream": streaming,
        "system": "You judge whether a goal condition is met.",
        "messages": [{"role": "user", "content": "Is the goal done?"}],
    });
    if let Some(output_config) = output_config {
        req["output_config"] = output_config;
    }
    if thinking {
        req["thinking"] = json!({"type": "enabled", "budget_tokens": 2000});
    }
    let payload = Json(serde_json::from_value(req).unwrap());
    if endpoint == "cc" {
        post_messages_cc(axum::extract::State(state), Extension(key()), payload).await
    } else {
        post_messages(axum::extract::State(state), Extension(key()), payload).await
    }
}

async fn body_string(response: Response) -> String {
    let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024).await.unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn parse_sse(body: &str) -> Vec<(String, serde_json::Value)> {
    body.split("\n\n")
        .filter(|block| !block.trim().is_empty())
        .map(|block| {
            let event = block
                .lines()
                .find_map(|line| line.strip_prefix("event: "))
                .unwrap()
                .to_owned();
            let data = block
                .lines()
                .find_map(|line| line.strip_prefix("data: "))
                .unwrap();
            (event, serde_json::from_str(data).unwrap())
        })
        .filter(|(event, _)| event != "ping")
        .collect()
}

/// 非 ping 事件的 (event, index) 序列；用于断言块结构。
fn sse_shape(events: &[(String, serde_json::Value)]) -> Vec<String> {
    events
        .iter()
        .filter(|(event, _)| event != "ping")
        .map(|(event, data)| match data.get("index") {
            Some(index) => format!("{event}#{index}"),
            None => event.clone(),
        })
        .collect()
}

fn sse_text(events: &[(String, serde_json::Value)]) -> String {
    events
        .iter()
        .filter(|(event, data)| event == "content_block_delta" && data["delta"]["type"] == "text_delta")
        .map(|(_, data)| data["delta"]["text"].as_str().unwrap())
        .collect()
}

fn assert_goal_json(text: &str, expected: serde_json::Value) {
    assert!(text.starts_with('{') && text.ends_with('}'), "not a bare JSON value: {text}");
    assert_eq!(serde_json::from_str::<serde_json::Value>(text).unwrap(), expected);
}

fn goal_cases() -> Vec<(&'static [&'static str], serde_json::Value)> {
    vec![
        (OPENING_CONDITION, json!({"ok": false, "reason": "file missing"})),
        (OPENING_CHECKING, json!({"ok": true, "reason": "all tests pass", "impossible": false})),
        (OPENING_CORE, json!({"ok": true, "reason": "done"})),
        (PURE_JSON, json!({"ok": true, "reason": "fine"})),
        // 多个合规值时取最后一个（结论通常在理由之后）
        (NONCONFORMING_THEN_OK, json!({"ok": true, "reason": "third"})),
    ]
}

#[tokio::test]
async fn structured_output_non_stream_extracts_json_from_prose() {
    for endpoint in ["messages", "cc"] {
        for (reply, expected) in goal_cases() {
            let f = fixture(Mode::Reply(reply)).await;
            let response = goal_request(f.state.clone(), endpoint, false, false, Some(goal_output_config())).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body: serde_json::Value = serde_json::from_str(&body_string(response).await).unwrap();
            let content = body["content"].as_array().unwrap();
            assert_eq!(content.len(), 1, "{endpoint}: {body}");
            assert_eq!(content[0]["type"], "text");
            assert_goal_json(content[0]["text"].as_str().unwrap(), expected);
            assert_eq!(body["type"], "message");
            assert!(body["id"].as_str().unwrap().starts_with("msg_"));
            assert_eq!(body["model"], "claude-haiku-5-5");
            assert_eq!(body["stop_reason"], "end_turn");
            assert_eq!(body["usage"]["output_tokens"], 17);
        }
    }
}

#[tokio::test]
async fn gateway_stream_respects_extract_thinking_disabled() {
    // L7：fixture 的 AppState 是 extract_thinking=false。流式与非流式必须一致：不拆出 thinking 块。
    let f = fixture(Mode::ThinkingText).await;
    let req = json!({"model": "gpt-5.6-sol", "max_tokens": 2048, "stream": true,
        "thinking": {"type": "enabled", "budget_tokens": 1024},
        "messages": [{"role": "user", "content": "hello"}]});
    for endpoint in ["messages", "cc"] {
        let response = if endpoint == "cc" {
            post_messages_cc(axum::extract::State(f.state.clone()), Extension(key()), Json(serde_json::from_value(req.clone()).unwrap())).await
        } else {
            post_messages(axum::extract::State(f.state.clone()), Extension(key()), Json(serde_json::from_value(req.clone()).unwrap())).await
        };
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let sse = String::from_utf8_lossy(&bytes);
        assert!(!sse.contains(r#""type":"thinking""#), "{endpoint}: {sse}");
        assert!(sse.contains("先想想</thinking>"), "{endpoint}: {sse}");
    }
}

#[tokio::test]
async fn structured_output_stream_emits_one_compliant_json_text_block() {
    for endpoint in ["messages", "cc"] {
        for (reply, expected) in goal_cases() {
            let f = fixture(Mode::Reply(reply)).await;
            let response = goal_request(f.state.clone(), endpoint, true, false, Some(goal_output_config())).await;
            assert_eq!(response.status(), StatusCode::OK);
            let events = parse_sse(&body_string(response).await);
            assert_eq!(
                sse_shape(&events),
                [
                    "message_start",
                    "content_block_start#0",
                    "content_block_delta#0",
                    "content_block_stop#0",
                    "message_delta",
                    "message_stop"
                ],
                "{endpoint}: {events:?}"
            );
            assert_eq!(events[0].1["message"]["model"], "claude-haiku-5-5");
            assert!(events[0].1["message"]["id"].as_str().unwrap().starts_with("msg_"));
            assert_eq!(events[1].1["content_block"]["type"], "text");
            assert_goal_json(&sse_text(&events), expected);
            let delta = &events.iter().find(|(event, _)| event == "message_delta").unwrap().1;
            assert_eq!(delta["delta"]["stop_reason"], "end_turn");
            assert_eq!(delta["usage"]["output_tokens"], 17);
        }
    }
}

#[tokio::test]
async fn structured_output_without_conforming_json_is_returned_unchanged() {
    let original: String = NO_JSON.concat();
    let f = fixture(Mode::Reply(NO_JSON)).await;
    let response = goal_request(f.state.clone(), "messages", false, false, Some(goal_output_config())).await;
    let body: serde_json::Value = serde_json::from_str(&body_string(response).await).unwrap();
    assert_eq!(body["content"], json!([{"type": "text", "text": original}]));
    assert_eq!(body["stop_reason"], "end_turn");

    let response = goal_request(f.state.clone(), "messages", true, false, Some(goal_output_config())).await;
    let events = parse_sse(&body_string(response).await);
    assert_eq!(sse_text(&events), original);
    let shape = sse_shape(&events);
    assert_eq!(shape.first().map(String::as_str), Some("message_start"));
    assert_eq!(shape.last().map(String::as_str), Some("message_stop"));
    assert!(shape.contains(&"message_delta".to_owned()));

    // 只有不符合 schema 的 JSON 同样原样返回。
    let f = fixture(Mode::Reply(&["{\"ok\":\"yes\"} and {\"reason\":2}"])).await;
    let response = goal_request(f.state.clone(), "messages", false, false, Some(goal_output_config())).await;
    let body: serde_json::Value = serde_json::from_str(&body_string(response).await).unwrap();
    assert_eq!(body["content"][0]["text"], "{\"ok\":\"yes\"} and {\"reason\":2}");
}

#[tokio::test]
async fn structured_output_keeps_thinking_blocks_before_json_text() {
    let f = fixture(Mode::ReplyWithReasoning(OPENING_CONDITION)).await;
    // 非流式默认不返回 thinking 块；开启 extract_thinking 后才有。
    let mut state = f.state.clone();
    state.extract_thinking = true;
    let response = goal_request(state, "messages", false, true, Some(goal_output_config())).await;
    let body: serde_json::Value = serde_json::from_str(&body_string(response).await).unwrap();
    let content = body["content"].as_array().unwrap();
    assert_eq!(content.len(), 2, "{body}");
    assert_eq!(content[0]["type"], "thinking");
    assert_eq!(content[0]["thinking"], "Let me think about the goal.");
    assert_eq!(content[1]["type"], "text");
    assert_goal_json(content[1]["text"].as_str().unwrap(), json!({"ok": false, "reason": "file missing"}));

    let response = goal_request(f.state.clone(), "messages", true, true, Some(goal_output_config())).await;
    let events = parse_sse(&body_string(response).await);
    assert_eq!(
        sse_shape(&events),
        [
            "message_start",
            "content_block_start#0",
            "content_block_delta#0",
            "content_block_delta#0",
            "content_block_delta#0",
            "content_block_stop#0",
            "content_block_start#1",
            "content_block_delta#1",
            "content_block_stop#1",
            "message_delta",
            "message_stop"
        ],
        "{events:?}"
    );
    assert_eq!(events[1].1["content_block"]["type"], "thinking");
    assert_eq!(events[6].1["content_block"]["type"], "text");
    assert_goal_json(&sse_text(&events), json!({"ok": false, "reason": "file missing"}));
}

#[tokio::test]
async fn structured_output_cc_preserves_thinking_with_format_only() {
    let fixture = fixture(Mode::ReplyWithReasoning(OPENING_CONDITION)).await;
    let response = goal_request(fixture.state.clone(), "cc", false, true, Some(goal_output_config())).await;
    let body: serde_json::Value = serde_json::from_str(&body_string(response).await).unwrap();
    assert_eq!(body["content"][0]["type"], "thinking");
    let response = goal_request(fixture.state.clone(), "cc", true, true, Some(goal_output_config())).await;
    let events = parse_sse(&body_string(response).await);
    assert!(events.iter().any(|(_, data)| data["content_block"]["type"] == "thinking"));
}

/// 去掉注入的指令、忽略空行与随机会话 id 后，便于比较“除注入外完全一致”。
fn strip_instruction(value: &mut serde_json::Value, instruction: &str) {
    match value {
        serde_json::Value::String(text) => {
            let stripped = text.replace(instruction, "");
            *text = stripped.split('\n').filter(|line| !line.is_empty()).collect::<Vec<_>>().join("\n");
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(|v| strip_instruction(v, instruction)),
        serde_json::Value::Object(map) => {
            // 每次请求都会随机生成的会话 id。
            for key in ["conversationId", "agentContinuationId"] {
                if let Some(id) = map.get_mut(key) { *id = json!("<id>"); }
            }
            map.values_mut().for_each(|v| strip_instruction(v, instruction));
        }
        _ => {}
    }
}

#[tokio::test]
async fn structured_output_injects_system_prompt_only_when_format_is_set_and_never_forwards_it() {
    let marker = super::structured_output::system_instruction(&goal_schema());
    let marker_line = marker.lines().next().unwrap().to_owned();
    let cases = [
        ("none", None),
        ("effort_only", Some(json!({"effort": "low"}))),
        ("format_only", Some(goal_output_config())),
        ("empty_object", Some(json!({}))),
        ("effort_and_format", Some(json!({"effort": "low", "format": goal_output_config()["format"].clone()}))),
        ("non_json_schema_format", Some(json!({"format": {"type": "text"}}))),
    ];
    let mut sent = HashMap::new();
    for (name, output_config) in cases {
        let f = fixture(Mode::Reply(PURE_JSON)).await;
        let response = goal_request(f.state.clone(), "messages", false, false, output_config).await;
        assert_eq!(response.status(), StatusCode::OK, "{name}");
        let _ = body_string(response).await;
        let bodies = f.bodies.lock();
        assert_eq!(bodies.len(), 1, "{name}");
        sent.insert(name, bodies[0].clone());
    }

    for (name, body) in &sent {
        // format / schema 不能出现在 Kiro 的额外字段里。
        let extra = body.get("additionalModelRequestFields").map(|v| v.to_string()).unwrap_or_default();
        assert!(!extra.contains("format") && !extra.contains("schema") && !extra.contains("json_schema"), "{name}: {extra}");
    }
    assert!(!sent["none"].to_string().contains(&marker_line));
    assert!(!sent["effort_only"].to_string().contains(&marker_line));
    assert!(!sent["non_json_schema_format"].to_string().contains(&marker_line));
    assert!(sent["format_only"].to_string().contains(&marker_line));
    assert!(sent["effort_and_format"].to_string().contains(&marker_line));

    // 除注入的指令外，只带 format 的请求与空 `output_config`（缺省 effort=high）逐字段一致。
    // 默认模型 haiku-5.5 支持原生 reasoning（#128），带 output_config 即下发 effort。
    let mut with_format = sent["format_only"].clone();
    strip_instruction(&mut with_format, &marker);
    let mut without = sent["empty_object"].clone();
    strip_instruction(&mut without, &marker);
    assert_eq!(with_format, without, "format-only must not change anything but the system prompt");
    let mut effort_and_format = sent["effort_and_format"].clone();
    strip_instruction(&mut effort_and_format, &marker);
    let mut effort_only = sent["effort_only"].clone();
    strip_instruction(&mut effort_only, &marker);
    assert_eq!(effort_and_format, effort_only, "effort must keep working next to format");
}

/// #128 x 结构化输出：claude-haiku-5.5 原生 reasoning 下，
/// - 只带 format：与 `{}` / `{effort:"high"}` 一致，缺省 effort=high 下发，format 绝不下发；
/// - format + effort：显式 effort（含 xhigh / max）原样透传；
/// - 不带 format / output_config：不下发 additionalModelRequestFields，回复不被改写。
#[tokio::test]
async fn structured_output_on_haiku_5_5_keeps_native_reasoning_effort() {
    let marker = super::structured_output::system_instruction(&goal_schema());
    let schema_format = goal_output_config()["format"].clone();
    let mut sent = HashMap::new();
    for (name, output_config) in [
        ("none", None),
        ("empty_object", Some(json!({}))),
        ("format_only", Some(goal_output_config())),
        ("effort_only_max", Some(json!({"effort": "max"}))),
        ("format_low", Some(json!({"effort": "low", "format": schema_format.clone()}))),
        ("format_medium", Some(json!({"effort": "medium", "format": schema_format.clone()}))),
        ("format_xhigh", Some(json!({"effort": "xhigh", "format": schema_format.clone()}))),
        ("format_max", Some(json!({"effort": "max", "format": schema_format.clone()}))),
    ] {
        let f = fixture(Mode::Reply(PURE_JSON)).await;
        let response = goal_request_for_model(
            f.state.clone(),
            "claude-haiku-5.5",
            "messages",
            false,
            false,
            output_config,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "{name}");
        let _ = body_string(response).await;
        let bodies = f.bodies.lock();
        assert_eq!(bodies.len(), 1, "{name}");
        sent.insert(name, bodies[0].clone());
    }
    let extra = |name: &str| {
        sent[name]
            .get("additionalModelRequestFields")
            .cloned()
            .unwrap_or(serde_json::Value::Null)
    };
    let high = json!({"output_config": {"effort": "high"}});
    assert_eq!(extra("none"), serde_json::Value::Null);
    assert_eq!(extra("empty_object"), high);
    assert_eq!(extra("format_only"), high);
    for (name, effort) in [
        ("effort_only_max", "max"),
        ("format_low", "low"),
        ("format_medium", "medium"),
        ("format_xhigh", "xhigh"),
        ("format_max", "max"),
    ] {
        assert_eq!(extra(name), json!({"output_config": {"effort": effort}}), "{name}");
    }
    // format / json_schema 绝不出现在上游请求的任何字段里（注入的 system 指令除外）。
    for (name, body) in &sent {
        let mut stripped = body.clone();
        strip_instruction(&mut stripped, &marker);
        let text = stripped.to_string().to_lowercase();
        assert!(!text.contains("json_schema") && !text.contains("\"format\""), "{name}: {text}");
    }
    // 只带 format 与空 output_config 除注入指令外完全一致。
    let mut a = sent["format_only"].clone();
    strip_instruction(&mut a, &marker);
    let mut b = sent["empty_object"].clone();
    strip_instruction(&mut b, &marker);
    assert_eq!(a, b);
    // 注入仅发生在带 json_schema format 的请求上。
    let marker_line = marker.lines().next().unwrap();
    for name in ["none", "empty_object", "effort_only_max"] {
        assert!(!sent[name].to_string().contains(marker_line), "{name}");
    }
    for name in ["format_only", "format_low", "format_medium", "format_xhigh", "format_max"] {
        assert!(sent[name].to_string().contains(marker_line), "{name}");
    }
}

/// master 语义：只要请求带了 `output_config`（哪怕只有 `format`），effort 默认为 `"high"`
/// 且非流式会提取 thinking 块。结构化输出改造不能改变这一点。
#[tokio::test]
async fn structured_output_format_only_keeps_master_effort_default() {
    // 选一个 master 会下发 additionalModelRequestFields 的模型：claude-sonnet-4.6
    // （model_supports_native_reasoning 命中，且非 opus 4.6 的 adaptive-only 限制）。
    for thinking in [false, true] {
        let mut sent = HashMap::new();
        for (name, output_config) in [
            ("format_only", Some(goal_output_config())),
            ("no_output_config", None),
            ("empty_object", Some(json!({}))),
            ("explicit_high", Some(json!({"effort": "high"}))),
            ("explicit_low", Some(json!({"effort": "low"}))),
        ] {
            let f = fixture(Mode::Reply(PURE_JSON)).await;
            let response = goal_request_for_model(
                f.state.clone(),
                "claude-sonnet-4.6",
                "messages",
                false,
                thinking,
                output_config,
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "{name}/{thinking}");
            let _ = body_string(response).await;
            let bodies = f.bodies.lock();
            assert_eq!(bodies.len(), 1, "{name}/{thinking}");
            sent.insert(name, bodies[0].clone());
        }

        let extra = |name: &str| {
            sent[name]
                .get("additionalModelRequestFields")
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        };
        // format-only 与 master 的「空的 / 显式 high 的 output_config」完全一致。
        assert_eq!(extra("format_only"), json!({"output_config": {"effort": "high"}}), "thinking={thinking}");
        assert_eq!(extra("format_only"), extra("empty_object"), "thinking={thinking}");
        assert_eq!(extra("format_only"), extra("explicit_high"), "thinking={thinking}");
        // 不带 output_config 时不带 reasoning 字段（master 同；thinking 启用时由 budget 推导）。
        if !thinking {
            assert_eq!(extra("no_output_config"), serde_json::Value::Null);
        }
        // 显式 effort 仍然照旧优先。
        assert_eq!(extra("explicit_low"), json!({"output_config": {"effort": "low"}}), "thinking={thinking}");

        // format 绝不下发；注入的 system 指令是唯一差别。
        let format_only = sent["format_only"].to_string();
        assert!(!format_only.to_lowercase().contains("format"), "{format_only}");
        assert!(!format_only.to_lowercase().contains("json_schema"), "{format_only}");
        let marker = super::structured_output::system_instruction(&goal_schema());
        let mut with_format = sent["format_only"].clone();
        strip_instruction(&mut with_format, &marker);
        let mut without = sent["explicit_high"].clone();
        strip_instruction(&mut without, &marker);
        assert_eq!(with_format, without, "thinking={thinking}");
    }
}

/// 非流式 + state.extract_thinking=false + thinking enabled + 只有 format 的 output_config：
/// 与 master 下 `output_config:{effort:"high"}` 的表现一致——因 payload 带 output_config，
/// thinking 仍作为独立内容块返回，其后的 JSON 文本块再被提取改写。
#[tokio::test]
async fn structured_output_format_only_extracts_thinking_like_master() {
    let f = fixture(Mode::ReplyWithReasoning(OPENING_CONDITION)).await;
    let mut state = f.state.clone();
    state.extract_thinking = false;

    let mut thinking = HashMap::new();
    for (name, output_config, bare_json) in [
        ("format_only", Some(goal_output_config()), true),
        ("explicit_high", Some(json!({"effort": "high"})), false),
    ] {
        let response = goal_request(state.clone(), "messages", false, true, output_config).await;
        assert_eq!(response.status(), StatusCode::OK, "{name}");
        let body: serde_json::Value = serde_json::from_str(&body_string(response).await).unwrap();
        let content = body["content"].as_array().unwrap();
        assert_eq!(content.len(), 2, "{name}: {body}");
        assert_eq!(content[0]["type"], "thinking", "{name}: {body}");
        assert_eq!(content[0]["thinking"], "Let me think about the goal.", "{name}");
        assert_eq!(content[1]["type"], "text", "{name}: {body}");
        if bare_json {
            // format 存在：text 块被改写为纯 JSON。
            assert_goal_json(
                content[1]["text"].as_str().unwrap(),
                json!({"ok": false, "reason": "file missing"}),
            );
        } else {
            assert_eq!(
                content[1]["text"],
                OPENING_CONDITION.concat(),
                "没有 format 时不改写回复"
            );
        }
        thinking.insert(name, content[0].clone());
    }
    // 两种 output_config 的 thinking 块完全相同：format 不影响 reasoning 行为。
    assert_eq!(thinking["format_only"], thinking["explicit_high"]);

    // 对照组：extract_thinking=false 且无 output_config 时不拆 thinking 块（master 同）。
    let f = fixture(Mode::ReplyWithReasoning(OPENING_CONDITION)).await;
    let mut state = f.state.clone();
    state.extract_thinking = false;
    let response = goal_request(state, "messages", false, true, None).await;
    let body: serde_json::Value = serde_json::from_str(&body_string(response).await).unwrap();
    assert_eq!(body["content"], json!([{"type": "text", "text": OPENING_CONDITION.concat()}]));
}

#[tokio::test]
async fn structured_output_is_inert_without_format() {
    // 没有 output_config / 只有 effort / 非 json_schema 的 format：响应和流都不被改写，
    // 即使回复里恰好含有符合某个 schema 的 JSON。
    let original: String = OPENING_CHECKING.concat();
    for output_config in [
        None,
        Some(json!({"effort": "low"})),
        Some(json!({"format": {"type": "text"}})),
        Some(json!({"format": {"type": "json_schema"}})),
    ] {
        let f = fixture(Mode::Reply(OPENING_CHECKING)).await;
        let response = goal_request(f.state.clone(), "messages", false, false, output_config.clone()).await;
        let body: serde_json::Value = serde_json::from_str(&body_string(response).await).unwrap();
        assert_eq!(body["content"], json!([{"type": "text", "text": original}]), "{output_config:?}");

        let response = goal_request(f.state.clone(), "messages", true, false, output_config.clone()).await;
        let events = parse_sse(&body_string(response).await);
        assert_eq!(sse_text(&events), original, "{output_config:?}");
        // 实时转发：文本按上游片段逐段下发，而不是合并成一个 delta。
        let deltas = events
            .iter()
            .filter(|(event, data)| event == "content_block_delta" && data["delta"]["type"] == "text_delta")
            .count();
        assert!(deltas >= 2, "{output_config:?}: text was not streamed live: {events:?}");
    }
}

#[tokio::test]
async fn structured_output_stream_failure_keeps_buffered_events_in_order() {
    let f = fixture(Mode::Interrupted).await;
    let response = goal_request(f.state.clone(), "messages", true, false, Some(goal_output_config())).await;
    let events = parse_sse(&body_string(response).await);
    let shape = sse_shape(&events);
    assert_eq!(shape.first().map(String::as_str), Some("message_start"));
    assert!(shape.contains(&"content_block_delta#0".to_owned()), "{shape:?}");
    assert_eq!(shape.last().map(String::as_str), Some("error"), "{shape:?}");
    assert!(!shape.contains(&"message_stop".to_owned()));
}

/// 结构化流式会缓冲到流末才发出内容：trace 首字时间应记在真正发出时，
/// 而不是上游内容到达时；不带 format 的请求照旧按到达时间记。
#[tokio::test]
async fn structured_output_stream_first_token_trace_is_emit_time() {
    let mut first_token = HashMap::new();
    for (name, output_config) in [("structured", Some(goal_output_config())), ("plain", None)] {
        let mut f = fixture(Mode::Normal(Duration::from_millis(300))).await;
        let store = Arc::new(TraceStore::open_in_memory().unwrap());
        f.state = f.state.clone().with_trace_store(Some(store.clone()));
        let response = goal_request(f.state.clone(), "messages", true, false, output_config).await;
        assert_eq!(response.status(), StatusCode::OK, "{name}");
        let _ = body_string(response).await;
        let (records, count) = store.query_paged(&TraceQuery {
            limit: 10,
            ..Default::default()
        });
        assert_eq!(count, 1, "{name}");
        first_token.insert(name, records[0].first_token_ms.unwrap());
    }
    assert!(first_token["structured"] >= 300, "{first_token:?}");
    assert!(first_token["plain"] < 300, "{first_token:?}");
}

#[tokio::test]
async fn gateway_upstream_exception_keeps_classified_reason() {
    // M3：流中途的 ThrottlingException 必须以 rate_limit_error 告诉客户端，且不带上游原文。
    let f = fixture(Mode::Throttled).await;
    let sse = {
        let response = request(f.state.clone(), "messages", true).await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        String::from_utf8_lossy(&bytes).into_owned()
    };
    assert!(sse.contains("rate_limit_error"), "{sse}");
    assert!(!sse.contains("message_stop"), "{sse}");
    assert!(!sse.contains("123456789012"), "{sse}");
    let response = request(f.state.clone(), "messages", false).await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn gateway_search_loop_rejects_truncated_tool_json() {
    // L10：web_search 循环里半截工具参数不能以 {} 交给客户端执行。
    let mut failures = Vec::new();
    for streaming in [false, true] {
        let f = fixture(Mode::SearchInvalidToolJson).await;
        let payload = json!({
            "model":"gpt-5.6-sol","max_tokens":64,"stream":streaming,
            "messages":[{"role":"user","content":"hello"}],
            "tools":[
                {"type":"web_search_20250305","name":"web_search"},
                {"name":"run","description":"test command","input_schema":{"type":"object","properties":{"command":{"type":"string"}}}}
            ]
        });
        let response = post_messages(
            axum::extract::State(f.state.clone()),
            Extension(key()),
            Json(serde_json::from_value(payload).unwrap()),
        )
        .await;
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = String::from_utf8_lossy(&bytes).into_owned();
        let failed_visibly = status != StatusCode::OK || body.contains("event: error");
        if !failed_visibly || body.contains(r#""stop_reason":"tool_use""#) {
            failures.push(format!("stream={streaming}, status={status}, body={body}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// 读流式响应里最后一份 usage（message_start 先给占位，message_delta 给最终值）。
async fn streamed_cache_usage(response: Response) -> (u64, u64) {
    let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024).await.unwrap();
    let text = String::from_utf8_lossy(&bytes);
    let (mut read, mut create) = (0, 0);
    for line in text.lines().filter_map(|l| l.strip_prefix("data: ")) {
        let Ok(event) = serde_json::from_str::<serde_json::Value>(line) else { continue };
        let usage = event.get("usage").or_else(|| event.pointer("/message/usage"));
        if let Some(u) = usage {
            if let Some(v) = u.get("cache_read_input_tokens").and_then(|v| v.as_u64()) { read = v; }
            if let Some(v) = u.get("cache_creation_input_tokens").and_then(|v| v.as_u64()) { create = v; }
        }
    }
    (read, create)
}

#[tokio::test]
async fn gateway_dsh_master_key_reports_cache_hits_without_session_header() {
    // 回归：DSH（pi-ai）用主 Key、不带 metadata.user_id，也发不出会话请求头。
    // 以前 isolation_seed 返回 None → 每个请求都是 miss_no_seed，DSH 永远显示缓存命中 0%。
    // 请求体是 pi-ai 1.4.0 真实生成的三轮（见 fixtures/dsh-pi-ai-1.4.0/README.md），不手造。
    let turns: Vec<serde_json::Value> = [
        include_str!("fixtures/dsh-pi-ai-1.4.0/turn1.json"),
        include_str!("fixtures/dsh-pi-ai-1.4.0/turn2.json"),
        include_str!("fixtures/dsh-pi-ai-1.4.0/turn3.json"),
    ]
    .iter()
    .map(|s| serde_json::from_str(s).unwrap())
    .collect();
    let f = fixture(Mode::NoProviderUsage).await;
    let state = f
        .state
        .clone()
        .with_cache_meter(Some(Arc::new(crate::anthropic::cache_metering::CacheMeter::new(None))));
    let send = |body: serde_json::Value| {
        let state = state.clone();
        async move {
            let response = post_messages(
                axum::extract::State(state),
                Extension(key()),
                Json(serde_json::from_value(body).unwrap()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            streamed_cache_usage(response).await
        }
    };

    let (read1, create1) = send(turns[0].clone()).await;
    assert_eq!(read1, 0, "首轮没有可读的缓存");
    assert!(create1 > 0, "首轮应写入缓存，实际 create={create1}");

    let (read2, _) = send(turns[1].clone()).await;
    assert!(read2 > 0, "同一会话第二轮应读到缓存（DSH 0% 的 bug），实际 read={read2}");

    let (read3, _) = send(turns[2].clone()).await;
    assert!(read3 > read2, "第三轮应读到第二轮写入的更长前缀：read2={read2} read3={read3}");

    // 另一个会话：第一条 user 消息不同 → 不应读到上一个会话的缓存
    let mut other = turns[1].clone();
    other["messages"][0]["content"] = json!("另一个会话的第一条消息。");
    let (read_other, create_other) = send(other).await;
    assert_eq!(read_other, 0, "不同会话不应命中");
    assert!(create_other > 0);
}
