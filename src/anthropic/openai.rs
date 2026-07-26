//! OpenAI Chat Completions 兼容端点
//!
//! 把 OpenAI `POST /v1/chat/completions` 请求翻译成内部的 Anthropic
//! [`MessagesRequest`]，复用 [`super::handlers::post_messages`] 的完整链路
//! （模型映射、多凭据故障转移、用量计量、工具映射……），再把 Anthropic 响应
//! 翻译回 OpenAI 格式。
//!
//! 这样只会说 OpenAI 协议的客户端（如 Codex CLI，`wire_api = "chat"`）也能
//! 直接走 Kiro 后端，无需额外的翻译代理进程。
//!
//! 说明：内部调用始终以非流式方式执行，`stream: true` 的请求在拿到完整结果后
//! 合成为 OpenAI 的 `chat.completion.chunk` SSE 序列。对 Codex 这类"拿到结果再
//! 展示"的客户端，语义与逐 token 流式一致；正确性（含工具调用）完全保留。

use std::collections::BTreeMap;

use axum::{
    Json,
    body::{Body, to_bytes},
    extract::{Extension, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::handlers::post_messages;
use super::middleware::{AppState, KeyContext};
use super::types::{Message, MessagesRequest, OutputConfig, SystemMessage, Tool};

/// 读取内部响应体时的上限（64MB，与请求体上限对齐）
const MAX_INNER_BODY: usize = 64 * 1024 * 1024;

/// 未显式给出 max_tokens 时的默认输出上限
const DEFAULT_MAX_TOKENS: i32 = 32000;

// ============================ 请求类型 ============================

#[derive(Debug, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    #[serde(default)]
    pub messages: Vec<Value>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub max_tokens: Option<i32>,
    #[serde(default)]
    pub max_completion_tokens: Option<i32>,
    #[serde(default)]
    pub tools: Option<Vec<Value>>,
    #[serde(default)]
    pub tool_choice: Option<Value>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
}

// ============================ Handler ============================

/// `POST /v1/chat/completions`
pub async fn post_chat_completions(
    State(state): State<AppState>,
    Extension(key_ctx): Extension<KeyContext>,
    Json(req): Json<ChatCompletionRequest>,
) -> Response {
    let want_stream = req.stream;
    let model = req.model.clone();

    tracing::info!(
        model = %model,
        stream = %want_stream,
        message_count = %req.messages.len(),
        "Received POST /v1/chat/completions request"
    );

    // 1. OpenAI -> Anthropic 请求翻译
    let anthropic_req = match openai_to_anthropic(req) {
        Ok(r) => r,
        Err(msg) => {
            return openai_error(StatusCode::BAD_REQUEST, "invalid_request_error", &msg);
        }
    };

    // 2. 复用 Anthropic 全链路（内部强制非流式）
    let inner = post_messages(State(state), Extension(key_ctx), Json(anthropic_req)).await;

    let status = inner.status();
    let body_bytes = match to_bytes(inner.into_body(), MAX_INNER_BODY).await {
        Ok(b) => b,
        Err(e) => {
            return openai_error(
                StatusCode::BAD_GATEWAY,
                "api_error",
                &format!("failed to read upstream response: {e}"),
            );
        }
    };

    // 上游非 2xx：原样透传（Anthropic 错误体已是 {"error":{type,message}} 形状）
    if !status.is_success() {
        return Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body_bytes))
            .unwrap();
    }

    let anthropic: Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => {
            return openai_error(
                StatusCode::BAD_GATEWAY,
                "api_error",
                &format!("failed to parse upstream response: {e}"),
            );
        }
    };

    // 3. Anthropic -> OpenAI 响应翻译
    let parsed = parse_anthropic_message(&anthropic, &model);

    if want_stream {
        let sse = build_stream_sse(&parsed);
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from(sse))
            .unwrap()
    } else {
        let body = build_completion_json(&parsed);
        (StatusCode::OK, Json(body)).into_response()
    }
}

// ============================ 请求翻译 ============================

fn openai_to_anthropic(req: ChatCompletionRequest) -> Result<MessagesRequest, String> {
    let max_tokens = req
        .max_tokens
        .or(req.max_completion_tokens)
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_MAX_TOKENS);

    let mut system: Vec<SystemMessage> = Vec::new();
    // 合并后的对话消息：(role, content blocks)
    let mut merged: Vec<(String, Vec<Value>)> = Vec::new();

    for m in &req.messages {
        let role = m.get("role").and_then(|v| v.as_str()).unwrap_or("");
        match role {
            "system" | "developer" => {
                for text in collect_text_strings(m.get("content")) {
                    system.push(SystemMessage {
                        text,
                        cache_control: None,
                    });
                }
            }
            "user" => {
                let blocks = content_blocks(m.get("content"));
                push_merged(&mut merged, "user", blocks);
            }
            "assistant" => {
                let mut blocks = content_blocks(m.get("content"));
                if let Some(calls) = m.get("tool_calls").and_then(|v| v.as_array()) {
                    for call in calls {
                        let id = call
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let func = call.get("function");
                        let name = func
                            .and_then(|f| f.get("name"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let args_str = func
                            .and_then(|f| f.get("arguments"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("{}");
                        let input: Value =
                            serde_json::from_str(args_str).unwrap_or_else(|_| json!({}));
                        blocks.push(json!({
                            "type": "tool_use",
                            "id": id,
                            "name": name,
                            "input": input,
                        }));
                    }
                }
                push_merged(&mut merged, "assistant", blocks);
            }
            "tool" => {
                let tool_use_id = m
                    .get("tool_call_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let content = collect_text_strings(m.get("content")).join("\n");
                let block = json!({
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": content,
                });
                // Anthropic 里 tool_result 属于 user 轮
                push_merged(&mut merged, "user", vec![block]);
            }
            _ => {}
        }
    }

    // 丢弃空内容轮，Anthropic 不接受空 content
    let messages: Vec<Message> = merged
        .into_iter()
        .filter(|(_, blocks)| !blocks.is_empty())
        .map(|(role, blocks)| Message {
            role,
            content: Value::Array(blocks),
        })
        .collect();

    if messages.is_empty() {
        return Err("messages must contain at least one user/assistant message".to_string());
    }

    let tools = req.tools.as_ref().map(|ts| convert_tools(ts));
    let tool_choice = req.tool_choice.as_ref().and_then(convert_tool_choice);
    let output_config = req
        .reasoning_effort
        .filter(|e| !e.trim().is_empty())
        .map(|effort| OutputConfig { effort });

    Ok(MessagesRequest {
        model: req.model,
        max_tokens,
        messages,
        stream: false, // 内部始终非流式
        system: if system.is_empty() { None } else { Some(system) },
        tools,
        tool_choice,
        thinking: None,
        output_config,
        metadata: None,
    })
}

/// 追加到 merged，若与上一轮 role 相同则合并 content blocks
pub(super) fn push_merged(merged: &mut Vec<(String, Vec<Value>)>, role: &str, blocks: Vec<Value>) {
    if blocks.is_empty() {
        return;
    }
    if let Some(last) = merged.last_mut() {
        if last.0 == role {
            last.1.extend(blocks);
            return;
        }
    }
    merged.push((role.to_string(), blocks));
}

/// 把 OpenAI message.content（字符串或数组）转成 Anthropic content blocks
fn content_blocks(content: Option<&Value>) -> Vec<Value> {
    let mut out = Vec::new();
    match content {
        Some(Value::String(s)) => {
            if !s.is_empty() {
                out.push(json!({"type": "text", "text": s}));
            }
        }
        Some(Value::Array(parts)) => {
            for part in parts {
                let ty = part.get("type").and_then(|v| v.as_str()).unwrap_or("");
                match ty {
                    "text" | "input_text" => {
                        if let Some(t) = part.get("text").and_then(|v| v.as_str()) {
                            out.push(json!({"type": "text", "text": t}));
                        }
                    }
                    "image_url" => {
                        if let Some(block) = image_block(part) {
                            out.push(block);
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    out
}

/// 仅收集纯文本（system / tool 内容用）
pub(super) fn collect_text_strings(content: Option<&Value>) -> Vec<String> {
    let mut out = Vec::new();
    match content {
        Some(Value::String(s)) => {
            if !s.is_empty() {
                out.push(s.clone());
            }
        }
        Some(Value::Array(parts)) => {
            for part in parts {
                if let Some(t) = part.get("text").and_then(|v| v.as_str()) {
                    if !t.is_empty() {
                        out.push(t.to_string());
                    }
                }
            }
        }
        _ => {}
    }
    out
}

/// 把 OpenAI image_url（仅支持 data: URL）转成 Anthropic image block
fn image_block(part: &Value) -> Option<Value> {
    let url = part
        .get("image_url")
        .and_then(|iu| iu.get("url"))
        .and_then(|v| v.as_str())?;
    let rest = url.strip_prefix("data:")?;
    let (media_type, data) = rest.split_once(";base64,")?;
    Some(json!({
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": data,
        }
    }))
}

pub(super) fn convert_tools(tools: &[Value]) -> Vec<Tool> {
    let mut out = Vec::new();
    for t in tools {
        // OpenAI: {type:"function", function:{name, description, parameters}}
        let func = t.get("function").unwrap_or(t);
        let name = func
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if name.is_empty() {
            continue;
        }
        let description = func
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let mut input_schema: BTreeMap<String, Value> = BTreeMap::new();
        if let Some(Value::Object(params)) = func.get("parameters") {
            for (k, v) in params {
                input_schema.insert(k.clone(), v.clone());
            }
        }
        out.push(Tool {
            tool_type: None,
            name,
            description,
            input_schema,
            max_uses: None,
            cache_control: None,
        });
    }
    out
}

fn convert_tool_choice(tc: &Value) -> Option<Value> {
    match tc {
        Value::String(s) => match s.as_str() {
            "auto" => Some(json!({"type": "auto"})),
            "required" => Some(json!({"type": "any"})),
            "none" => None,
            _ => Some(json!({"type": "auto"})),
        },
        Value::Object(_) => {
            let name = tc
                .get("function")
                .and_then(|f| f.get("name"))
                .and_then(|v| v.as_str());
            name.map(|n| json!({"type": "tool", "name": n}))
        }
        _ => None,
    }
}

// ============================ 响应翻译 ============================

pub(super) struct ParsedResponse {
    pub(super) model: String,
    pub(super) text: String,
    pub(super) tool_calls: Vec<Value>, // OpenAI tool_calls
    pub(super) finish_reason: String,
    pub(super) prompt_tokens: i64,
    pub(super) completion_tokens: i64,
    /// 内部代答的 web_search 展示（server_tool_use 块）：(id, query)。
    /// Responses 路径渲染为 web_search_call item。
    pub(super) web_searches: Vec<(String, String)>,
}

pub(super) fn parse_anthropic_message(anthropic: &Value, model: &str) -> ParsedResponse {
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    let mut web_searches = Vec::new();

    if let Some(blocks) = anthropic.get("content").and_then(|v| v.as_array()) {
        for block in blocks {
            match block.get("type").and_then(|v| v.as_str()) {
                Some("text") => {
                    if let Some(t) = block.get("text").and_then(|v| v.as_str()) {
                        text.push_str(t);
                    }
                }
                // Kiro's thinking blocks contain raw model reasoning rather than a
                // provider-authored summary. OpenAI-compatible endpoints must not
                // expose or replay that private content.
                Some("thinking") => {}
                Some("server_tool_use") => {
                    // 内部代答的 web_search 展示块（websearch_loop Contract A）
                    if block.get("name").and_then(|v| v.as_str()) == Some("web_search") {
                        let id = block
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let query = block
                            .pointer("/input/query")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        web_searches.push((id, query));
                    }
                }
                Some("tool_use") => {
                    let id = block
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let name = block
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let arguments = block
                        .get("input")
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "{}".to_string());
                    tool_calls.push(json!({
                        "id": id,
                        "type": "function",
                        "function": { "name": name, "arguments": arguments },
                    }));
                }
                _ => {} // web_search_tool_result / 其它块对 OpenAI 客户端无意义，忽略
            }
        }
    }

    let stop_reason = anthropic
        .get("stop_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("end_turn");
    let finish_reason = map_finish_reason(stop_reason, !tool_calls.is_empty()).to_string();

    let usage = anthropic.get("usage");
    let prompt_tokens = usage
        .and_then(|u| u.get("input_tokens"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let completion_tokens = usage
        .and_then(|u| u.get("output_tokens"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    ParsedResponse {
        model: model.to_string(),
        text,
        tool_calls,
        finish_reason,
        prompt_tokens,
        completion_tokens,
        web_searches,
    }
}

fn map_finish_reason(stop_reason: &str, has_tool_calls: bool) -> &'static str {
    match stop_reason {
        "tool_use" => "tool_calls",
        "max_tokens" | "model_context_window_exceeded" => "length",
        _ if has_tool_calls => "tool_calls",
        _ => "stop",
    }
}

pub(super) fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

fn new_id() -> String {
    format!("chatcmpl-{}", Uuid::new_v4().to_string().replace('-', ""))
}

fn build_completion_json(p: &ParsedResponse) -> Value {
    let content: Value = if p.text.is_empty() && !p.tool_calls.is_empty() {
        Value::Null
    } else {
        Value::String(p.text.clone())
    };

    let mut message = json!({ "role": "assistant", "content": content });
    if !p.tool_calls.is_empty() {
        message["tool_calls"] = Value::Array(p.tool_calls.clone());
    }

    json!({
        "id": new_id(),
        "object": "chat.completion",
        "created": now_ts(),
        "model": p.model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": p.finish_reason,
        }],
        "usage": {
            "prompt_tokens": p.prompt_tokens,
            "completion_tokens": p.completion_tokens,
            "total_tokens": p.prompt_tokens + p.completion_tokens,
        }
    })
}

/// 把完整结果合成为 OpenAI chat.completion.chunk SSE 序列
fn build_stream_sse(p: &ParsedResponse) -> String {
    let id = new_id();
    let created = now_ts();
    let mut out = String::new();

    let mut push_chunk = |delta: Value, finish: Option<&str>| {
        let chunk = json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": p.model,
            "choices": [{
                "index": 0,
                "delta": delta,
                "finish_reason": finish,
            }],
        });
        out.push_str("data: ");
        out.push_str(&chunk.to_string());
        out.push_str("\n\n");
    };

    // 角色帧
    push_chunk(json!({ "role": "assistant" }), None);

    // 文本帧
    if !p.text.is_empty() {
        push_chunk(json!({ "content": p.text }), None);
    }

    // 工具调用帧
    for (i, tc) in p.tool_calls.iter().enumerate() {
        let delta = json!({
            "tool_calls": [{
                "index": i,
                "id": tc.get("id").cloned().unwrap_or(Value::Null),
                "type": "function",
                "function": tc.get("function").cloned().unwrap_or(json!({})),
            }]
        });
        push_chunk(delta, None);
    }

    // 结束帧（带 usage）
    let final_chunk = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": p.model,
        "choices": [{
            "index": 0,
            "delta": {},
            "finish_reason": p.finish_reason,
        }],
        "usage": {
            "prompt_tokens": p.prompt_tokens,
            "completion_tokens": p.completion_tokens,
            "total_tokens": p.prompt_tokens + p.completion_tokens,
        }
    });
    out.push_str("data: ");
    out.push_str(&final_chunk.to_string());
    out.push_str("\n\n");
    out.push_str("data: [DONE]\n\n");

    out
}

fn openai_error(status: StatusCode, err_type: &str, message: &str) -> Response {
    let body = json!({
        "error": {
            "message": message,
            "type": err_type,
        }
    });
    (status, Json(body)).into_response()
}
