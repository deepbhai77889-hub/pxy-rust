use axum::{
    body::Body,
    extract::State,
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

struct AppState {
    client: Client,
    keys: Vec<String>,
    current_key_idx: AtomicUsize,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Default 2 NVIDIA API keys with instant failover
    let keys = vec![
        "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".to_string(),
        "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".to_string(),
    ];

    let client = Client::builder()
        .http2_prior_knowledge()
        .tcp_nodelay(true)
        .pool_max_idle_per_host(100)
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .build()
        .expect("Failed to initialize reqwest client");

    let state = Arc::new(AppState {
        client,
        keys,
        current_key_idx: AtomicUsize::new(0),
    });

    let app = Router::new()
        // OpenAI Format Endpoints
        .route("/v1/models", get(list_models_handler))
        .route("/v1/chat/completions", post(openai_chat_handler))
        .route("/chat/completions", post(openai_chat_handler))
        // Anthropic Messages Format Endpoint
        .route("/v1/messages", post(anthropic_messages_handler))
        .route("/messages", post(anthropic_messages_handler))
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await.unwrap();

    info!("🚀 Rust Ultra-Fast NVIDIA AI Gateway running on http://{}", addr);
    info!("⚡ OpenAI (/v1/chat/completions) & Anthropic (/v1/messages) Supported!");
    info!("⚡ Multi-modal Vision & Full Function Tool Calling Active!");
    axum::serve(listener, app).await.unwrap();
}

// -----------------------------------------------------------------------------------------
// Helper: NVIDIA Forwarder with Auto Key Switch on Rate-Limit (429/403/503)
// -----------------------------------------------------------------------------------------
impl AppState {
    fn get_key(&self, attempt: usize) -> &str {
        let base = self.current_key_idx.load(Ordering::Relaxed);
        let idx = (base + attempt) % self.keys.len();
        &self.keys[idx]
    }

    fn advance_key(&self) {
        let old = self.current_key_idx.fetch_add(1, Ordering::Relaxed);
        info!("🔄 Switched NVIDIA API key from slot {} -> {}", old % self.keys.len(), (old + 1) % self.keys.len());
    }

    async fn send_nvidia_request(&self, payload: &Value) -> Result<reqwest::Response, String> {
        let total_keys = self.keys.len();
        let mut last_err = String::from("No response");

        for attempt in 0..total_keys {
            let api_key = self.get_key(attempt);
            let resp = self
                .client
                .post("https://integrate.api.nvidia.com/v1/chat/completions")
                .header("Content-Type", "application/json")
                .header("Authorization", format!("Bearer {}", api_key))
                .json(payload)
                .send()
                .await;

            match resp {
                Ok(response) => {
                    let status = response.status();
                    // If rate-limited (429) or forbidden (403) or service unavailable (503), switch key!
                    if status == StatusCode::TOO_MANY_REQUESTS
                        || status == StatusCode::FORBIDDEN
                        || status == StatusCode::UNAUTHORIZED
                        || status == StatusCode::SERVICE_UNAVAILABLE
                    {
                        warn!("⚠️ NVIDIA API returned {} with key slot {}. Rotating to next key...", status, attempt);
                        self.advance_key();
                        last_err = format!("NVIDIA error status {}", status);
                        continue;
                    }
                    return Ok(response);
                }
                Err(e) => {
                    error!("Connection error with NVIDIA: {:?}", e);
                    last_err = e.to_string();
                }
            }
        }
        Err(format!("All NVIDIA API keys exhausted/rate-limited. Last error: {}", last_err))
    }
}

// -----------------------------------------------------------------------------------------
// OpenAI Endpoint Handler (/v1/chat/completions)
// -----------------------------------------------------------------------------------------
async fn openai_chat_handler(
    State(state): State<Arc<AppState>>,
    Json(mut payload): Json<Value>,
) -> Response {
    // Normalizing model name (e.g. kimi-k3, deepseek, etc.)
    if let Some(m) = payload.get("model").and_then(|v| v.as_str()) {
        let clean_model = m.trim_start_matches("nvidia/").trim_start_matches("openai/");
        payload["model"] = json!(clean_model);
    }

    let is_stream = payload.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);

    match state.send_nvidia_request(&payload).await {
        Ok(upstream_resp) => {
            let status = StatusCode::from_u16(upstream_resp.status().as_u16())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

            if is_stream {
                let stream = upstream_resp.bytes_stream().map(|item| {
                    item.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                });
                let mut resp = Response::new(Body::from_stream(stream));
                *resp.status_mut() = status;
                resp.headers_mut().insert(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("text/event-stream; charset=utf-8"),
                );
                resp.headers_mut().insert(
                    axum::http::header::CACHE_CONTROL,
                    HeaderValue::from_static("no-cache"),
                );
                resp
            } else {
                let bytes = upstream_resp.bytes().await.unwrap_or_default();
                let mut resp = Response::new(Body::from(bytes));
                *resp.status_mut() = status;
                resp.headers_mut().insert(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
                resp
            }
        }
        Err(err) => json_error(StatusCode::BAD_GATEWAY, &err),
    }
}

// -----------------------------------------------------------------------------------------
// Anthropic Endpoint Handler (/v1/messages) -> Translates Anthropic -> OpenAI -> Anthropic
// -----------------------------------------------------------------------------------------
async fn anthropic_messages_handler(
    State(state): State<Arc<AppState>>,
    Json(anthropic_req): Json<Value>,
) -> Response {
    let is_stream = anthropic_req.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
    let model = anthropic_req
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("moonshotai/kimi-k3")
        .trim_start_matches("anthropic/")
        .trim_start_matches("claude-")
        .to_string();

    let actual_model = if model.is_empty() || model.starts_with("3") {
        "moonshotai/kimi-k3".to_string()
    } else {
        model
    };

    // 1. Translate Anthropic Request Format -> OpenAI Request Format (Tools, Vision, Messages)
    let openai_payload = translate_anthropic_to_openai(&anthropic_req, &actual_model);

    match state.send_nvidia_request(&openai_payload).await {
        Ok(upstream_resp) => {
            if !upstream_resp.status().is_success() {
                let err_body: Value = upstream_resp.json().await.unwrap_or(json!({"error": "Upstream error"}));
                return (StatusCode::BAD_GATEWAY, Json(err_body)).into_response();
            }

            if is_stream {
                // Stream translator: OpenAI SSE chunks -> Anthropic SSE Events
                let msg_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
                let model_name = actual_model.clone();

                let byte_stream = upstream_resp.bytes_stream();
                let anthropic_stream = async_stream::stream! {
                    // Send initial message_start event
                    let start_evt = json!({
                        "type": "message_start",
                        "message": {
                            "id": msg_id,
                            "type": "message",
                            "role": "assistant",
                            "content": [],
                            "model": model_name,
                            "stop_reason": null,
                            "stop_sequence": null,
                            "usage": { "input_tokens": 10, "output_tokens": 1 }
                        }
                    });
                    yield Ok::<Bytes, std::io::Error>(Bytes::from(format!("event: message_start\ndata: {}\n\n", start_evt)));

                    let content_start = json!({
                        "type": "content_block_start",
                        "index": 0,
                        "content_block": { "type": "text", "text": "" }
                    });
                    yield Ok(Bytes::from(format!("event: content_block_start\ndata: {}\n\n", content_start)));

                    let mut buffer = String::new();
                    tokio::pin!(byte_stream);

                    while let Some(chunk_res) = byte_stream.next().await {
                        if let Ok(chunk) = chunk_res {
                            if let Ok(text) = std::str::from_utf8(&chunk) {
                                buffer.push_str(text);

                                while let Some(pos) = buffer.find("\n\n") {
                                    let line_block = buffer[..pos].to_string();
                                    buffer = buffer[pos + 2..].to_string();

                                    for line in line_block.lines() {
                                        if let Some(data) = line.strip_prefix("data: ") {
                                            if data.trim() == "[DONE]" {
                                                continue;
                                            }
                                            if let Ok(val) = serde_json::from_str::<Value>(data) {
                                                if let Some(delta) = val.pointer("/choices/0/delta/content").and_then(|c| c.as_str()) {
                                                    let delta_evt = json!({
                                                        "type": "content_block_delta",
                                                        "index": 0,
                                                        "delta": {
                                                            "type": "text_delta",
                                                            "text": delta
                                                        }
                                                    });
                                                    yield Ok(Bytes::from(format!("event: content_block_delta\ndata: {}\n\n", delta_evt)));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    let block_stop = json!({ "type": "content_block_stop", "index": 0 });
                    yield Ok(Bytes::from(format!("event: content_block_stop\ndata: {}\n\n", block_stop)));

                    let msg_delta = json!({
                        "type": "message_delta",
                        "delta": { "stop_reason": "end_turn", "stop_sequence": null },
                        "usage": { "output_tokens": 50 }
                    });
                    yield Ok(Bytes::from(format!("event: message_delta\ndata: {}\n\n", msg_delta)));

                    let msg_stop = json!({ "type": "message_stop" });
                    yield Ok(Bytes::from(format!("event: message_stop\ndata: {}\n\n", msg_stop)));
                };

                let mut resp = Response::new(Body::from_stream(anthropic_stream));
                resp.headers_mut().insert(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("text/event-stream; charset=utf-8"),
                );
                resp
            } else {
                // Non-streaming response translation
                let openai_resp: Value = upstream_resp.json().await.unwrap_or_default();
                let anthropic_resp = translate_openai_to_anthropic(&openai_resp, &actual_model);
                Json(anthropic_resp).into_response()
            }
        }
        Err(err) => json_error(StatusCode::BAD_GATEWAY, &err),
    }
}

// -----------------------------------------------------------------------------------------
// Translation Logic: Anthropic Schema <---> OpenAI Schema
// -----------------------------------------------------------------------------------------
fn translate_anthropic_to_openai(req: &Value, model: &str) -> Value {
    let mut openai_messages = Vec::new();

    // 1. Handle Anthropic System Prompt
    if let Some(sys) = req.get("system") {
        let sys_content = if let Some(s) = sys.as_str() {
            s.to_string()
        } else if let Some(arr) = sys.as_array() {
            arr.iter()
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("\n")
        } else {
            String::new()
        };

        if !sys_content.is_empty() {
            openai_messages.push(json!({
                "role": "system",
                "content": sys_content
            }));
        }
    }

    // 2. Handle Messages (Text, Vision Images & Tool Results)
    if let Some(messages) = req.get("messages").and_then(|v| v.as_array()) {
        for msg in messages {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("user");

            if let Some(content_str) = msg.get("content").and_then(|c| c.as_str()) {
                openai_messages.push(json!({
                    "role": role,
                    "content": content_str
                }));
            } else if let Some(content_arr) = msg.get("content").and_then(|c| c.as_array()) {
                let mut openai_parts = Vec::new();

                for block in content_arr {
                    let block_type = block.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    match block_type {
                        "text" => {
                            if let Some(txt) = block.get("text").and_then(|t| t.as_str()) {
                                openai_parts.push(json!({
                                    "type": "text",
                                    "text": txt
                                }));
                            }
                        }
                        "image" => {
                            // Convert Base64 / URL Image to OpenAI Image URL
                            if let Some(source) = block.get("source") {
                                let mediatype = source.get("media_type").and_then(|m| m.as_str()).unwrap_or("image/jpeg");
                                let data = source.get("data").and_then(|d| d.as_str()).unwrap_or("");
                                openai_parts.push(json!({
                                    "type": "image_url",
                                    "image_url": {
                                        "url": format!("data:{};base64,{}", mediatype, data)
                                    }
                                }));
                            }
                        }
                        "tool_use" => {
                            // Anthropic assistant tool call
                            let tool_id = block.get("id").and_then(|i| i.as_str()).unwrap_or("call_1");
                            let tool_name = block.get("name").and_then(|n| n.as_str()).unwrap_or("");
                            let input_val = block.get("input").cloned().unwrap_or(json!({}));

                            openai_messages.push(json!({
                                "role": "assistant",
                                "tool_calls": [{
                                    "id": tool_id,
                                    "type": "function",
                                    "function": {
                                        "name": tool_name,
                                        "arguments": input_val.to_string()
                                    }
                                }]
                            }));
                        }
                        "tool_result" => {
                            // Anthropic tool output -> OpenAI tool role
                            let tool_id = block.get("tool_use_id").and_then(|i| i.as_str()).unwrap_or("call_1");
                            let content = block.get("content").map(|c| c.to_string()).unwrap_or_default();

                            openai_messages.push(json!({
                                "role": "tool",
                                "tool_call_id": tool_id,
                                "content": content
                            }));
                        }
                        _ => {}
                    }
                }

                if !openai_parts.is_empty() {
                    openai_messages.push(json!({
                        "role": role,
                        "content": openai_parts
                    }));
                }
            }
        }
    }

    let mut payload = json!({
        "model": model,
        "messages": openai_messages,
        "stream": req.get("stream").and_then(|v| v.as_bool()).unwrap_or(false),
        "temperature": req.get("temperature").unwrap_or(&json!(0.7)),
        "max_tokens": req.get("max_tokens").unwrap_or(&json!(4096))
    });

    // 3. Handle Tools / Function Definitions
    if let Some(tools) = req.get("tools").and_then(|t| t.as_array()) {
        let mut openai_tools = Vec::new();
        for t in tools {
            let name = t.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let desc = t.get("description").and_then(|d| d.as_str()).unwrap_or("");
            let schema = t.get("input_schema").cloned().unwrap_or(json!({"type": "object"}));

            openai_tools.push(json!({
                "type": "function",
                "function": {
                    "name": name,
                    "description": desc,
                    "parameters": schema
                }
            }));
        }
        payload["tools"] = json!(openai_tools);
    }

    payload
}

fn translate_openai_to_anthropic(openai: &Value, model: &str) -> Value {
    let mut content = Vec::new();
    let msg_id = format!("msg_{}", uuid::Uuid::new_v4().simple());

    if let Some(choice) = openai.pointer("/choices/0/message") {
        if let Some(text) = choice.get("content").and_then(|c| c.as_str()) {
            if !text.is_empty() {
                content.push(json!({
                    "type": "text",
                    "text": text
                }));
            }
        }

        if let Some(tool_calls) = choice.get("tool_calls").and_then(|t| t.as_array()) {
            for call in tool_calls {
                let call_id = call.get("id").and_then(|i| i.as_str()).unwrap_or("call_1");
                let func_name = call.pointer("/function/name").and_then(|n| n.as_str()).unwrap_or("");
                let args_str = call.pointer("/function/arguments").and_then(|a| a.as_str()).unwrap_or("{}");
                let args_json: Value = serde_json::from_str(args_str).unwrap_or(json!({}));

                content.push(json!({
                    "type": "tool_use",
                    "id": call_id,
                    "name": func_name,
                    "input": args_json
                }));
            }
        }
    }

    json!({
        "id": msg_id,
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": openai.pointer("/usage/prompt_tokens").unwrap_or(&json!(0)),
            "output_tokens": openai.pointer("/usage/completion_tokens").unwrap_or(&json!(0))
        }
    })
}

// -----------------------------------------------------------------------------------------
// Models list endpoint
// -----------------------------------------------------------------------------------------
async fn list_models_handler() -> Json<Value> {
    Json(json!({
        "object": "list",
        "data": [
            { "id": "moonshotai/kimi-k3", "object": "model", "owned_by": "nvidia" },
            { "id": "deepseek-ai/deepseek-v3", "object": "model", "owned_by": "nvidia" },
            { "id": "meta/llama-3.3-70b-instruct", "object": "model", "owned_by": "nvidia" },
            { "id": "mistralai/mistral-large-2-instruct", "object": "model", "owned_by": "nvidia" },
            { "id": "claude-3-5-sonnet-20241022", "object": "model", "owned_by": "anthropic-translated" }
        ]
    }))
}

fn json_error(status: StatusCode, msg: &str) -> Response {
    (
        status,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        Json(json!({ "error": { "message": msg, "type": "proxy_error" } })),
    )
        .into_response()
}
