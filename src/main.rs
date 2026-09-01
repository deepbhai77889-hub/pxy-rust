use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use futures_util::TryStreamExt;
use reqwest::Client;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

struct AppState {
    client: Client,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // High performance connection pool client
    let client = Client::builder()
        .http2_prior_knowledge()
        .tcp_nodelay(true)
        .pool_max_idle_per_host(100)
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .build()
        .expect("Failed to initialize reqwest client");

    let state = Arc::new(AppState { client });

    let app = Router::new()
        .fallback(any(relay_handler))
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await.unwrap();

    info!("🚀 Rust Ultra-Fast Relay Proxy running on http://{}", addr);
    axum::serve(listener, app).await.unwrap();
}

async fn relay_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    req: Request,
) -> Response {
    let (parts, body) = req.into_parts();
    let headers = parts.headers;
    let method = parts.method;

    // Extract target headers
    let target = match headers.get("x-relay-target") {
        Some(t) => match t.to_str() {
            Ok(s) => s.trim_end_matches('/').to_string(),
            Err(_) => return bad_request("Invalid x-relay-target header"),
        },
        None => return bad_request("Missing x-relay-target header"),
    };

    let relay_path = headers
        .get("x-relay-path")
        .and_then(|p| p.to_str().ok())
        .unwrap_or("/");

    let target_url = format!("{}{}", target, relay_path);

    // Filter and prepare headers to forward
    let mut req_headers = HeaderMap::new();
    for (name, value) in &headers {
        let name_str = name.as_str();
        if name_str != "x-relay-target" && name_str != "x-relay-path" && name_str != "host" {
            req_headers.insert(name.clone(), value.clone());
        }
    }

    // Convert Axum streaming body to reqwest streaming body (Zero Copy)
    let body_stream = body.into_data_stream();
    let reqwest_body = reqwest::Body::wrap_stream(body_stream);

    let mut request_builder = state.client.request(method, &target_url).headers(req_headers);

    // Only attach body if method allows
    if parts.method != Method::GET && parts.method != Method::HEAD {
        request_builder = request_builder.body(reqwest_body);
    }

    match request_builder.send().await {
        Ok(upstream_resp) => {
            let status = StatusCode::from_u16(upstream_resp.status().as_u16())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

            let mut resp_headers = HeaderMap::new();
            for (name, value) in upstream_resp.headers() {
                resp_headers.insert(name.clone(), value.clone());
            }

            // Zero-copy stream chunks directly back to client
            let stream = upstream_resp.bytes_stream().map_err(|e| {
                error!("Stream chunk error: {:?}", e);
                std::io::Error::new(std::io::ErrorKind::Other, e)
            });

            let axum_body = Body::from_stream(stream);

            let mut response = Response::new(axum_body);
            *response.status_mut() = status;
            *response.headers_mut() = resp_headers;
            response
        }
        Err(err) => {
            error!("Upstream relay error: {:?}", err);
            (
                StatusCode::BAD_GATEWAY,
                format!("{{\"error\": \"Relay request failed: {}\"}}", err),
            )
                .into_response()
        }
    }
}

fn bad_request(msg: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        format!("{{\"error\": \"{}\"}}", msg),
    )
        .into_response()
}
