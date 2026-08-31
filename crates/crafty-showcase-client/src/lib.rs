//! Minimal HTTP/1.1 client for product showcase gateways (no extra deps).

use std::fmt;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// HTTP response from a showcase gateway.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    /// Status code (e.g. 202).
    pub status: u16,
    /// Raw response bytes (headers + body).
    pub raw: Vec<u8>,
}

impl HttpResponse {
    /// Response body after headers (best-effort split).
    #[must_use]
    pub fn body(&self) -> &[u8] {
        if let Some(pos) = self.raw.windows(4).position(|w| w == b"\r\n\r\n") {
            return &self.raw[pos + 4..];
        }
        &[]
    }

    /// Whether status is 2xx.
    #[must_use]
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// Client error.
#[derive(Debug)]
pub enum ClientError {
    /// TCP or I/O failure.
    Io(std::io::Error),
    /// Response could not be parsed.
    BadResponse(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::BadResponse(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

fn host_only(base: &str) -> String {
    base.trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or(base)
        .to_string()
}

/// POST JSON to `http://{base}{path}`.
pub async fn post_json(
    base: &str,
    path: &str,
    json_body: &str,
) -> Result<HttpResponse, ClientError> {
    let host = host_only(base);
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json_body}",
        json_body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(&host).await?;
    stream.write_all(req.as_bytes()).await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    parse_response(&raw)
}

/// Enqueue a tier C job (`POST /jobs/{stream}` → 202).
pub async fn enqueue_job(
    gateway: &str,
    stream: &str,
    payload: &str,
) -> Result<HttpResponse, ClientError> {
    let body = serde_json::json!({ "payload": payload }).to_string();
    post_json(gateway, &format!("/jobs/{stream}"), &body).await
}

/// Cast to an actor group (`POST /actors/{group}/cast` → 202).
pub async fn cast_actor(
    gateway: &str,
    group: &str,
    payload: &str,
) -> Result<HttpResponse, ClientError> {
    let body = serde_json::json!({ "payload": payload }).to_string();
    post_json(gateway, &format!("/actors/{group}/cast"), &body).await
}

/// Run a keyed saga (`POST /workflows/run` → 200).
pub async fn run_workflow(trigger: &str, saga_id: &str) -> Result<HttpResponse, ClientError> {
    let body = serde_json::json!({ "saga_id": saga_id }).to_string();
    post_json(trigger, "/workflows/run", &body).await
}

/// Resume a keyed saga (`POST /workflows/resume` → 200).
pub async fn resume_workflow(trigger: &str, saga_id: &str) -> Result<HttpResponse, ClientError> {
    let body = serde_json::json!({ "saga_id": saga_id }).to_string();
    post_json(trigger, "/workflows/resume", &body).await
}

/// Send one chat line over WebSocket (`/ws?user=…`).
pub async fn ws_chat(gateway: &str, user: &str, message: &str) -> Result<String, ClientError> {
    let host = host_only(gateway);
    let url = format!("ws://{host}/ws?user={user}");
    let (mut ws, _) = connect_async(&url)
        .await
        .map_err(|e| ClientError::Io(std::io::Error::other(e)))?;
    ws.send(Message::Text(message.into()))
        .await
        .map_err(|e| ClientError::Io(std::io::Error::other(e)))?;
    let frame = ws
        .next()
        .await
        .ok_or_else(|| ClientError::BadResponse("websocket closed without reply".into()))?
        .map_err(|e| ClientError::Io(std::io::Error::other(e)))?;
    match frame {
        Message::Text(text) => Ok(text),
        other => Err(ClientError::BadResponse(format!(
            "unexpected websocket frame: {other:?}"
        ))),
    }
}

fn parse_response(raw: &[u8]) -> Result<HttpResponse, ClientError> {
    let text = String::from_utf8_lossy(raw);
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| ClientError::BadResponse("missing HTTP status line".into()))?;
    Ok(HttpResponse {
        status,
        raw: raw.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_status_from_response() {
        let raw = b"HTTP/1.1 202 Accepted\r\n\r\nok";
        let resp = parse_response(raw).unwrap();
        assert_eq!(resp.status, 202);
        assert!(resp.is_success());
    }
}
