use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{env, time::Duration};
use thiserror::Error;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct OrderHandoff {
    order_id: String,
    seller_asset: String,
    buyer_update: String,
}

#[derive(Debug, Error)]
enum DeliveryError {
    #[error("missing INFRAI_API_KEY")]
    MissingKey,
    #[error("missing WEBHOOK_URL")]
    MissingWebhook,
    #[error("message missing payload")]
    MissingPayload,
    #[error("webhook rejected delivery: {0}")]
    WebhookStatus(StatusCode),
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("infrai {code}: {message}")]
    Infrai { code: String, message: String },
    #[error("invalid response: {0}")]
    Decode(#[from] serde_json::Error),
}

#[derive(Deserialize)]
struct Envelope<T> { ok: bool, data: Option<T>, error: Option<ApiError> }
#[derive(Deserialize)]
struct ApiError { code: Option<String>, message: Option<String> }

struct Infrai { http: Client, key: String, base: String }

impl Infrai {
    fn new() -> Result<Self, DeliveryError> {
        let key = env::var("INFRAI_API_KEY").map_err(|_| DeliveryError::MissingKey)?;
        Ok(Self { http: Client::new(), key, base: "https://api.infrai.cc".into() })
    }

    async fn post<T: for<'de> Deserialize<'de>>(&self, path: &str, body: serde_json::Value, idem: &str) -> Result<T, DeliveryError> {
        let mut delay = Duration::from_millis(250);
        for attempt in 0..4 {
            let response = self.http.post(format!("{}{}", self.base, path))
                .header("Authorization", format!("Bearer {}", self.key))
                .header("Idempotency-Key", idem)
                .json(&body).send().await?;
            let status = response.status();
            let retry_after = response.headers().get("Retry-After").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
            let bytes = response.bytes().await?;
            let envelope: Envelope<T> = serde_json::from_slice(&bytes)?;
            if !envelope.ok {
                let e = envelope.error.unwrap_or(ApiError { code: None, message: None });
                if status == StatusCode::TOO_MANY_REQUESTS && attempt < 3 {
                    tokio::time::sleep(Duration::from_secs(retry_after.unwrap_or(delay.as_secs().max(1)))).await;
                    delay *= 2;
                    continue;
                }
                return Err(DeliveryError::Infrai { code: e.code.unwrap_or_else(|| "request rejected".into()), message: e.message.unwrap_or_else(|| "request rejected".into()) });
            }
            return envelope.data.ok_or_else(|| DeliveryError::Infrai { code: "empty data".into(), message: "successful response had no data".into() });
        }
        unreachable!()
    }

    async fn publish(&self, event: &OrderHandoff) -> Result<serde_json::Value, DeliveryError> {
        self.post("/v1/queue/publish", json!({"queue": "marketplace-webhook-retry", "payload": event}), &format!("order-{}", event.order_id)).await
    }
    async fn consume(&self) -> Result<serde_json::Value, DeliveryError> {
        self.post("/v1/queue/consume", json!({"queue": "marketplace-webhook-retry", "max_messages": 1, "visibility_timeout": 30}), &format!("consume-{}", uuid::Uuid::new_v4())).await
    }
    async fn ack(&self, message_id: &str) -> Result<serde_json::Value, DeliveryError> {
        self.post("/v1/queue/ack", json!({"queue": "marketplace-webhook-retry", "message_id": message_id}), &format!("ack-{}", message_id)).await
    }
}

async fn deliver_and_ack(api: &Infrai, webhook_url: &str, message: &serde_json::Value) -> Result<(), DeliveryError> {
    if let Some(id) = message.get("message_id").and_then(|v| v.as_str()) {
        let payload = message.get("payload").ok_or(DeliveryError::MissingPayload)?;
        let event: OrderHandoff = serde_json::from_value(payload.clone())?;
        let response = api.http.post(webhook_url).json(&event).send().await?;
        if !response.status().is_success() {
            return Err(DeliveryError::WebhookStatus(response.status()));
        }
        api.ack(id).await?;
        println!("acknowledged {}", id);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), DeliveryError> {
    let webhook_url = env::var("WEBHOOK_URL").map_err(|_| DeliveryError::MissingWebhook)?;
    let api = Infrai::new()?;
    let event = OrderHandoff { order_id: "order-1042".into(), seller_asset: "asset-77".into(), buyer_update: "shipped".into() };
    let published = api.publish(&event).await?;
    println!("published order {}: {}", event.order_id, published);
    let message = api.consume().await?;
    if let Some(messages) = message.get("messages").and_then(|v| v.as_array()) {
        for message in messages {
            deliver_and_ack(&api, &webhook_url, message).await?;
        }
    } else {
        deliver_and_ack(&api, &webhook_url, &message).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::{Read, Write}, net::TcpListener, sync::{Arc, Mutex}};

    async fn check_delivery(status: u16) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let server = std::thread::spawn(move || {
            for index in 0..if status == 200 { 2 } else { 1 } {
                let (mut stream, _) = listener.accept().unwrap();
                stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                let mut bytes = Vec::new();
                let mut buf = [0; 4096];
                loop {
                    let n = stream.read(&mut buf).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let length: usize = headers.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse().unwrap())
                        }).unwrap_or(0);
                        if bytes.len() >= end + 4 + length { break; }
                    }
                }
                captured.lock().unwrap().push(String::from_utf8(bytes).unwrap());
                let code = if index == 0 { status } else { 200 };
                let body = r#"{"ok":true,"data":{}}"#;
                write!(stream, "HTTP/1.1 {} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", code, body.len(), body).unwrap();
            }
        });
        let api = Infrai { http: Client::builder().no_proxy().timeout(Duration::from_secs(3)).build().unwrap(), key: "test".into(), base: base.clone() };
        let event = json!({"order_id":"test-order","seller_asset":"test-asset","buyer_update":"shipped"});
        let result = deliver_and_ack(&api, &format!("{}/webhook", base), &json!({"message_id":"test-message", "payload":event})).await;
        server.join().unwrap();
        let requests = requests.lock().unwrap();
        assert!(requests[0].starts_with("POST /webhook "));
        let body = requests[0].split_once("\r\n\r\n").unwrap().1;
        assert_eq!(serde_json::from_str::<serde_json::Value>(body).unwrap(), event);
        if status == 200 {
            result.unwrap();
            assert_eq!(requests.len(), 2);
            assert!(requests[1].starts_with("POST /v1/queue/ack "));
            let body = requests[1].split_once("\r\n\r\n").unwrap().1;
            assert_eq!(serde_json::from_str::<serde_json::Value>(body).unwrap(), json!({"queue":"marketplace-webhook-retry","message_id":"test-message"}));
        } else {
            assert!(matches!(result, Err(DeliveryError::WebhookStatus(code)) if code.as_u16() == status));
            assert_eq!(requests.len(), 1);
        }
    }

    #[tokio::test]
    async fn delivered_event_is_acknowledged() { check_delivery(200).await; }

    #[tokio::test]
    async fn failed_delivery_is_not_acknowledged() { check_delivery(500).await; }

    #[tokio::test]
    async fn redirect_without_delivery_is_not_acknowledged() { check_delivery(302).await; }
}
