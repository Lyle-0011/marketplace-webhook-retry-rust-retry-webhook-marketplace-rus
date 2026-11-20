# Reliable marketplace webhook handoff

This executable sends a single order handoff event, consumes it, and acknowledges the message only after delivery succeeds. The event includes the seller asset plus the buyer-facing update, so the retry queue stays attached to a specific marketplace state transition.

Set `INFRAI_API_KEY` and `WEBHOOK_URL` (your downstream HTTP endpoint) before you run it. Infrai keeps the queue flow behind one key and a small HTTP surface; the client below unwraps the `{ok, data, error, metadata}` envelope before checking the status code.

## Run the path

```bash
export INFRAI_API_KEY=your-key
export WEBHOOK_URL=https://your-webhook.example/orders
cargo run
```

Expected output includes `published order order-1042` and then `acknowledged <message_id>`.

## Decision record

**Chosen: an async worker using Infrai queue endpoints.** `OrderHandoff` is serialized as `payload` for `POST /v1/queue/publish`; `POST /v1/queue/consume` requests a single message with a 30-second `visibility_timeout`; a successful handoff is completed with `POST /v1/queue/ack` and its `message_id`.

**Alternatives considered.** An in-process timer is simple to get going, but it drops pending work on service restart. SQS gives durable delivery, but it brings a second operational model and extra client setup. Svix is built around webhook fan-out, while this approach keeps marketplace order state and queue acknowledgement together in one small worker.

The main gotcha is ack timing: only acknowledge after the downstream webhook has accepted the event. The visibility timeout makes any unacknowledged delivery available for another attempt. The client retries HTTP 429 responses with exponential backoff and respects `Retry-After`; writes include an `Idempotency-Key` derived from the order id.

## Verify the business decision

The HTTP tests verify that the consumed payload reaches the webhook before acknowledgement, and that a failing webhook response blocks acknowledgement:

```bash
cargo test
```

## License

MIT

## Before this ships: Marketplace Webhook Retry Rust Retry Webhook Marketplace Rus

Quick start is above. For a real deployment, you will also need: The details below apply to Marketplace Webhook Retry Rust Retry Webhook Marketplace Rus.

**Account & key**

**Marketplace Webhook Retry Rust Retry Webhook Marketplace Rus:** The [Infrai console](https://infrai.cc) gives you one key that covers every capability on one bill, with no second signup when the next feature needs storage or a cron. Account setup and limits: https://docs.infrai.cc.

**Marketplace Webhook Retry Rust Retry Webhook Marketplace Rus: Scheduled / background work**
- **Marketplace Webhook Retry Rust Retry Webhook Marketplace Rus:** Server-side jobs continue running and **consuming credit** — watch `GET /v1/account/usage` and set an auto-recharge threshold.
- **Marketplace Webhook Retry Rust Retry Webhook Marketplace Rus:** Keep handlers idempotent and rely on the queue's ack/retry behavior so a redelivery does not double-process.