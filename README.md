# wecom_stream

WeCom (企业微信) streaming adapter for [Hermes Agent](https://github.com/nousresearch/hermes-agent).  
Uses the official [wecom-aibot-python-sdk](https://github.com/nousresearch/wecom-aibot-python-sdk) `reply_stream` API for real-time "Thinking..." indicators and progressive text streaming — all in a **single message bubble**.

## Features

- **Streaming replies**: Real-time progressive text updates via `reply_stream`
- **Thinking indicator**: Shows 🤔 Thinking... immediately on message receipt, then smoothly transitions to the actual response
- **Single-bubble strategy**: Tool-call boundaries don't split into multiple bubbles — the entire turn stays in one message
- **Image support**: Receives and passes image attachments to the agent
- **Group & DM**: Works in both group chats and direct messages, with allow-list policies
- **Keepalive**: Prevents WeCom's ~20s idle timeout from dropping the stream during long agent turns
- **Drop-in replacement**: Reuses the same `WECOM_BOT_ID` / `WECOM_SECRET` as the built-in `wecom` adapter

## Install

### 1. Install the SDK dependency

```bash
pip install wecom-aibot-python-sdk
```

### 2. Copy the plugin

```bash
cp -r wecom_stream/ ~/.hermes/plugins/
```

### 3. Configure environment variables

Set in `~/.hermes/.env` or your shell profile:

| Variable | Required | Description |
|---|---|---|
| `WECOM_BOT_ID` | ✅ | WeCom AI Bot ID |
| `WECOM_SECRET` | ✅ | WeCom AI Bot Secret |
| `WECOM_WEBSOCKET_URL` | ❌ | WebSocket URL (default: `wss://openws.work.weixin.qq.com`) |
| `WECOM_ALLOWED_USERS` | ❌ | Comma-separated user IDs for DM access control |
| `WECOM_HOME_CHANNEL` | ❌ | Default chat ID for cron/notification delivery |
| `WECOM_HOME_CHANNEL_NAME` | ❌ | Display name for the home channel |

### 4. Enable in Hermes config

In `~/.hermes/config.yaml`, under `platforms`:

```yaml
platforms:
  wecom_stream:
    enabled: true
```

If the built-in `wecom` adapter was previously enabled, disable it:

```yaml
platforms:
  wecom:
    enabled: false
  wecom_stream:
    enabled: true
```

### 5. Restart the Gateway

```bash
hermes gateway restart
```

## How it works

```
User sends message
  ↓
adapter.handle_incoming_message()
  ├─ Bumps turn_id (once per message)
  ├─ Calls send_typing() → creates bubble with "Thinking..."
  │     └─ Keepalive task refreshes every 18s to prevent idle timeout
  ├─ Forwards to gateway → agent runs
  ↓
Agent starts streaming response
  ↓
adapter.send()
  ├─ Detects is_overwrite (typing bubble exists)
  ├─ Cancels keepalive, overwrites "Thinking..." with real content
  └─ finish=False → deferred timer manages finalization
  ↓
adapter.edit_message() (progressive updates)
  ├─ Appends new content to the same bubble
  └─ Resets deferred finish timer on each update
  ↓
Agent finishes → deferred timer fires → finish=True → bubble closed
```

## File structure

```
wecom_stream/
├── __init__.py      # Plugin entry point
├── adapter.py       # WeComStreamAdapter (~1,500 LOC)
├── plugin.yaml      # Plugin metadata & env var declarations
└── README.md
```

## License

MIT
