"""
WeCom Stream adapter — streaming replies via reply_stream API.

Uses the official wecom-aibot-python-sdk WSClient for WebSocket connection
management and the reply_stream() method for progressive message updates.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import time as _time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from aibot import WSClient, WSClientOptions, generate_req_id
    AIBOT_AVAILABLE = True
except ImportError:
    AIBOT_AVAILABLE = False
    WSClient = None  # type: ignore[assignment,misc]
    WSClientOptions = None  # type: ignore[assignment,misc]
    generate_req_id = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.platforms.helpers import MessageDeduplicator

logger = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://openws.work.weixin.qq.com"
MAX_MESSAGE_LENGTH = 4000
DEDUP_MAX_SIZE = 1000

# Streaming constants (inspired by QwenPaw)
PROCESSING_TEXT = "\U0001f914 Thinking..."
PROCESSING_REFRESH_INTERVAL = 18.0  # seconds; WeCom drops stream if idle > ~20s
PROCESSING_MAX_DURATION = 180.0

# Media upload via WebSocket (WeCom aibot upload protocol)
_UPLOAD_CMD_INIT = "aibot_upload_media_init"
_UPLOAD_CMD_CHUNK = "aibot_upload_media_chunk"
_UPLOAD_CMD_FINISH = "aibot_upload_media_finish"
_UPLOAD_CMDS = (_UPLOAD_CMD_INIT, _UPLOAD_CMD_CHUNK, _UPLOAD_CMD_FINISH)
_UPLOAD_CHUNK_SIZE = 512 * 1024  # 512 KB raw data per chunk
_UPLOAD_ACK_TIMEOUT = 30.0  # seconds to wait for each upload ack

# Media type → WeCom msgtype mapping
_MEDIA_MSGTYPE: Dict[str, str] = {
    "image": "image",
    "voice": "voice",
    "video": "video",
    "file": "file",
}


def check_wecom_stream_requirements() -> bool:
    """Check if the aibot SDK is available."""
    return AIBOT_AVAILABLE


def _coerce_list(value: Any) -> List[str]:
    """Coerce config values into a trimmed string list."""
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _normalize_entry(raw: str) -> str:
    """Normalize allowlist entries."""
    value = str(raw).strip()
    value = re.sub(r"^wecom:", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^(user|group):", "", value, flags=re.IGNORECASE)
    return value.strip()


def _entry_matches(entries: List[str], target: str) -> bool:
    """Case-insensitive allowlist match with * support."""
    normalized_target = str(target).strip().lower()
    for entry in entries:
        normalized = _normalize_entry(entry).lower()
        if normalized == "*" or normalized == normalized_target:
            return True
    return False


class _SdkLogAdapter(logging.LoggerAdapter):
    """Adapt Hermes logger to the aibot SDK\'s logger interface."""

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)


class WeComStreamAdapter(BasePlatformAdapter):
    """WeCom adapter using aibot SDK with reply_stream for streaming replies.

    Streaming flow (what the user sees in WeCom):
      1. User sends message
      2. ``send_typing()`` is called → bubble appears with "\\U0001f914 Thinking..."
      3. ``send(first_chunk)`` is called → Thinking bubble is overwritten with text
      4. ``edit_message(accumulated)`` is called → bubble updates in-place
      5. ``edit_message(final)`` is called → stream is finalized (immutable)

    Non-streaming flow (cron, proactive messages):
      ``send()`` with no prior typing session → final message (finish=True)

    Session states:
      - typing: ``is_typing=True, finished=False`` — Thinking bubble visible
      - streaming: ``is_typing=False, finished=False`` — real content streaming
      - finalized: session removed from ``_stream_sessions``
    """

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    SUPPORTS_MESSAGE_EDITING = True
    REQUIRES_EDIT_FINALIZE = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("wecom_stream"))

        extra = config.extra or {}
        self._bot_id = str(extra.get("bot_id") or os.getenv("WECOM_STREAM_BOT_ID", "") or os.getenv("WECOM_BOT_ID", "")).strip()
        self._secret = str(extra.get("secret") or os.getenv("WECOM_STREAM_SECRET", "") or os.getenv("WECOM_SECRET", "")).strip()
        self._ws_url = str(
            extra.get("websocket_url")
            or extra.get("websocketUrl")
            or os.getenv("WECOM_STREAM_WEBSOCKET_URL", "") or os.getenv("WECOM_WEBSOCKET_URL", DEFAULT_WS_URL)
        ).strip() or DEFAULT_WS_URL

        self._dm_policy = str(extra.get("dm_policy") or os.getenv("WECOM_DM_POLICY", "open")).strip().lower()
        self._allow_from = _coerce_list(
            extra.get("allow_from")
            or extra.get("allowFrom")
            or os.getenv("WECOM_ALLOWED_USERS", "")
        )
        self._group_policy = str(extra.get("group_policy") or os.getenv("WECOM_GROUP_POLICY", "open")).strip().lower()
        self._group_allow_from = _coerce_list(extra.get("group_allow_from") or extra.get("groupAllowFrom"))
        self._groups = extra.get("groups") if isinstance(extra.get("groups"), dict) else {}

        # aibot SDK client
        self._client: Optional["WSClient"] = None
        self._sdk_thread: Optional[_time.monotonic] = None  # track SDK thread
        self._listen_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._dedup = MessageDeduplicator(max_size=DEDUP_MAX_SIZE)

        # Text batching (same as wecom adapter)
        self._text_batch_delay_seconds = float(os.getenv("HERMES_WECOM_TEXT_BATCH_DELAY_SECONDS", "0.6"))
        self._text_batch_split_delay_seconds = float(os.getenv("HERMES_WECOM_TEXT_BATCH_SPLIT_DELAY_SECONDS", "2.0"))
        self._pending_text_batches: Dict[str, MessageEvent] = {}
        self._pending_text_batch_tasks: Dict[str, asyncio.Task] = {}
        self._SPLIT_THRESHOLD = 3900

        # Per-stream state: chat_id -> stream session info
        self._stream_sessions: Dict[str, dict] = {}
        # Per-chat frame cache: chat_id -> last inbound WeCom frame
        self._last_frames: Dict[str, Any] = {}
        # Monotonic counter to detect stale keepalive invocations
        self._keepalive_generation: Dict[str, int] = {}
        # Per-chat turn counter — incremented by send_typing() on each new
        # user message.  is_reuse_live compares session._turn_id against
        # this to detect genuine new turns vs same-turn segment breaks.
        self._turn_id: Dict[str, int] = {}

        # Media upload state
        self._upload_ack_futures: Dict[str, asyncio.Future] = {}
        self._upload_lock: Optional[asyncio.Lock] = None
        self._ws_loop: Optional[asyncio.AbstractEventLoop] = None
        self._on_message_intercepted = False

        # Media download directory for received images/files
        self._media_dir: Path = Path(
            os.getenv("WECOM_MEDIA_DIR") or "~/.hermes/media"
        ).expanduser().resolve()
        self._media_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Connect to WeCom via the aibot SDK."""
        if not AIBOT_AVAILABLE:
            message = "WeCom Stream startup failed: wecom-aibot-python-sdk not installed"
            self._set_fatal_error("wecom_stream_missing_dep", message, retryable=True)
            logger.warning("[%s] %s. Run: pip install wecom-aibot-python-sdk", self.name, message)
            return False
        if not self._bot_id or not self._secret:
            message = "WeCom Stream startup failed: WECOM_STREAM_BOT_ID/WECOM_STREAM_SECRET (or WECOM_BOT_ID/WECOM_SECRET) are required"
            self._set_fatal_error("wecom_stream_missing_creds", message, retryable=True)
            logger.warning("[%s] %s", self.name, message)
            return False

        try:
            self._loop = asyncio.get_running_loop()
            sdk_logger = _SdkLogAdapter(logger, {"extra": {}})
            options = WSClientOptions(
                bot_id=self._bot_id,
                secret=self._secret,
                max_reconnect_attempts=-1,  # infinite reconnect
                logger=sdk_logger,
            )
            self._client = WSClient(options)
            self._client.on("message", self._on_sdk_message)

            # connect() is blocking; run it in a thread
            await self._loop.run_in_executor(None, self._client.connect)

            # Intercept WS frames to route upload acks to waiting futures
            self._install_ws_message_interceptor()

            self._mark_connected()
            self._listen_task = asyncio.create_task(self._sdk_run_loop())
            self._upload_lock = asyncio.Lock()
            logger.info("[%s] Connected via aibot SDK to %s", self.name, self._ws_url)
            return True
        except Exception as exc:
            message = f"WeCom Stream startup failed: {exc}"
            self._set_fatal_error("wecom_stream_connect_error", message, retryable=True)
            logger.error("[%s] Failed to connect: %s", self.name, exc, exc_info=True)
            return False

    async def disconnect(self) -> None:
        """Disconnect from WeCom."""
        self._running = False
        self._mark_disconnected()

        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None

        # Cancel all active typing keepalive tasks
        for chat_id, session in list(self._stream_sessions.items()):
            task = session.get("keepalive_task")
            if task and not task.done():
                task.cancel()
        self._stream_sessions.clear()
        self._last_frames.clear()
        self._keepalive_generation.clear()
        self._upload_ack_futures.clear()
        self._ws_loop = None
        self._on_message_intercepted = False

        if self._client:
            try:
                self._client.disconnect()
            except Exception as exc:
                logger.debug("[%s] SDK disconnect error: %s", self.name, exc)
            self._client = None

        self._dedup.clear()
        logger.info("[%s] Disconnected", self.name)

    async def _sdk_run_loop(self) -> None:
        """Keep the SDK\'s internal event loop alive.

        The aibot SDK\'s run() is blocking. We run it in an executor
        and handle reconnection internally via the SDK\'s built-in
        reconnect logic (max_reconnect_attempts=-1 = infinite).
        """
        try:
            await self._loop.run_in_executor(None, self._client.run)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            if self._running:
                logger.warning("[%s] SDK run loop exited: %s", self.name, exc)
                self._mark_disconnected()

    # ------------------------------------------------------------------
    # Inbound message handling
    # ------------------------------------------------------------------

    def _on_sdk_message(self, frame: Dict[str, Any]) -> None:
        """Handle inbound message from the aibot SDK.

        The SDK calls this synchronously from its own internal thread.
        We must use ``call_soon_threadsafe`` to schedule async processing
        on the main event loop.
        """
        if self._loop is None or self._loop.is_closed():
            logger.error("[%s] SDK message received but no event loop", self.name)
            return
        try:
            self._loop.call_soon_threadsafe(
                self._loop.create_task, self._process_frame(frame)
            )
        except RuntimeError as exc:
            logger.error("[%s] Failed to schedule frame processing: %s", self.name, exc)

    async def _process_frame(self, frame: Dict[str, Any]) -> None:
        """Process a single inbound frame (runs on the event loop)."""
        body = frame.get("body") if isinstance(frame, dict) else {}
        if not isinstance(body, dict):
            return

        msg_id = str(body.get("msgid") or frame.get("headers", {}).get("req_id") or uuid.uuid4().hex)
        if self._dedup.is_duplicate(msg_id):
            logger.debug("[%s] Duplicate message %s ignored", self.name, msg_id)
            return

        sender = body.get("from") if isinstance(body.get("from"), dict) else {}
        sender_id = str(sender.get("userid") or "").strip()
        chat_id = str(body.get("chatid") or sender_id).strip()
        if not chat_id:
            logger.debug("[%s] Missing chat id, skipping message", self.name)
            return

        # Store the frame for later reply_stream calls
        self._last_frames[chat_id] = frame

        is_group = str(body.get("chattype") or "").lower() == "group"
        if is_group:
            if not self._is_group_allowed(chat_id, sender_id):
                logger.debug("[%s] Group %s / sender %s blocked by policy", self.name, chat_id, sender_id)
                return
        elif not self._is_dm_allowed(sender_id):
            logger.debug("[%s] DM sender %s blocked by policy", self.name, sender_id)
            return

        text, reply_text = self._extract_text(body)
        if is_group and text:
            text = re.sub(r"^\@\S+\s*", "", text).strip()

        msgtype = str(body.get("msgtype") or "").lower()
        message_type = self._derive_message_type(body, text)

        # Download image media if present
        media_urls: List[str] = []
        media_types: List[str] = []
        if msgtype == "image":
            img_info = body.get("image") if isinstance(body.get("image"), dict) else {}
            url = img_info.get("url") or ""
            aes_key = img_info.get("aeskey") or ""
            if url:
                local_path = await self._download_media(
                    url, aes_key=aes_key, filename_hint="image.jpg",
                )
                if local_path:
                    media_urls.append(local_path)
                    media_types.append("image")
                    if not text:
                        text = "[image]"
                else:
                    if not text:
                        text = "[image: download failed]"
            else:
                if not text:
                    text = "[image: no url]"

        has_reply_context = bool(reply_text and text)

        if not text and reply_text:
            text = reply_text

        if not text:
            logger.debug("[%s] Empty WeCom message skipped", self.name)
            return

        source = self.build_source(
            chat_id=chat_id,
            chat_type="group" if is_group else "dm",
            user_id=sender_id or None,
            user_name=sender_id or None,
        )

        event = MessageEvent(
            text=text,
            message_type=message_type,
            source=source,
            raw_message=frame,
            message_id=msg_id,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=f"quote:{msg_id}" if has_reply_context else None,
            reply_to_text=reply_text if has_reply_context else None,
            timestamp=datetime.now(tz=timezone.utc),
        )

        # Bump per-chat turn counter HERE (not in send_typing) so that
        # multiple send_typing calls within the same turn — the adapter's
        # own eager call below, plus any gateway-level send_typing calls
        # (e.g. the run.py _handle_message_with_agent patch) — all share
        # the same turn_id.  This prevents the DEDUP logic from mistaking
        # the second call for a new turn and finalizing the first bubble
        # (which would leave an empty orphan bubble behind).
        self._turn_id[chat_id] = self._turn_id.get(chat_id, 0) + 1

        # Eagerly show typing indicator before any batching delay —
        # the user sees "Thinking..." immediately, then real content
        # overwrites it when the agent turn starts streaming.
        # IMPORTANT: we must AWAIT this (not fire-and-forget) to guarantee
        # the typing session exists before handle_message runs. Otherwise,
        # if handle_message completes first, send() sees no session and
        # creates a finished=True session; then the late send_typing() would
        # create an orphaned Thinking bubble that never gets overwritten.
        await self.send_typing(chat_id, metadata={"wecom_frame": frame})

        if message_type == MessageType.TEXT and self._text_batch_delay_seconds > 0:
            self._enqueue_text_event(event)
        else:
            await self.handle_message(event)

    # ------------------------------------------------------------------
    # Outbound message sending (streaming via reply_stream)
    # ------------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send or start a streaming reply via reply_stream.

        Streaming path (called by GatewayStreamConsumer):
          - ``send_typing()`` already created a stream session with Thinking...
          - We overwrite the Thinking bubble with real content (finish=False)
          - ``edit_message()`` will be called next to update and finalize

        Non-streaming path (cron, proactive messages):
          - No prior typing session exists
          - We send a final message (finish=True)
        """
        if not chat_id:
            return SendResult(success=False, error="chat_id is required")
        if not content:
            return SendResult(success=False, error="content is required")

        frame = self._resolve_frame(chat_id, metadata)
        if not frame:
            logger.warning("[%s] No frame available for chat %s, falling back to send_message", self.name, chat_id)
            return await self._proactive_send(chat_id, content)

        try:
            # Check if there's an active stream session (created by send_typing
            # or a segment-break-suppressed edit_message that kept it alive).
            session = self._stream_sessions.get(chat_id)
            is_overwrite = (
                session is not None
                and not session.get("finished")
                and session.get("is_typing") is True
            )
            # Reuse a live (non-finished, non-typing) session created by a
            # prior segment — this is the key to single-bubble across tool
            # boundaries.  The stream_id is still valid on the WeCom side
            # because we suppressed finish=True in edit_message().
            is_reuse_live = (
                not is_overwrite
                and session is not None
                and not session.get("finished")
                and not session.get("is_typing")
            )

            if is_overwrite:
                # Overwrite the Thinking... bubble with real content.
                # Increment generation so any in-flight keepalive reply_stream
                # calls will be detected as stale and discarded.
                self._keepalive_generation[chat_id] = self._keepalive_generation.get(chat_id, 0) + 1
                stream_id = session["stream_id"]
                finish = False  # let deferred finish close it later
                # Cancel the typing keepalive — we're now streaming real content.
                # Note: cancelling an asyncio task does NOT abort an in-flight
                # network call. The keepalive will check _keepalive_generation
                # when it returns and won't do anything harmful.
                keepalive = session.get("keepalive_task")
                if keepalive and not keepalive.done():
                    keepalive.cancel()
                # Mark session as "streaming" (no longer typing) but still active
                session["keepalive_task"] = None
                session["is_typing"] = False
                session["_prefix"] = ""  # first segment — no prefix
                logger.debug("[%s] Overwriting typing bubble with content in %s", self.name, chat_id)
            elif is_reuse_live:
                # Detect new turn via turn_id mismatch: send_typing() bumps
                # _turn_id on every new user message.  If the session was
                # created in a prior turn, finalize the old bubble and start
                # a fresh session for the new turn.
                _cur_turn = self._turn_id.get(chat_id, 0)
                if session.get("_turn_id", 0) != _cur_turn:
                    # New turn — finalize old bubble and create fresh session.
                    _dft = session.get("_deferred_finish_task")
                    if _dft:
                        _dft.cancel()
                        session["_deferred_finish_task"] = None
                    try:
                        await self._reply_stream(
                            frame, stream_id=session["stream_id"],
                            content=session.get("_displayed_content", ""),
                            finish=True,
                        )
                    except Exception:
                        logger.debug("[%s] Failed to finalize old bubble on new-turn detection",
                                     self.name, exc_info=True)
                    session["finished"] = True
                    logger.debug("[%s] New turn detected (turn_id %d→%d) in %s — finalized old bubble",
                                 self.name, session.get("_turn_id", 0), _cur_turn, chat_id)
                    # Create fresh streaming-ready session for the new turn
                    new_stream_id = generate_req_id("stream")
                    self._stream_sessions[chat_id] = {
                        "stream_id": new_stream_id,
                        "frame": frame,
                        "started": _time.monotonic(),
                        "keepalive_task": None,
                        "finished": False,
                        "is_typing": False,
                        "_prefix": "",
                        "_turn_id": _cur_turn,
                    }
                    # Redirect local vars to the new session; keep
                    # is_reuse_live=True so the normal prefix+send path
                    # below works correctly (sets prefix="", finish=False).
                    session = self._stream_sessions[chat_id]
                    stream_id = new_stream_id

                if is_reuse_live:
                    # Genuine segment-break reuse (same turn, tool boundary).
                    # Snapshot the displayed content as _prefix so that
                    # GatewayStreamConsumer's reset _accumulated is compensated:
                    #   displayed = _prefix + stream_consumer_accumulated
                    session["_prefix"] = session.get("_displayed_content", "")
                    stream_id = session["stream_id"]
                    finish = False  # not done yet — let the deferred finish close it
                    logger.debug("[%s] Reusing live stream session in %s (prefix=%d chars)",
                                 self.name, chat_id, len(session["_prefix"]))

            if not is_overwrite and not is_reuse_live:
                # No active typing session — create a new stream
                stream_id = generate_req_id("stream")
                finish = True  # non-streaming: finalize immediately
                self._stream_sessions[chat_id] = {
                    "stream_id": stream_id,
                    "frame": frame,
                    "started": _time.monotonic(),
                    "keepalive_task": None,
                    "finished": True,
                    "is_typing": False,
                    "_turn_id": self._turn_id.get(chat_id, 0),
                }

            # Prepend _prefix (content from prior segments) so the bubble
            # shows the full accumulated text across tool-call boundaries.
            prefix = session.get("_prefix", "") if session else ""
            full_content = (prefix + content)[:MAX_MESSAGE_LENGTH]

            response = await self._client.reply_stream(
                frame,
                stream_id=stream_id,
                content=full_content,
                finish=finish,
            )

            error = self._response_error(response)
            if error:
                # Mark as finished so _keep_typing won't create an orphan
                if session:
                    session["finished"] = True
                else:
                    self._stream_sessions[chat_id] = {
                        "stream_id": stream_id,
                        "frame": frame,
                        "started": _time.monotonic(),
                        "keepalive_task": None,
                        "finished": True,
                        "is_typing": False,
                        "_turn_id": self._turn_id.get(chat_id, 0),
                    }
                return SendResult(success=False, error=error)

            # Track displayed content for cross-segment prefix on reuse.
            if session:
                session["_displayed_content"] = full_content

            return SendResult(
                success=True,
                message_id=stream_id,
                raw_response=response,
            )
        except Exception as exc:
            logger.error("[%s] reply_stream send failed: %s", self.name, exc)
            return SendResult(success=False, error=str(exc))

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit (overwrite) a message via reply_stream.

        Called by GatewayStreamConsumer for progressive streaming updates.
        The message_id is the stream_id from the initial send().

        WeCom adapter keeps the same bubble alive across tool boundaries
        (segment breaks) by suppressing ``finish=True`` on intermediate
        finalizes.  Only the turn-final finalize truly closes the bubble.
        This prevents the gateway's segment-break mechanism from creating
        multiple bubbles on WeCom (one per tool-call boundary).
        """
        if not chat_id:
            return SendResult(success=False, error="chat_id is required")

        session = self._stream_sessions.get(chat_id)
        if not session or session.get("finished"):
            # No active stream session — this can happen if send() was called
            # with finish=True (non-streaming). Fall back to a new send.
            if finalize or not content:
                return SendResult(success=False, error="No active stream session")
            return await self.send(chat_id, content)

        frame = session.get("frame")
        stream_id = session.get("stream_id") or message_id

        if not frame:
            return SendResult(success=False, error="No frame for stream session")

        # WeCom single-bubble strategy:
        # Never send finish=True via edit_message.  Instead, defer the
        # finish to a short timer that fires after the last edit.
        # This prevents segment-break finalizes from closing the bubble
        # prematurely while still ensuring the bubble is finalized after
        # the turn ends (when no more edits arrive).
        #
        # The timer is cancelled and restarted on every edit, so it only
        # fires when edits have truly stopped (turn is complete).
        #
        # We do NOT set session["finished"] here — the deferred timer
        # is the sole authority on finishing the bubble.  This keeps the
        # session alive across segment breaks so is_reuse_live can work.
        self._schedule_deferred_finish(session, stream_id, frame)

        # Prepend _prefix (content from prior segments that was erased by
        # GatewayStreamConsumer._reset_segment_state) so the bubble always
        # shows the full accumulated text.
        prefix = session.get("_prefix", "")
        full_content = (prefix + content)[:MAX_MESSAGE_LENGTH]

        try:
            response = await self._client.reply_stream(
                frame,
                stream_id=stream_id,
                content=full_content,
                finish=False,  # finish deferred to timer (single-bubble strategy)
            )

            error = self._response_error(response)
            if error:
                return SendResult(success=False, error=error)

            # Track displayed content so is_reuse_live can snapshot it
            # as _prefix when the next segment starts.
            session["_displayed_content"] = full_content

            # NOTE: We intentionally do NOT set session["finished"]=True
            # even when finalize=True.  The deferred timer handles it.
            # This keeps the session alive for is_reuse_live after segment
            # breaks.  The timer will fire ~3s after the last edit and
            # send finish=True to WeCom.

            return SendResult(success=True, message_id=stream_id, raw_response=response)
        except Exception as exc:
            logger.error("[%s] reply_stream edit failed: %s", self.name, exc)
            return SendResult(success=False, error=str(exc))

    async def _keep_typing(
        self,
        chat_id: str,
        interval: float = 2.0,
        metadata=None,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        """No-op override: wecom_stream manages its own keepalive.

        The base-class _keep_typing() calls send_typing() every 2 seconds.
        For reply_stream platforms this is harmful: after edit_message(
        finalize=True) marks the session as finished=True (tombstone),
        the loop's next send_typing() call sees the tombstone, decides
        "no active session", and creates a BRAND-NEW orphan "Thinking..."
        bubble that is never overwritten.

        WeCom Stream uses its own _keepalive_typing() task (started by
        send_typing) which is cleanly cancelled when send() begins
        streaming.  This follows the same pattern as QwenPaw's
        _keepalive_processing — no external typing loop needed.
        """
        return

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Show "🤔 Thinking..." via reply_stream.

        This creates a temporary stream session with the Thinking placeholder.
        It will be overwritten by the actual response when send() is called.
        A keepalive task refreshes the placeholder every 18 seconds to prevent
        the WeCom server from dropping the stream (idle timeout is ~20s).

        IMPORTANT: each reply_stream call with a NEW stream_id creates a NEW
        message bubble in WeCom.  If send_typing is called while an active
        session exists (typing OR streaming), we must reuse it — not create another.
        """
        frame = self._resolve_frame(chat_id, metadata)
        if not frame:
            logger.debug("[%s] No frame for typing indicator in %s", self.name, chat_id)
            return

        # ── Dedup: reuse existing active session ──────────────────────
        # Block: if there's ANY active (unfinished) session — whether it's
        # still in typing state or already being streamed — we must NOT
        # create a new bubble. Creating a second bubble would show a
        # duplicate "Thinking..." to the user.
        #
        # NOTE: turn_id is bumped in handle_incoming_message(), NOT here.
        # This ensures that multiple send_typing calls within the same
        # turn (adapter's eager call + gateway's _handle_message_with_agent
        # patch) share the same turn_id and are correctly deduped.
        # DEBUG: log call stack to trace duplicate send_typing calls
        import traceback
        logger.info("[%s] send_typing CALLED for %s (turn %d)\n%s",
                     self.name, chat_id, self._turn_id.get(chat_id, 0),
                     "".join(traceback.format_stack()))
        existing = self._stream_sessions.get(chat_id)
        if existing and not existing.get("finished"):
            # If turn_id changed, the old bubble belongs to a previous turn
            # — finalize it and create a fresh typing session.
            if existing.get("_turn_id", 0) != self._turn_id[chat_id]:
                logger.info("[%s] send_typing NEW TURN (old turn %d != %d) — finalizing old session in %s",
                             self.name, existing.get("_turn_id", 0),
                             self._turn_id[chat_id], chat_id)
                await self._finalize_old_session(chat_id, existing)
                # Fall through to create new typing session below
            else:
                logger.info("[%s] send_typing DEDUP SKIP in %s (is_typing=%s, finished=%s, turn %d)",
                             self.name, chat_id, existing.get("is_typing"),
                             existing.get("finished"), self._turn_id[chat_id])
                return

        try:
            stream_id = generate_req_id("typing")

            # Cancel previous keepalive/session if any (finished/stale ones)
            old_session = self._stream_sessions.pop(chat_id, None)
            if old_session:
                task = old_session.get("keepalive_task")
                if task and not task.done():
                    task.cancel()
            # Reset generation counter for this chat
            self._keepalive_generation[chat_id] = 0

            response = await self._client.reply_stream(
                frame, stream_id=stream_id, content=PROCESSING_TEXT, finish=False,
            )

            error = self._response_error(response)
            if error:
                logger.debug("[%s] Typing indicator failed: %s", self.name, error)
                return

            # Store session and start keepalive
            self._stream_sessions[chat_id] = {
                "stream_id": stream_id,
                "frame": frame,
                "started": _time.monotonic(),
                "keepalive_task": asyncio.create_task(
                    self._keepalive_typing(chat_id, frame, stream_id, generation=0)
                ),
                "finished": False,
                "is_typing": True,
                "_turn_id": self._turn_id.get(chat_id, 0),
            }
            logger.info("[%s] send_typing CREATED session in %s (stream_id=%s, turn %d)", self.name, chat_id, stream_id[:16], self._turn_id[chat_id])
        except Exception as exc:
            logger.debug("[%s] send_typing failed: %s", self.name, exc)

    async def _keepalive_typing(self, chat_id: str, frame: Dict[str, Any], stream_id: str, generation: int = 0) -> None:
        """Periodically refresh the Thinking... placeholder.

        WeCom drops streams that are not updated within ~20 seconds.
        This task refreshes every 18 seconds until cancelled.

        The ``generation`` parameter ensures that if send() cancels this task
        but a reply_stream call was already in-flight, the keepalive checks
        its generation after returning and won't cause issues.  This is a
        belt-and-suspenders defense against the fact that cancelling an
        asyncio task does NOT abort an in-flight network I/O call.
        """
        try:
            while self._running:
                await asyncio.sleep(PROCESSING_REFRESH_INTERVAL)

                # ── Stale generation check ────────────────────────────
                # If send() cancelled the keepalive, it incremented the
                # generation. This keepalive may have a reply_stream call
                # already in-flight. When it returns here, the generation
                # mismatch tells us to stop — the bubble was already
                # overwritten with real content.
                if self._keepalive_generation.get(chat_id, 0) != generation:
                    logger.debug("[%s] Keepalive for %s skipped — stale generation (expected=%d, actual=%d)",
                                 self.name, chat_id, generation, self._keepalive_generation.get(chat_id, -1))
                    return

                # Check if session is still active and still in typing state
                session = self._stream_sessions.get(chat_id)
                if not session or session.get("finished") or session.get("stream_id") != stream_id:
                    return  # session was replaced or finalized
                if not session.get("is_typing"):
                    return  # session was upgraded to streaming by send()

                elapsed = _time.monotonic() - session.get("started", 0)
                if elapsed > PROCESSING_MAX_DURATION:
                    # Force-finish: WeCom may not allow new reply_stream calls
                    # after ~180s on the same stream_id. Let it expire so
                    # send() can start fresh on the next message.
                    logger.warning("[%s] Typing keepalive exceeded max duration for %s", self.name, chat_id)
                    try:
                        await self._client.reply_stream(
                            frame, stream_id=stream_id, content=PROCESSING_TEXT, finish=True,
                        )
                    except Exception:
                        pass
                    # Clean up so send_typing can create a fresh bubble
                    self._stream_sessions.pop(chat_id, None)
                    return

                # Double-check generation right before the API call
                if self._keepalive_generation.get(chat_id, 0) != generation:
                    return

                try:
                    await self._client.reply_stream(
                        frame, stream_id=stream_id, content=PROCESSING_TEXT, finish=False,
                    )
                except Exception as exc:
                    logger.debug("[%s] Keepalive refresh failed: %s", self.name, exc)
                    return

                # Final generation check after API call completes
                if self._keepalive_generation.get(chat_id, 0) != generation:
                    return
        except asyncio.CancelledError:
            return

    # ------------------------------------------------------------------
    # Media upload & sending (WeCom WebSocket upload protocol)
    # ------------------------------------------------------------------

    def _install_ws_message_interceptor(self) -> None:
        """Intercept raw WS frames to route upload acks to waiting futures.

        The aibot SDK's on_message handler ignores frames with upload-related
        req_ids. We wrap it to intercept those before they're discarded.
        """
        if not self._client or self._on_message_intercepted:
            return
        ws_mgr = getattr(self._client, "_ws_manager", None)
        if not ws_mgr:
            logger.warning("[%s] Cannot install WS interceptor: no _ws_manager", self.name)
            return
        orig_on_message = ws_mgr.on_message

        def _ws_raw_handler(frame: Any) -> None:
            req_id = (frame.get("headers") or {}).get("req_id", "") if isinstance(frame, dict) else ""
            if req_id and req_id.startswith(_UPLOAD_CMDS):
                fut = self._upload_ack_futures.get(req_id)
                if fut and not fut.done() and self._loop:
                    self._loop.call_soon_threadsafe(fut.set_result, frame)
                return
            if orig_on_message:
                orig_on_message(frame)

        ws_mgr.on_message = _ws_raw_handler
        self._on_message_intercepted = True
        logger.debug("[%s] WS message interceptor installed for upload acks", self.name)

    def _capture_ws_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        """Capture the SDK's internal event loop for cross-loop sends."""
        if self._ws_loop:
            return self._ws_loop
        # The SDK's WS manager runs tasks via asyncio.ensure_future in its loop.
        # We can discover it from the WS manager's receive task.
        ws_mgr = getattr(self._client, "_ws_manager", None)
        if ws_mgr:
            task = getattr(ws_mgr, "_receive_task", None)
            if task and hasattr(task, "get_loop"):
                self._ws_loop = task.get_loop()
                return self._ws_loop
            # Fallback: try heartbeat task
            task = getattr(ws_mgr, "_heartbeat_task", None)
            if task and hasattr(task, "get_loop"):
                self._ws_loop = task.get_loop()
                return self._ws_loop
        return None

    async def _send_ws_cmd(
        self,
        cmd: str,
        body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Send a raw WebSocket command frame and await the ack.

        Bridges between Hermes' main event loop and the SDK's internal loop.
        Returns the ack frame body dict, or raises on timeout / error.
        """
        if not self._client:
            raise RuntimeError("WeCom client not connected")

        req_id = generate_req_id(cmd)
        main_loop = asyncio.get_running_loop()
        fut: asyncio.Future = main_loop.create_future()

        ws_loop = self._capture_ws_loop()
        if not ws_loop:
            raise RuntimeError("Cannot find SDK WebSocket event loop")

        self._upload_ack_futures[req_id] = fut

        async def _send() -> None:
            ws_mgr = self._client._ws_manager
            await ws_mgr.send({"cmd": cmd, "headers": {"req_id": req_id}, "body": body})

        try:
            send_future = asyncio.run_coroutine_threadsafe(_send(), ws_loop)
            send_future.add_done_callback(
                lambda f: f.result() if not f.cancelled() else None,
            )
            ack = await asyncio.wait_for(
                asyncio.shield(fut),
                timeout=_UPLOAD_ACK_TIMEOUT,
            )
        finally:
            self._upload_ack_futures.pop(req_id, None)

        errcode = ack.get("errcode", -1) if isinstance(ack, dict) else -1
        if errcode != 0:
            errmsg = ack.get("errmsg", "unknown") if isinstance(ack, dict) else "unknown"
            raise RuntimeError(f"wecom upload cmd={cmd} failed: errcode={errcode} errmsg={errmsg}")
        return ack.get("body") or {}

    async def _upload_media(
        self,
        file_path: str,
        media_type: str,
    ) -> Optional[str]:
        """Upload a local file via WebSocket chunks; return media_id.

        Args:
            file_path: Local file path.
            media_type: One of image / voice / video / file.
        Returns:
            media_id string, or None on failure.
        """
        if not self._client or not self._upload_lock:
            return None

        p = Path(file_path)
        if not p.is_file():
            logger.warning("[%s] Upload: file not found: %s", self.name, file_path)
            return None

        data = p.read_bytes()
        total_size = len(data)
        md5 = hashlib.md5(data).hexdigest()
        filename = p.name

        chunks: List[bytes] = [
            data[i: i + _UPLOAD_CHUNK_SIZE]
            for i in range(0, total_size, _UPLOAD_CHUNK_SIZE)
        ]
        total_chunks = len(chunks)

        async with self._upload_lock:
            try:
                # Step 1: init
                init_body = await self._send_ws_cmd(
                    _UPLOAD_CMD_INIT,
                    {
                        "type": media_type,
                        "filename": filename,
                        "total_size": total_size,
                        "total_chunks": total_chunks,
                        "md5": md5,
                    },
                )
                upload_id = init_body.get("upload_id", "")
                if not upload_id:
                    raise RuntimeError("Upload: empty upload_id")

                logger.debug(
                    "[%s] Upload init: upload_id=%s chunks=%d size=%d",
                    self.name, upload_id[:20], total_chunks, total_size,
                )

                # Step 2: chunks
                for idx, chunk in enumerate(chunks):
                    await self._send_ws_cmd(
                        _UPLOAD_CMD_CHUNK,
                        {
                            "upload_id": upload_id,
                            "chunk_index": idx,
                            "base64_data": base64.b64encode(chunk).decode(),
                        },
                    )

                # Step 3: finish
                finish_body = await self._send_ws_cmd(
                    _UPLOAD_CMD_FINISH,
                    {"upload_id": upload_id},
                )
                media_id = finish_body.get("media_id", "")
                if not media_id:
                    raise RuntimeError("Upload: empty media_id")

                logger.info(
                    "[%s] Upload done: media_id=%s type=%s file=%s",
                    self.name, media_id[:20], media_type, filename,
                )
                return media_id
            except Exception as exc:
                logger.error("[%s] Upload failed for %s: %s", self.name, filename, exc)
                return None

    async def send_media(
        self,
        chat_id: str,
        file_path: str,
        media_type: str = "file",
        frame: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a file and send it as a media message.

        Args:
            chat_id: Target chat ID.
            file_path: Local file path to send.
            media_type: One of image / voice / video / file (default: file).
            frame: Optional WeCom frame for reply-based sending.
            metadata: Optional metadata dict (e.g. thread_id).
        """
        if not chat_id:
            return SendResult(success=False, error="chat_id is required")

        media_id = await self._upload_media(file_path, media_type)
        if not media_id:
            return SendResult(success=False, error="Media upload failed")

        msgtype = _MEDIA_MSGTYPE.get(media_type, "file")
        msg_body: Dict[str, Any] = {
            "msgtype": msgtype,
            msgtype: {"media_id": media_id},
        }

        try:
            if frame and self._client:
                response = await self._client.reply(frame, msg_body)
            elif self._client:
                response = await self._client.send_message(chat_id, msg_body)
            else:
                return SendResult(success=False, error="Client not connected")

            error = self._response_error(response)
            if error:
                return SendResult(success=False, error=error)
            return SendResult(
                success=True,
                message_id=str(response.get("headers", {}).get("req_id") or uuid.uuid4().hex[:12]),
                raw_response=response,
            )
        except Exception as exc:
            logger.error("[%s] Send media failed: %s", self.name, exc)
            return SendResult(success=False, error=str(exc))

    # ------------------------------------------------------------------
    # Deferred finish for single-bubble strategy
    # ------------------------------------------------------------------

    _DEFERRED_FINISH_DELAY = 3.0  # seconds after last edit before sending finish=True

    def _schedule_deferred_finish(
        self,
        session: Dict[str, Any],
        stream_id: str,
        frame: Dict[str, Any],
    ) -> None:
        """Schedule a deferred finish=True call.

        The timer is cancelled on every new edit, so it only fires when
        edits have stopped (i.e. the turn is truly over).  This enables
        single-bubble behavior across tool boundaries: segment-break
        finalizes don't close the bubble, and the deferred timer handles
        the real finish after the last edit.
        """
        # Cancel any existing deferred finish
        existing = session.get("_deferred_finish_task")
        if existing and not existing.done():
            existing.cancel()
        session["_deferred_finish_task"] = asyncio.ensure_future(
            self._deferred_finish(session, stream_id, frame)
        )

    async def _deferred_finish(
        self,
        session: Dict[str, Any],
        stream_id: str,
        frame: Dict[str, Any],
    ) -> None:
        """Send finish=True after a delay, unless cancelled by a new edit."""
        try:
            await asyncio.sleep(self._DEFERRED_FINISH_DELAY)
        except asyncio.CancelledError:
            return
        if session.get("finished"):
            return
        logger.debug("[%s] Deferred finish for stream_id=%s", self.name, stream_id[:20])
        # Send the last displayed content with finish=True so WeCom
        # shows the final text (not an empty bubble).
        last_content = session.get("_displayed_content", "")
        try:
            await self._client.reply_stream(frame, stream_id=stream_id, content=last_content, finish=True)
            session["finished"] = True
            task = session.get("keepalive_task")
            if task and not task.done():
                task.cancel()
        except Exception:
            logger.warning("[%s] Deferred finish FAILED for stream_id=%s — session will stay finished=False!",
                            self.name, stream_id[:20], exc_info=True)

    async def _finalize_old_session(
        self, chat_id: str, session: Dict[str, Any],
    ) -> None:
        """Immediately finalize a stale session (finish=True + cleanup).

        Called when send_typing detects that the existing session belongs
        to a previous turn (turn_id mismatch).  Unlike _deferred_finish,
        this runs immediately — no delay.
        """
        # Cancel any pending deferred-finish timer
        dft = session.get("_deferred_finish_task")
        if dft and not dft.done():
            dft.cancel()
        session["_deferred_finish_task"] = None

        stream_id = session.get("stream_id")
        frame = session.get("frame")
        if stream_id and frame:
            try:
                last_content = session.get("_displayed_content", "")
                await self._client.reply_stream(
                    frame, stream_id=stream_id, content=last_content, finish=True,
                )
            except Exception:
                logger.debug("[%s] _finalize_old_session reply_stream failed", self.name, exc_info=True)

        session["finished"] = True
        task = session.get("keepalive_task")
        if task and not task.done():
            task.cancel()

        # Remove from active sessions so the new typing path can create fresh
        self._stream_sessions.pop(chat_id, None)

    # ------------------------------------------------------------------
    # Proactive / non-reply sends (fallback)
    # ------------------------------------------------------------------

    async def _proactive_send(self, chat_id: str, content: str) -> SendResult:
        """Send a proactive message using send_message (no reply context).

        Used as fallback when no inbound frame is available (e.g. cron delivery).
        """
        try:
            response = await self._client.send_message(
                chat_id,
                {
                    "msgtype": "markdown",
                    "markdown": {"content": content[:MAX_MESSAGE_LENGTH]},
                },
            )
            error = self._response_error(response)
            if error:
                return SendResult(success=False, error=error)
            return SendResult(
                success=True,
                message_id=str(response.get("headers", {}).get("req_id") or uuid.uuid4().hex[:12]),
                raw_response=response,
            )
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_frame(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Get the WeCom frame for reply_stream calls.

        Priority:
        1. metadata["wecom_frame"] — if gateway passes it
        2. self._last_frames[chat_id] — stored from the last inbound message
        """
        if metadata and isinstance(metadata, dict):
            frame = metadata.get("wecom_frame")
            if frame and isinstance(frame, dict):
                return frame
        return self._last_frames.get(chat_id)

    @staticmethod
    def _response_error(response: Any) -> Optional[str]:
        """Extract error from a WeCom response frame."""
        if not response or not isinstance(response, dict):
            return "Empty response"
        body = response.get("body") if isinstance(response.get("body"), dict) else {}
        errcode = body.get("errcode", 0)
        if errcode and errcode != 0:
            errmsg = body.get("errmsg", "unknown error")
            return f"errcode={errcode}: {errmsg}"
        return None

    # ------------------------------------------------------------------
    # Media download (image/file from WeCom)
    # ------------------------------------------------------------------

    async def _download_media(
        self,
        url: str,
        aes_key: str = "",
        filename_hint: str = "file.bin",
    ) -> Optional[str]:
        """Download (and optionally decrypt) WeCom media; return local path."""
        if not self._client:
            return None
        try:
            async def _do_download():
                return await self._client.download_file(url, aes_key or None)

            ws_loop = self._capture_ws_loop()
            if ws_loop and ws_loop is not asyncio.get_running_loop():
                future = asyncio.run_coroutine_threadsafe(_do_download(), ws_loop)
                data, filename = future.result(timeout=30)
            else:
                data, filename = await _do_download()
            fn = filename or filename_hint
            hint_ext = Path(filename_hint).suffix
            if hint_ext and Path(fn).suffix in ("", ".bin", ".file"):
                fn = (Path(fn).stem or "file") + hint_ext
            self._media_dir.mkdir(parents=True, exist_ok=True)
            safe_name = (
                "".join(
                    c
                    for c in fn.replace("企业微信截图", "screenshot")
                    if c.isalnum() or c in "-_."
                )
                or "media"
            )
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            path = self._media_dir / f"wecom_{url_hash}_{safe_name}"
            path.write_bytes(data)
            logger.info("[%s] Downloaded media to %s", self.name, path)
            return str(path)
        except Exception:
            logger.exception("[%s] _download_media failed url=%s", self.name, url[:60])
            return None

    # ------------------------------------------------------------------
    # Text extraction (simplified from wecom adapter)
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_text(body: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        """Extract plain text and quoted text from a callback payload."""
        text_parts: List[str] = []
        reply_text: Optional[str] = None
        msgtype = str(body.get("msgtype") or "").lower()

        if msgtype == "mixed":
            _raw_mixed = body.get("mixed")
            mixed = _raw_mixed if isinstance(_raw_mixed, dict) else {}
            _raw_items = mixed.get("msg_item")
            items = _raw_items if isinstance(_raw_items, list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                if str(item.get("msgtype") or "").lower() == "text":
                    _raw_text = item.get("text")
                    text_block = _raw_text if isinstance(_raw_text, dict) else {}
                    content = str(text_block.get("content") or "").strip()
                    if content:
                        text_parts.append(content)
        else:
            text_block = body.get("text") if isinstance(body.get("text"), dict) else {}
            content = str(text_block.get("content") or "").strip()
            if content:
                text_parts.append(content)

            if msgtype == "voice":
                voice_block = body.get("voice") if isinstance(body.get("voice"), dict) else {}
                voice_text = str(voice_block.get("content") or "").strip()
                if voice_text:
                    text_parts.append(voice_text)

        quote = body.get("quote") if isinstance(body.get("quote"), dict) else {}
        quote_type = str(quote.get("msgtype") or "").lower()
        if quote_type == "text":
            quote_text = quote.get("text") if isinstance(quote.get("text"), dict) else {}
            reply_text = str(quote_text.get("content") or "").strip() or None
        elif quote_type == "voice":
            quote_voice = quote.get("voice") if isinstance(quote.get("voice"), dict) else {}
            reply_text = str(quote_voice.get("content") or "").strip() or None

        return "\n".join(part for part in text_parts if part).strip(), reply_text

    @staticmethod
    def _derive_message_type(body: Dict[str, Any], text: str = "") -> MessageType:
        """Derive the message type from the payload."""
        msgtype = str(body.get("msgtype") or "").lower()
        if msgtype in ("image",):
            return MessageType.PHOTO
        if msgtype in ("file", "appmsg"):
            return MessageType.DOCUMENT
        if msgtype in ("voice",):
            return MessageType.VOICE
        if msgtype in ("video",):
            return MessageType.VIDEO
        return MessageType.TEXT

    # ------------------------------------------------------------------
    # Access control (same as wecom adapter)
    # ------------------------------------------------------------------

    def _is_dm_allowed(self, sender_id: str) -> bool:
        if self._dm_policy == "disabled":
            return False
        if self._dm_policy == "open":
            return True
        return _entry_matches(self._allow_from, sender_id)

    def _is_group_allowed(self, group_id: str, sender_id: str) -> bool:
        if self._group_policy == "disabled":
            return False
        if self._group_policy == "open":
            return True
        if _entry_matches(self._group_allow_from, group_id):
            return True
        group_rules = self._groups.get(group_id, {})
        if isinstance(group_rules, dict):
            group_allow = _coerce_list(group_rules.get("allow_from", []))
            if group_allow:
                return _entry_matches(group_allow, sender_id)
        return False

    # ------------------------------------------------------------------
    # Text batching (handles WeCom client-side splits)
    # ------------------------------------------------------------------

    def _text_batch_key(self, event: MessageEvent) -> str:
        from gateway.session import build_session_key
        return build_session_key(
            event.source,
            group_sessions_per_user=self.config.extra.get("group_sessions_per_user", True),
            thread_sessions_per_user=self.config.extra.get("thread_sessions_per_user", False),
        )

    def _enqueue_text_event(self, event: MessageEvent) -> None:
        key = self._text_batch_key(event)
        existing = self._pending_text_batches.get(key)
        chunk_len = len(event.text or "")
        if existing is None:
            event._last_chunk_len = chunk_len  # type: ignore[attr-defined]
            self._pending_text_batches[key] = event
        else:
            if event.text:
                existing.text = f"{existing.text}\n{event.text}" if existing.text else event.text
            existing._last_chunk_len = chunk_len  # type: ignore[attr-defined]

        prior_task = self._pending_text_batch_tasks.get(key)
        if prior_task and not prior_task.done():
            prior_task.cancel()
        self._pending_text_batch_tasks[key] = asyncio.create_task(
            self._flush_text_batch(key)
        )

    async def _flush_text_batch(self, key: str) -> None:
        current_task = asyncio.current_task()
        try:
            pending = self._pending_text_batches.get(key)
            last_len = getattr(pending, "_last_chunk_len", 0) if pending else 0
            delay = self._text_batch_split_delay_seconds if last_len >= self._SPLIT_THRESHOLD else self._text_batch_delay_seconds
            await asyncio.sleep(delay)

            if self._pending_text_batch_tasks.get(key) is not current_task:
                return
            event = self._pending_text_batches.pop(key, None)
            if not event:
                return
            logger.info("[WeComStream] Flushing text batch %s (%d chars)", key, len(event.text or ""))
            await self.handle_message(event)
        finally:
            if self._pending_text_batch_tasks.get(key) is not current_task:
                self._pending_text_batch_tasks.pop(key, None)

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {
            "name": chat_id,
            "type": "group" if chat_id and chat_id.lower().startswith("group") else "dm",
        }


# ------------------------------------------------------------------
# Plugin entry point
# ------------------------------------------------------------------

def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system.

    ``ctx`` is a :class:`PluginContext` that provides ``register_platform()``.
    """
    import os as _os

    def _factory(cfg):
        return WeComStreamAdapter(cfg)

    ctx.register_platform(
        name="wecom_stream",
        label="WeCom Stream",
        adapter_factory=_factory,
        check_fn=check_wecom_stream_requirements,
        validate_config=lambda cfg: bool(
            cfg.extra.get("bot_id") or _os.getenv("WECOM_STREAM_BOT_ID", "") or _os.getenv("WECOM_BOT_ID", "")
        ),
        required_env=["WECOM_STREAM_BOT_ID", "WECOM_STREAM_SECRET"],
        install_hint="pip install wecom-aibot-python-sdk",
        # Cron home-channel delivery
        cron_deliver_env_var="WECOM_HOME_CHANNEL",
        # WeCom hard limit per message
        max_message_length=4000,
        # Display
        emoji="\U0001f4ac",
        allow_update_command=True,
        pii_safe=False,
        platform_hint="",
    )
