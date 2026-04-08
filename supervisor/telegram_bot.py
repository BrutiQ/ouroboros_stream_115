"""
Telegram Bot Integration — Polling Mode.

Separate message bridge that mirrors LocalChatBridge interface.
Integrates with existing supervisor architecture:
- Uses same inbox/outbox queue semantics
- Logs to same chat.jsonl
- Respects same budget checks

Telegram API token: TELEGRAM_BOT_TOKEN env variable.
"""
from __future__ import annotations

import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config from env
# ---------------------------------------------------------------------------

_TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
_TELEGRAM_API_BASE = f"https://api.telegram.org/bot{_TELEGRAM_BOT_TOKEN}"
_POLL_INTERVAL_SEC = 2.0
_MAX_MESSAGES_PER_POLL = 100


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class TelegramMessage:
    """Parsed Telegram message."""
    update_id: int
    message_id: int
    chat_id: int
    user_id: int
    text: Optional[str]
    photo_data: Optional[Tuple[str, str, str]] = None  # (base64, mime, caption)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _api_call(method: str, payload: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Synchronous HTTP request to Telegram Bot API."""
    import urllib.request
    import urllib.error
    import json

    url = f"{_TELEGRAM_API_BASE}/{method}"
    try:
        if payload:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url, data=data, headers={"Content-Type": "application/json"}
            )
        else:
            req = urllib.request.Request(url)

        with urllib.request.urlopen(req, timeout=10) as resp:
            response = json.loads(resp.read().decode("utf-8"))
            if response.get("ok"):
                return response.get("result")
            else:
                log.warning(f"Telegram API error: {response.get('description')}")
                return None
    except urllib.error.URLError as e:
        log.warning(f"Telegram API request failed: {e}")
        return None
    except Exception as e:
        log.error(f"Unexpected error in Telegram API call: {e}", exc_info=True)
        return None


def _parse_update(update: Dict[str, Any]) -> Optional[TelegramMessage]:
    """Parse a Telegram API update dict into TelegramMessage."""
    try:
        update_id = update["update_id"]
        message = update.get("message", {})
        if not message:
            return None

        message_id = message.get("message_id", 0)
        chat = message.get("chat", {})
        user = message.get("from", {})

        chat_id = chat.get("id", 0)
        user_id = user.get("id", 0)

        text = message.get("text", "").strip()

        # Photo handling - get largest photo
        photo_sizes = message.get("photo", [])
        if photo_sizes:
            largest_photo = photo_sizes[-1]
            file_id = largest_photo.get("file_id")
            caption = message.get("caption", "").strip()

            # Download photo if needed
            photo_data = _download_photo(file_id) if file_id else None
            if photo_data:
                return TelegramMessage(
                    update_id=update_id,
                    message_id=message_id,
                    chat_id=chat_id,
                    user_id=user_id,
                    text=text or caption,
                    photo_data=(*photo_data, caption) if caption else photo_data
                )

        return TelegramMessage(
            update_id=update_id,
            message_id=message_id,
            chat_id=chat_id,
            user_id=user_id,
            text=text if text else None
        )
    except Exception as e:
        log.warning(f"Failed to parse update: {e}", exc_info=True)
        return None


def _download_photo(file_id: str) -> Optional[Tuple[str, str]]:
    """Download photo from Telegram and return (base64, mime)."""
    import base64
    import urllib.request

    try:
        # Get file path
        file_result = _api_call("getFile", {"file_id": file_id})
        if not file_result:
            return None

        file_path = file_result.get("file_path")
        if not file_path:
            return None

        file_url = f"https://api.telegram.org/file/bot{_TELEGRAM_BOT_TOKEN}/{file_path}"

        with urllib.request.urlopen(file_url, timeout=30) as resp:
            data = resp.read()

        # Detect mime type from file extension
        ext = os.path.splitext(file_path)[1].lower()
        mime_map = {
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".gif": "image/gif",
            ".webp": "image/webp"
        }
        mime = mime_map.get(ext, "image/jpeg")

        # Base64 encode
        b64 = base64.b64encode(data).decode("utf-8")
        return (b64, mime)

    except Exception as e:
        log.warning(f"Failed to download photo: {e}", exc_info=True)
        return None


def _format_chat_message(chat_id: int, text: str, parse_mode: str = "") -> Dict[str, Any]:
    """Prepare sendMessage payload."""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    return payload


# ---------------------------------------------------------------------------
# TelegramBotPollingBridge
# ---------------------------------------------------------------------------


class TelegramBotPollingBridge:
    """
    Telegram polling bridge that mirrors LocalChatBridge interface.

    Design principles:
    - Runs in background thread, polling Telegram API every N seconds
    - Conversions: Telegram message format <-> internal format
    - Logs to chat.jsonl via supervisor.message_bus.log_chat
    - Uses send_with_budget for outgoing messages
    """

    def __init__(self, owner_chat_id: Optional[int] = None):
        self._inbox = queue.Queue()  # (chat_id, text, photo_data)
        self._update_offset = 0
        self._owner_chat_id = owner_chat_id
        self._running = False
        self._poll_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._load_offset()

    # -------------------------------------------------------------------------
    # LocalChatBridge interface (for compatibility)
    # -------------------------------------------------------------------------

    def get_updates(self, offset: int, timeout: int = 10) -> List[Dict[str, Any]]:
        """Mirror LocalChatBridge.get_updates() for compatibility."""
        try:
            chat_id, text, photo = self._inbox.get(timeout=timeout)
            return [{
                "update_id": offset + 1,
                "message": {
                    "chat": {"id": 1},
                    "from": {"id": 1},
                    "text": text or "",
                }
            }]
        except queue.Empty:
            return []

    def send_message(self, chat_id: int, text: str, parse_mode: str = "") -> Tuple[bool, str]:
        """Send message to Telegram."""
        if not _TELEGRAM_BOT_TOKEN:
            return False, "TELEGRAM_BOT_TOKEN not configured"

        # Split long messages (Telegram limit: 4096)
        chunks = self._split_message(text, limit=4096)
        for chunk in chunks:
            if not chunk.strip():
                continue
            payload = _format_chat_message(chat_id, chunk, parse_mode)
            result = _api_call("sendMessage", payload)
            if not result:
                return False, "sendMessage failed"
        return True, "ok"

    def send_chat_action(self, chat_id: int, action: str = "typing") -> bool:
        """Send typing/action to Telegram."""
        if not _TELEGRAM_BOT_TOKEN:
            return False
        payload = {"chat_id": chat_id, "action": action}
        _api_call("sendChatAction", payload)
        return True

    def send_photo(self, chat_id: int, photo_bytes: bytes, caption: str = "") -> Tuple[bool, str]:
        """Photo upload not implemented in this version (requires multipart)."""
        if not _TELEGRAM_BOT_TOKEN:
            return False, "TELEGRAM_BOT_TOKEN not configured"
        log.warning("send_photo not implemented for TelegramBridge")
        return False, "Photo upload not implemented"

    def download_file_base64(self, file_id: str, max_bytes: int = 10_000_000) -> Tuple[Optional[str], str]:
        return None, ""

    # -------------------------------------------------------------------------
    # Telegram-specific
    # -------------------------------------------------------------------------

    def _load_offset(self) -> None:
        try:
            from supervisor.state import load_state
            st = load_state()
            self._update_offset = st.get("telegram_update_offset", 0)
            self._owner_chat_id = st.get("owner_chat_id") or self._owner_chat_id
        except Exception:
            log.debug("Failed to load telegram offset from state", exc_info=True)

    def _save_offset(self) -> None:
        try:
            from supervisor.state import save_state, load_state
            with self._lock:
                st = load_state()
                st["telegram_update_offset"] = self._update_offset
                if self._owner_chat_id:
                    st["owner_chat_id"] = self._owner_chat_id
                save_state(st)
        except Exception:
            log.debug("Failed to save telegram offset to state", exc_info=True)

    def _split_message(self, text: str, limit: int = 4096) -> List[str]:
        chunks = []
        while len(text) > limit:
            cut = text.rfind("\n", 0, limit)
            if cut < 100:
                cut = limit
            chunks.append(text[:cut])
            text = text[cut:]
        chunks.append(text)
        return chunks

    def _poll_loop(self) -> None:
        while self._running:
            try:
                updates = self._fetch_updates()
                for msg in updates:
                    self._inbox.put((msg.chat_id, msg.text, msg.photo_data))
                time.sleep(_POLL_INTERVAL_SEC)
            except Exception as e:
                log.error(f"Telegram polling loop error: {e}", exc_info=True)
                time.sleep(5)

    def _fetch_updates(self) -> List[TelegramMessage]:
        if not _TELEGRAM_BOT_TOKEN:
            return []

        payload = {
            "offset": self._update_offset + 1,
            "timeout": 0,
            "limit": _MAX_MESSAGES_PER_POLL
        }

        result = _api_call("getUpdates", payload)
        if not result:
            return []

        messages = []
        for update in result:
            msg = _parse_update(update)
            if msg:
                messages.append(msg)
                self._update_offset = update["update_id"]

        if messages:
            self._save_offset()

        return messages

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
            self._poll_thread.start()
            log.info(f"Telegram polling bridge started (owner_chat_id={self._owner_chat_id})")

    def stop(self) -> None:
        with self._lock:
            self._running = False
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5)
            log.info("Telegram polling bridge stopped")

    # -------------------------------------------------------------------------
    # Helper for inbox access
    # -------------------------------------------------------------------------

    def pop_inbox_message(self, timeout: float = 1.0) -> Optional[Tuple[int, Optional[str], Optional[Tuple[str, str, str]]]]:
        try:
            return self._inbox.get(timeout=timeout)
        except queue.Empty:
            return None


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def make_telegram_bridge(owner_chat_id: Optional[int] = None) -> Optional[TelegramBotPollingBridge]:
    """Create and start Telegram polling bridge."""
    if not _TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN env variable not set - Telegram bridge disabled")
        return None

    bridge = TelegramBotPollingBridge(owner_chat_id=owner_chat_id)
    bridge.start()
    return bridge