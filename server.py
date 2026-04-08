"""
Ouroboros Agent Server — Self-editable entry point.

This file lives in REPO_DIR and can be modified by the agent.
It runs as a subprocess of the launcher, serving the web UI and
coordinating the supervisor/worker system.

Starlette + uvicorn on localhost:{PORT}.
"""

import asyncio
import json
import logging
import os
import pathlib
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse, FileResponse
from starlette.routing import Route, Mount, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket, WebSocketDisconnect

import uvicorn

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_DIR = pathlib.Path(os.environ.get("OUROBOROS_REPO_DIR", pathlib.Path(__file__).parent))
DATA_DIR = pathlib.Path(os.environ.get("OUROBOROS_DATA_DIR",
    pathlib.Path.home() / "Ouroboros" / "data"))
PORT = int(os.environ.get("OUROBOROS_SERVER_PORT", "8765"))

sys.path.insert(0, str(REPO_DIR))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_log_dir = DATA_DIR / "logs"
_log_dir.mkdir(parents=True, exist_ok=True)
from logging.handlers import RotatingFileHandler
_file_handler = RotatingFileHandler(
    _log_dir / "server.log", maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter(_LOG_FORMAT))
logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT, handlers=[_file_handler, logging.StreamHandler()])
log = logging.getLogger("server")

# ---------------------------------------------------------------------------
# Restart signal
# ---------------------------------------------------------------------------
RESTART_EXIT_CODE = 42
PANIC_EXIT_CODE = 99
_restart_requested = threading.Event()

# ---------------------------------------------------------------------------
# WebSocket connections manager
# ---------------------------------------------------------------------------
_ws_clients: List[WebSocket] = []
_ws_lock = threading.Lock()


async def broadcast_ws(msg: dict) -> None:
    """Send a message to all connected WebSocket clients."""
    data = json.dumps(msg, ensure_ascii=False, default=str)
    with _ws_lock:
        clients = list(_ws_clients)
    dead = []
    for ws in clients:
        try:
            await ws.send_text(data)
        except Exception:
            dead.append(ws)
    if dead:
        with _ws_lock:
            for ws in dead:
                try:
                    _ws_clients.remove(ws)
                except ValueError:
                    pass


def broadcast_ws_sync(msg: dict) -> None:
    """Thread-safe sync wrapper for broadcasting.

    Uses the saved _event_loop reference (set in startup_event) rather than
    asyncio.get_event_loop(), which is unreliable from non-main threads
    in Python 3.10+.
    """
    loop = _event_loop
    if loop is None:
        return
    try:
        asyncio.run_coroutine_threadsafe(broadcast_ws(msg), loop)
    except RuntimeError:
        pass


# ---------------------------------------------------------------------------
# Settings (single source of truth: ouroboros.config)
# ---------------------------------------------------------------------------
from ouroboros.config import (
    SETTINGS_DEFAULTS as _SETTINGS_DEFAULTS,
    load_settings, save_settings, apply_settings_to_env as _apply_settings_to_env,
)


# ---------------------------------------------------------------------------
# Supervisor integration
# ---------------------------------------------------------------------------
_supervisor_ready = threading.Event()
_supervisor_error: Optional[str] = None
_event_loop: Optional[asyncio.AbstractEventLoop] = None


def _run_supervisor(settings: dict) -> None:
    """Initialize and run the supervisor loop. Called in a background thread."""
    global _supervisor_error

    _apply_settings_to_env(settings)

    try:
        from supervisor.message_bus import init as bus_init
        from supervisor.telegram_bot import make_telegram_bridge
        from supervisor.message_bus import CompositeBridge
        from supervisor.message_bus import LocalChatBridge

        bridge = LocalChatBridge()
        bridge._broadcast_fn = broadcast_ws_sync

        from ouroboros.utils import set_log_sink
        set_log_sink(bridge.push_log)

        bus_init(
            drive_root=DATA_DIR,
            total_budget_limit=float(settings.get("TOTAL_BUDGET", 1000.0)),
            budget_report_every=10,
            chat_bridge=bridge,
        )

        # Telegram Bot Integration (Polling Mode)
        telegram_bot_enabled = bool(settings.get("TELEGRAM_ENABLED", False))
        telegram_bot = None
        if telegram_bot_enabled:
            telegram_bot = make_telegram_bridge()
            if telegram_bot:
                bridge = CompositeBridge([bridge, telegram_bot])
                log.info("CompositeBridge created: LocalChatBridge + TelegramBotPollingBridge")
            else:
                log.warning("Telegram bot failed to start (missing TELEGRAM_BOT_TOKEN env variable). Telegram bot disabled.")
        else:
            log.info("Telegram bot disabled. Use Settings to enable (requires TELEGRAM_BOT_TOKEN env variable).")

        from supervisor.state import init as state_init, init_state, load_state, save_state
        from supervisor.state import append_jsonl, update_budget_from_usage, rotate_chat_log_if_needed
        state_init(DATA_DIR, float(settings.get("TOTAL_BUDGET", 1000.0)))
        init_state()

        # Load or create owner mapping
        owner_data_path = DATA_DIR / "state" / "owner.json"
        if owner_data_path.exists():
            with owner_data_path.open("r") as f:
                owner_data = json.load(f)
        else:
            owner_data = {"owner_chat_id": None}
            owner_data_path.parent.mkdir(parents=True, exist_ok=True)
            with owner_data_path.open("w") as f:
                json.dump(owner_data, f)

        # Worker thread: run agent loop
        def worker_target():
            try:
                from ouroboros.loop import run as run_loop
                run_loop()
            except Exception as e:
                import traceback
                _supervisor_error = f"Worker exception: {e}\n{traceback.format_exc()}"
                log.exception("Worker exception")

        worker_thread = threading.Thread(target=worker_target, daemon=True)
        worker_thread.start()

        _supervisor_ready.set()

        # Supervisor loop: process tasks, handle restart requests
        from supervisor.queue import pop_task, set_task_result, freeze_queue, unfreeze_queue
        from supervisor.events import log_event
        from supervisor.state import get_state

        while True:
            time.sleep(0.1)

            if _restart_requested.is_set():
                log.info("Restart requested by user (Settings or /restart command).")
                broadcast_ws_sync({"type": "restart", "timestamp": datetime.now(timezone.utc).isoformat()})
                time.sleep(1)  # Give time to send the message
                sys.exit(RESTART_EXIT_CODE)

            task = pop_task()
            if task:
                queue_size_before = task.get("queue_size_before", 0)
                log.info(f"Task popped: id={task['id']}, queue_size_before={queue_size_before}")

                try:
                    from supervisor.workers import run_foreground_worker
                    result_data = run_foreground_worker(task)
                    set_task_result(task["id"], result_data)

                    broadcast_ws_sync({"type": "task_complete", "task_id": task["id"], "result": result_data})
                    log.info(f"Task completed: id={task['id']}")

                    llm_total = result_data.get("llm_calls", 0)
                    llm_rub = float(result_data.get("llm_rub", 0.0))
                    stt_total = result_data.get("stt_calls", 0)
                    stt_rub = float(result_data.get("stt_rub", 0.0))
                    tot = llm_rub + stt_rub

                    if tot > 0:
                        update_budget_from_usage(dict(llm_total=llm_total, llm_rub=llm_rub,
                                                    stt_total=stt_total, stt_rub=stt_rub))

                except Exception as e:
                    import traceback
                    error_msg = f"Task execution error: {e}\n{traceback.format_exc()}"
                    log.exception("Task execution error")
                    set_task_result(task["id"], {"error": error_msg})
                    broadcast_ws_sync({"type": "task_error", "task_id": task["id"], "error": error_msg})

            # Check budget and auto-pause if needed
            try:
                state = get_state()
                current_remaining = state.get("budget", {}).get("remaining_rub", 0.0)
                total_budget = state.get("budget", {}).get("total_rub", 1000.0)
                threshold = total_budget * 0.5
                if current_remaining < threshold and current_remaining > threshold - 1.0:
                    log.warning(f"Budget warning: remaining {current_remaining:.2f} RUB out of {total_budget:.2f} RUB")
                    broadcast_ws_sync({
                        "type": "budget_warning",
                        "remaining_rub": current_remaining,
                        "total_rub": total_budget
                    })
                if current_remaining <= 0 and _supervisor_ready.is_set():
                    log.error("Budget exhausted. Pausing task processing.")
                    broadcast_ws_sync({"type": "budget_exhausted"})
                    freeze_queue()
            except Exception as e:
                log.exception("Error checking budget")
    except Exception as e:
        import traceback
        _supervisor_error = f"Supervisor exception: {e}\n{traceback.format_exc()}"
        log.exception("Supervisor exception")


# ---------------------------------------------------------------------------
# HTTP API endpoints
# ---------------------------------------------------------------------------

async def api_status(request: Request) -> JSONResponse:
    """Health check / status endpoint."""
    return JSONResponse({
        "status": "ok",
        "supervisor_ready": _supervisor_ready.is_set(),
        "supervisor_error": _supervisor_error,
        "ws_clients": len(_ws_clients),
    })


async def api_settings(request: Request) -> JSONResponse:
    """Load settings from ouroboros.config."""
    settings = load_settings()
    return JSONResponse(settings)


async def api_settings_save(request: Request) -> JSONResponse:
    """Save settings to ouroboros.config."""
    try:
        data = await request.json()
        save_settings(data)
        _apply_settings_to_env(data)
        return JSONResponse({"status": "ok", "saved": True})
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=400)


async def api_restart(request: Request) -> JSONResponse:
    """Request a restart."""
    _restart_requested.set()
    return JSONResponse({"status": "ok", "restarting": True})


async def api_static(request: Request) -> FileResponse:
    """Serve static files from REPO_DIR/static/."""
    path = request.path_params["path"] or "index.html"
    file_path = REPO_DIR / "static" / path
    if not file_path.exists():
        file_path = REPO_DIR / "static" / "index.html"
    return FileResponse(file_path)


async def ws_endpoint(websocket: WebSocket) -> None:
    """WebSocket endpoint for real-time updates."""
    await websocket.accept()
    with _ws_lock:
        _ws_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        with _ws_lock:
            try:
                _ws_clients.remove(websocket)
            except ValueError:
                pass


# ---------------------------------------------------------------------------
# Uvicorn startup/shutdown
# ---------------------------------------------------------------------------

async def startup_event():
    """Start the supervisor in a background thread."""
    global _event_loop
    _event_loop = asyncio.get_running_loop()

    settings = load_settings()

    thread = threading.Thread(target=_run_supervisor, args=(settings,), daemon=True)
    thread.start()

    log.info("Server started on port %s", PORT)
    log.info("WebSocket endpoint: ws://localhost:%s/ws", PORT)


async def shutdown_event():
    """Cleanup on shutdown."""
    log.info("Server shutting down")


# ---------------------------------------------------------------------------
# Routes and app
# ---------------------------------------------------------------------------

routes = [
    Route("/api/status", api_status, methods=["GET"]),
    Route("/api/settings", api_settings, methods=["GET"]),
    Route("/api/settings/save", api_settings_save, methods=["POST"]),
    Route("/api/restart", api_restart, methods=["POST"]),
    Route("/static/{path:path}", api_static, methods=["GET"]),
    WebSocketRoute("/ws", ws_endpoint),
]

app = Starlette(
    routes=routes,
    on_startup=[startup_event],
    on_shutdown=[shutdown_event],
    static_files={"/static": str(REPO_DIR / "static")}
)


if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="127.0.0.1",
        port=PORT,
        log_level="info",
        lifespan="on",
    )