#!/usr/bin/env python3
"""
worker.py  –  Per-user transfer worker  (v7.0 — TDLib / pytdbot)

Runs inside a dedicated Heroku one-off dyno.
Usage (spawned by heroku_manager.py):
    python3 worker.py --user-id=12345 --task-id=abc-def-ghi

v7.0 changes (Pyrofork → TDLib):
  • pyrogram.Client → pytdbot.Client (tdjson).
  • Bot client: Client(token=BOT_TOKEN, …) — TDLib bot auth.
  • User client: the archived TDLib session blob from Mongo is restored to a
    files_directory and opened with user_bot=True.
  • Session validity check: get_me() → getMe() after authorizationStateReady.
  • Update-conflict bug is GONE BY DESIGN: TDLib delivers updates per
    connection, so this worker's bot client can listen to updateFile /
    updateMessageSendSucceeded events without touching main.py's stream.
  • on_updateFile / on_updateMessageSendSucceeded are wired into progress.py
    so progress bars + upload completion work inside the worker.
"""

import asyncio
import argparse
import gc
import logging
import os
import sys
import time

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

import psutil

import config
import database as db
from transfer import transfer_process, _raise_if_error
from session_manager import session_manager, SessionExpiredError
import progress as progress_mod

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [WORKER] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ── DYNO RAM LIMIT MAP ────────────────────────────────────────────────────────

DYNO_RAM_LIMITS = {
    "free":            512  * 1024 * 1024,
    "eco":             512  * 1024 * 1024,
    "basic":           512  * 1024 * 1024,
    "standard-1x":     512  * 1024 * 1024,
    "standard-2x":    1024  * 1024 * 1024,
    "performance-m":  2560  * 1024 * 1024,
    "performance-l": 14336  * 1024 * 1024,
}

def get_dyno_ram_limit() -> int:
    size = os.environ.get("WORKER_DYNO_SIZE", "standard-1x").lower().strip()
    return DYNO_RAM_LIMITS.get(size, 512 * 1024 * 1024)


# ── TDLIB CLIENT FACTORY ──────────────────────────────────────────────────────

def _register_bridges(client):
    """Wire updateFile / send-result events into progress.py trackers."""

    @client.on_updateFile()
    async def _on_file(c, update):
        try:
            await progress_mod.dispatch_update_file(update.file)
        except Exception:
            pass

    @client.on_updateMessageSendSucceeded()
    async def _on_send_ok(c, update):
        await progress_mod.dispatch_send_succeeded(update)

    @client.on_updateMessageSendFailed()
    async def _on_send_fail(c, update):
        await progress_mod.dispatch_send_failed(update)


def make_bot_client(name: str = "bot"):
    from pytdbot import Client
    client = Client(
        token=config.BOT_TOKEN,
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        files_directory=f"/tmp/td_{name}_{os.getpid()}",
        database_encryption_key=config.TD_ENCRYPTION_KEY,
        use_file_database=False,
        use_chat_info_database=True,
        use_message_database=False,
        workers=4,
        td_verbosity=1,
        default_parse_mode="markdown",
    )
    _register_bridges(client)
    return client


# ── MOCK EVENT ────────────────────────────────────────────────────────────────

class MockEvent:
    """
    Thin adapter so transfer_process can call event.respond() without a real
    pytdbot Message object available.
    """
    def __init__(self, bot_client, chat_id: int):
        self.chat_id   = chat_id
        self._bot      = bot_client
        self.sender_id = None

    async def respond(self, text: str, reply_markup=None):
        try:
            res = await self._bot.sendTextMessage(
                self.chat_id, text, reply_markup=reply_markup
            )
            if config.is_error(res):
                logger.warning(f"MockEvent.respond TDLib error: {res.message}")
                return None
            return res
        except Exception as e:
            logger.warning(f"MockEvent.respond failed: {e}")
            return None


# ── RAM REPORTER ──────────────────────────────────────────────────────────────

async def ram_reporter(user_id: int, task_id: str, stop_event: asyncio.Event):
    process   = psutil.Process(os.getpid())
    interval  = 10
    ram_total = get_dyno_ram_limit()
    dyno_size = os.environ.get("WORKER_DYNO_SIZE", "standard-1x")
    total_mb  = ram_total / (1024 * 1024)
    logger.info(f"💾 Dyno RAM limit: {total_mb:.0f} MB ({dyno_size})")

    while not stop_event.is_set():
        try:
            mem_info = process.memory_info()
            ram_used = min(mem_info.rss, ram_total)
            used_mb  = ram_used / (1024 * 1024)
            pct      = ram_used / ram_total * 100 if ram_total else 0
            label    = f"{used_mb:.0f}MB / {total_mb:.0f}MB ({pct:.1f}%)"

            await db.update_dyno_ram(user_id, ram_used, ram_total, label)

            if await db.is_cleanup_requested(user_id):
                logger.info(f"🧹 RAM cleanup triggered for user {user_id}")
                before = process.memory_info().rss
                gc.collect()
                after  = process.memory_info().rss
                freed  = (before - after) / (1024 * 1024)
                logger.info(f"✅ gc.collect freed ~{freed:.1f} MB")
                await db.clear_cleanup_flag(user_id)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"ram_reporter error: {e}")

        try:
            await asyncio.wait_for(
                asyncio.shield(stop_event.wait()),
                timeout=interval
            )
        except asyncio.TimeoutError:
            pass


# ── PREFLIGHT PEER RESOLUTION ─────────────────────────────────────────────────

async def worker_preflight(bot_client, dest_id, chat_id: int) -> bool:
    """
    Resolve the destination chat via TDLib BEFORE any file is sent.
    Returns True if OK to proceed, False if the bot lacks permission (abort).
    """
    logger.info(f"🔍 Worker preflight: resolving dest peer {dest_id}…")
    try:
        chat = await bot_client.getChat(chat_id=dest_id)
        if config.is_error(chat):
            retry = config.get_retry_after(chat)
            if retry:
                logger.warning(f"Preflight FloodWait {retry}s — waiting…")
                await asyncio.sleep(retry + 2)
                return True
            msg = (chat.message or "").upper()
            if 'CHAT_NOT_FOUND' in msg or 'NOT_A_MEMBER' in msg or 'ACCESS' in msg:
                raise PermissionError(chat.message)
            raise Exception(chat.message)
        logger.info(f"✅ Worker preflight OK: {getattr(chat, 'title', '?')!r} ({dest_id})")
        return True

    except PermissionError:
        logger.error(f"❌ Dest {dest_id} is private/inaccessible to the bot")
        try:
            await bot_client.sendTextMessage(
                chat_id,
                "❌ **Destination channel is private or bot is not a member.**\n\n"
                "Make sure the bot is added as **Full Admin** in the channel."
            )
        except Exception:
            pass
        return False

    except Exception as e:
        logger.warning(f"Worker preflight warning (non-fatal): {type(e).__name__}: {e}")
        return True


async def worker_preflight_source(user_client, source_id, chat_id: int, bot_client) -> bool:
    """
    Resolve the SOURCE chat on the user client before any getMessages() call.
    Returns True if OK, False if the source is genuinely inaccessible.
    """
    logger.info(f"🔍 Worker preflight: resolving source peer {source_id}…")
    try:
        if isinstance(source_id, str) and not source_id.lstrip('-').isdigit():
            chat = await user_client.searchPublicChat(username=source_id.lstrip('@'))
        else:
            chat = await user_client.getChat(chat_id=int(source_id))
        if config.is_error(chat):
            retry = config.get_retry_after(chat)
            if retry:
                logger.warning(f"Source preflight FloodWait {retry}s — waiting…")
                await asyncio.sleep(retry + 2)
                return True
            raise Exception(chat.message)
        logger.info(f"✅ Source preflight OK: {getattr(chat, 'title', chat.id)!r}")
        return True

    except Exception as e:
        logger.error(f"❌ Cannot resolve source chat {source_id}: {e}")
        try:
            await bot_client.sendTextMessage(
                chat_id,
                "❌ **Could not access the source channel/group.**\n\n"
                "Make sure your logged-in account is a member of the source "
                "chat, then run `/clone` again."
            )
        except Exception:
            pass
        return False


# ── MAIN ──────────────────────────────────────────────────────────────────────

async def main(user_id: int, task_id: str):
    logger.info(f"━━ Worker starting: user={user_id} task={task_id} ━━")

    await db.init_db()

    # ── Load task ──────────────────────────────────────────────────────────
    task = await db.get_transfer_task(task_id)
    if not task:
        logger.error(f"Task {task_id} not found in DB — exiting.")
        return

    task_data     = task['data']
    chat_id       = task_data['chat_id']
    dest_id       = task_data['dest_id']
    dest_topic_id = task_data.get('dest_topic_id')

    # ── Load user session ──────────────────────────────────────────────────
    is_valid, session_blob, phone = await db.check_user(user_id)
    if not session_blob:
        logger.error(f"No session for user {user_id} — exiting.")
        await db.update_task_status(task_id, 'failed')
        return

    users      = await db.get_all_users()
    first_name = next((u[3] for u in users if u[0] == user_id), "User")

    dyno_name = os.environ.get("DYNO", "run.unknown")
    await db.register_user_dyno(user_id, dyno_name, task_id, first_name)
    logger.info(f"📌 Registered as dyno: {dyno_name}")

    stop_event    = asyncio.Event()
    reporter_task = asyncio.create_task(ram_reporter(user_id, task_id, stop_event))

    # ── Connect bot_client (TDLib bot) ──────────────────────────────────────
    # Same TDLib rule as main.py: ONE shared ClientManager per process —
    # bot + user clients both attach to it (a second receiver thread would
    # SIGABRT the whole dyno). Manager created with a LIST so stopping a
    # managed client never closes the manager itself.
    from pytdbot import ClientManager
    import session_manager as session_manager_mod
    from session_manager import wait_until_ready

    client_manager = ClientManager([], verbosity=1, loop=asyncio.get_running_loop())
    session_manager_mod.set_client_manager(client_manager)
    await client_manager.start()

    bot_client = make_bot_client("worker_bot")
    await client_manager.add_client(bot_client, start_client=True)
    await wait_until_ready(bot_client, timeout=90)
    logger.info("✅ Bot client started (TDLib)")

    async def _shutdown_clients():
        stop_event.set()
        reporter_task.cancel()
        try: await session_manager.stop_user_session(user_client)
        except Exception: pass
        try: await bot_client.stop()
        except Exception: pass
        try: await client_manager.close()
        except Exception: pass

    async def _abort(msg: str):
        try:
            await bot_client.sendTextMessage(chat_id, msg)
        except Exception:
            pass
        await db.update_task_status(task_id, 'failed')
        await db.clear_user_dyno(user_id)
        await _shutdown_clients()

    # ── Connect user_client (restored TDLib session) ───────────────────────
    user_client = None
    try:
        user_client = await session_manager.start_user_session(session_blob, user_id)
        me = await user_client.getMe()
        if config.is_error(me) or not me:
            raise SessionExpiredError("getMe() failed — session invalid.")
        logger.info(f"✅ User client authorised: {me.first_name} ({me.id})")

    except SessionExpiredError:
        logger.error("User session invalid/expired — exiting.")
        await _abort(
            "❌ **Your Telegram session has expired.**\n\n"
            "Please use `/login` to reconnect your account and run `/clone` again."
        )
        return

    except Exception as e:
        msg_up = str(e).upper()
        if 'USER_DEACTIVATED' in msg_up or 'BANNED' in msg_up:
            logger.error("User account deactivated/banned.")
            await _abort("❌ Your Telegram account has been deactivated or banned.")
            return
        logger.error(f"User client start failed: {e}")
        await _abort(
            f"❌ Could not start transfer session.\n`{e}`\n\nTry `/login` again."
        )
        return

    # ── Initialise PTB Bot singleton ───────────────────────────────────────
    try:
        await config.get_ptb_bot()
        logger.info("✅ PTB Bot singleton ready")
    except Exception as e:
        logger.warning(f"PTB init failed (small files will use bot_client): {e}")

    # ── Worker preflight: resolve dest peer ───────────────────────────────
    preflight_ok = await worker_preflight(bot_client, dest_id, chat_id)
    if not preflight_ok:
        await db.update_task_status(task_id, 'failed')
        await db.clear_user_dyno(user_id)
        await _shutdown_clients()
        return

    # ── Worker preflight: resolve SOURCE peer ──────────────────────────────
    source_preflight_ok = await worker_preflight_source(
        user_client, task_data['source_id'], chat_id, bot_client
    )
    if not source_preflight_ok:
        await db.update_task_status(task_id, 'failed')
        await db.clear_user_dyno(user_id)
        await _shutdown_clients()
        return

    # ── Build MockEvent and session stub ───────────────────────────────────
    mock_event = MockEvent(bot_client, chat_id)
    session_id = task_data.get('session_id', task_id)
    config.active_sessions[session_id] = {
        'settings':      task_data.get('settings', {}),
        'user_id':       user_id,
        'step':          'running',
        'stop_flag':     False,
        'task_id':       task_id,
        'dest_topic_id': dest_topic_id,
        'chat_id':       chat_id,
    }

    await db.update_task_status(task_id, 'running')

    # ── Run transfer ───────────────────────────────────────────────────────
    try:
        result, retry_delay = await transfer_process(
            event         = mock_event,
            user_client   = user_client,
            bot_client    = bot_client,
            source_id     = task_data['source_id'],
            dest_id       = dest_id,
            start_msg     = task_data['start_msg'],
            end_msg       = task_data['end_msg'],
            session_id    = session_id,
            log_channel   = task_data.get('log_channel'),
            topic_id      = task_data.get('topic_id'),
            dest_topic_id = dest_topic_id,
        )

        if result == 'completed':
            await db.update_task_status(task_id, 'done')
            logger.info(f"✅ Transfer completed for user {user_id}")
        elif result == 'stopped_by_user':
            await db.update_task_status(task_id, 'stop_requested')
            logger.info(f"🚫 Transfer stopped by user/admin for user {user_id}")
        else:
            # Retryable — the watchdog auto-spawns a fresh dyno from the
            # exact last checkpoint after the computed backoff.
            delay = retry_delay or config.RETRY_AFTER_FAILURE_SECONDS
            logger.warning(f"⚠️ Transfer ended retryable — scheduling retry in {delay}s")
            await db.schedule_task_retry(task_id, delay)
    except Exception as e:
        logger.error(f"Worker transfer crashed: {e}", exc_info=True)
        await db.schedule_task_retry(task_id, config.RETRY_AFTER_FAILURE_SECONDS)

    # ── Cleanup ────────────────────────────────────────────────────────────
    await db.clear_user_dyno(user_id)
    await _shutdown_clients()
    logger.info("━━ Worker exiting ━━")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Per-user Telegram transfer worker")
    parser.add_argument('--user-id', type=int, required=True)
    parser.add_argument('--task-id', type=str, required=True)
    args = parser.parse_args()

    try:
        asyncio.run(main(args.user_id, args.task_id))
    except (KeyboardInterrupt, SystemExit):
        logger.info("Worker interrupted, exiting cleanly.")

    sys.exit(0)
