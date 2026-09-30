#!/usr/bin/env python3
"""
main.py  –  Bot entry point  (v7.1 — TDLib / pytdbot)

Changes from v6.x (Pyrofork):
  • pyrogram.Client + idle() → pytdbot.Client(token=…) + client.idle()
  • TDLib delivers updates per-connection — worker dynos no longer compete
    with this process for an update stream (the old no_updates bug class is
    gone by design).
  • updateFile / updateMessageSendSucceeded events are bridged into
    progress.py so in-process fallback transfers get progress bars and
    reliable upload completion too.

v7.1 hotfix (login crash):
  • TDLib allows exactly ONE td_receive() thread per process. v7.0 let every
    Client spawn its own ClientManager/receiver → starting a login client
    next to the bot client aborted the whole process (SIGABRT / status 134)
    before the OTP could be sent. All clients now share ONE ClientManager.
"""

import asyncio

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from aiohttp import web

import config
from handlers import register_handlers
import database as db
import bot_balancer
import progress as progress_mod
from watchdog import watchdog_loop, cleanup_expired_subscriptions

# Strong references for background tasks (see v6.0 note — GC would otherwise
# silently kill the watchdog).
_background_tasks = set()


def _spawn_background(factory, name: str):
    task = asyncio.create_task(factory(), name=name)
    _background_tasks.add(task)

    def _on_done(t: asyncio.Task):
        _background_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc:
            config.logger.error(f"💥 Background task '{name}' died: {exc}", exc_info=exc)
            config.logger.warning(f"🔁 Restarting background task '{name}'…")
            _spawn_background(factory, name)

    task.add_done_callback(_on_done)
    return task


# ── WEB SERVER ────────────────────────────────────────────────────────────────

async def handle(request):
    return web.Response(text="🔥 Content Saver Bot v7.0 — TDLib (pytdbot) Edition")


async def start_web_server():
    app    = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', config.PORT)
    await site.start()
    config.logger.info(f"⚡ Web Server — Port {config.PORT}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

async def main():
    config.logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    config.logger.info("🚀 Content Saver Bot v7.0 (TDLib / pytdbot) Starting…")
    config.logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    if not config.API_ID or not config.API_HASH:
        config.logger.error("❌ MISSING CONFIGURATION — API_ID / API_HASH not set. Exiting.")
        return

    # ── Database ──────────────────────────────────────────────────────────────
    await db.init_db()
    config.logger.info("💾 Database Initialised")

    # ── TDLib bot client ──────────────────────────────────────────────────────
    #
    # ⚠️ TDLib RULE: exactly ONE td_receive() thread per process.
    # Every pytdbot Client that calls .start() alone spawns its own
    # ClientManager → its own receiver thread → a second client (login,
    # transfer, worker bot) crashes the whole process with SIGABRT
    # ("Receive must not be called simultaneously from two different
    # threads"). So ALL clients in this process (bot + temp login clients +
    # in-process transfer user clients) live on ONE shared ClientManager.
    from pytdbot import Client, ClientManager
    import session_manager
    from session_manager import wait_until_ready

    bot_client = Client(
        token=config.BOT_TOKEN,
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        files_directory="/tmp/td_bot_main",
        database_encryption_key=config.TD_ENCRYPTION_KEY,
        use_file_database=False,
        use_chat_info_database=True,
        use_message_database=False,
        workers=8,               # same concurrency as the Pyrogram workers=8
        td_verbosity=1,
        default_parse_mode="markdown",
    )

    # Shared manager: created with a LIST so start_clients_on_add=True —
    # without that, stopping any managed client would close the whole manager
    # and kill the bot's update stream.
    client_manager = ClientManager(
        [bot_client], verbosity=1, loop=asyncio.get_running_loop()
    )
    session_manager.set_client_manager(client_manager)

    # ── Wire TDLib file/send events into the progress trackers ────────────────

    @bot_client.on_updateFile()
    async def _on_file(c, update):
        try:
            await progress_mod.dispatch_update_file(update.file)
        except Exception:
            pass

    @bot_client.on_updateMessageSendSucceeded()
    async def _on_send_ok(c, update):
        await progress_mod.dispatch_send_succeeded(update)

    @bot_client.on_updateMessageSendFailed()
    async def _on_send_fail(c, update):
        await progress_mod.dispatch_send_failed(update)

    # ── Register all command/callback handlers BEFORE start ───────────────────
    register_handlers(bot_client)

    await client_manager.start()          # starts receiver thread + bot client
    await wait_until_ready(bot_client)    # shared-manager start() doesn't block
    config.logger.info("✅ Bot client started (TDLib)")

    # ── Web server ────────────────────────────────────────────────────────────
    _spawn_background(start_web_server, "web_server")

    # ── Bot registry (cross-bot load balancing) ───────────────────────────────
    if config.BOT_ID:
        username = await bot_balancer.refresh_registry(bot_client)
        config.logger.info(
            f"📡 Bot registry: id={config.BOT_ID} type={config.BOT_TYPE} "
            f"username=@{username or '?'}"
        )
    else:
        config.logger.warning(
            "⚠️ BOT_ID not set — cross-bot purchase load balancing is disabled "
            "for this bot (set BOT_ID in Heroku Config Vars to enable it)."
        )

    # ── Reliability watchdog ──────────────────────────────────────────────────
    await cleanup_expired_subscriptions()
    _spawn_background(lambda: watchdog_loop(bot_client), "watchdog_loop")
    config.logger.info("🐕 Watchdog task launched (strong ref held — won't be GC'd)")

    config.logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    config.logger.info("✅ System Online!")
    config.logger.info(f"👑 Admin ID: {config.ADMIN_ID}")
    config.logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # ── Run until Ctrl-C ──────────────────────────────────────────────────────
    await bot_client.idle()

    # Graceful shutdown: stop the bot client, then close the shared manager —
    # its receiver thread is non-daemon and blocked in td_receive(), so
    # skipping this hangs the interpreter at exit (stuck dyno until TTL).
    try:
        await bot_client.stop()
    except Exception:
        pass
    try:
        await client_manager.close()
    except Exception:
        pass


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        config.logger.info("Bot stopped.")
