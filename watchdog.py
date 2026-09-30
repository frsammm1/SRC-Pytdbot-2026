"""
watchdog.py — background reliability loop for the always-on bot dyno.

Two jobs, run on a timer from main.py:

  1. AUTO-RESUME
     Detect one-off worker dynos that died mid-transfer (Heroku restart,
     OOM-kill, crash — anything that never reached the worker's own cleanup
     code) and silently spawn a fresh dyno that continues the exact same job
     from the last real-time checkpoint. No button, no user action needed.
     Only ever acts for a user whose subscription is currently active.

     This does NOT fire for a transfer the user or admin deliberately
     stopped — /stop and /revoke mark the task 'stop_requested', so the
     watchdog's `status == 'running'` filter skips it entirely.

  2. EXPIRY CLEANUP
     Once a user's subscription is no longer valid, their checkpoint/task
     records are wiped so nothing stale lingers in Mongo. Active subscribers
     are never touched — their progress record stays exact and durable for
     as long as their subscription lasts.
"""

import uuid
import asyncio

import config
import database as db
import bot_balancer
from heroku_manager import heroku_manager

HEROKU_MODE = bool(config.HEROKU_API_TOKEN and config.HEROKU_APP_NAME)


async def auto_resume_stale_tasks() -> None:
    if not HEROKU_MODE:
        return  # in-process fallback mode has no separate dynos to watch

    # Two failure shapes, one recovery path:
    #  - 'running' but silent  → dyno crashed/OOM-killed without cleanup
    #  - 'retry_pending', due  → worker exited cleanly but gave up early
    #    (5 consecutive failures, preflight issue, internal crash)
    tasks = await db.get_stale_running_tasks(config.DYNO_STALE_THRESHOLD)
    tasks += await db.get_due_retry_tasks()

    for task in tasks:
        user_id     = task.get('user_id')
        old_task_id = task.get('task_id')
        try:
            # Defense in depth: if this user's CURRENT dyno record points at
            # a DIFFERENT, newer task_id, this task has already been
            # superseded (a manual /clone, resume, or /stop→restart started
            # after this one was scheduled). Resuming it here would spawn a
            # second dyno racing the one the user is already looking at —
            # exactly the "same message downloads twice" bug. Skip it.
            current_dyno = await db.get_user_dyno(user_id)
            if (current_dyno and current_dyno.get('task_id')
                    and current_dyno.get('task_id') != old_task_id
                    and current_dyno.get('status') == 'running'):
                await db.update_task_status(old_task_id, 'superseded')
                continue

            is_valid, _, _ = await db.check_user(user_id)
            if not is_valid:
                # Subscription lapsed mid-transfer — leave it for the expiry
                # cleanup sweep rather than resuming a job that's no longer paid for.
                await db.update_task_status(old_task_id, 'expired')
                continue

            checkpoint = await db.get_transfer_checkpoint(user_id)
            if not checkpoint:
                await db.update_task_status(old_task_id, 'failed')
                continue

            start_msg = int(checkpoint.get('current_msg', 0)) + 1
            end_msg   = int(checkpoint.get('end_msg', 0))
            if start_msg > end_msg:
                await db.update_task_status(old_task_id, 'done')
                await db.clear_transfer_checkpoint(user_id)
                continue

            chat_id = checkpoint.get('chat_id')
            if not chat_id:
                config.logger.warning(
                    f"Watchdog: checkpoint for user {user_id} has no chat_id — "
                    f"can't auto-resume safely, skipping."
                )
                continue

            # Checkpoints store source_id/dest_id as strings (safe for Mongo).
            # Pyrogram treats a string chat-id argument as a USERNAME to
            # resolve, not a numeric peer ID — so passing the raw string
            # straight into a fresh dyno's get_chat()/get_messages() fails
            # with PeerIdInvalid/"could not access channel" even for an
            # account that's genuinely a member. Numeric IDs MUST be ints.
            raw_source_id = checkpoint.get('source_id')
            raw_dest_id   = checkpoint.get('dest_id')
            source_id = raw_source_id
            try:
                if str(raw_source_id).lstrip('-').isdigit():
                    source_id = int(raw_source_id)
            except Exception:
                pass
            try:
                dest_id = int(raw_dest_id)
            except Exception:
                dest_id = raw_dest_id

            new_task_id = str(uuid.uuid4())
            task_data = {
                'chat_id':       chat_id,
                'source_id':     source_id,
                'dest_id':       dest_id,
                'start_msg':     start_msg,
                'end_msg':       end_msg,
                'session_id':    str(uuid.uuid4()),
                'log_channel':   checkpoint.get('log_channel'),
                'topic_id':      checkpoint.get('topic_id'),
                'dest_topic_id': checkpoint.get('dest_topic_id'),
                'settings':      checkpoint.get('settings', {}),
            }
            await db.create_transfer_task(new_task_id, user_id, task_data)
            dyno_data = await heroku_manager.spawn_user_dyno(user_id, new_task_id)
            await db.update_task_status(old_task_id, 'superseded')

            if dyno_data and dyno_data.get('name'):
                config.logger.info(
                    f"🔄 Watchdog auto-resumed user {user_id}: "
                    f"msg {start_msg}→{end_msg} on dyno {dyno_data['name']}"
                )
            else:
                config.logger.error(
                    f"Watchdog: failed to spawn resume dyno for user {user_id}"
                )

        except Exception as e:
            config.logger.error(
                f"Watchdog auto-resume error for user {user_id}: {e}", exc_info=True
            )


async def cleanup_expired_subscriptions() -> None:
    try:
        affected = await db.cleanup_expired_subscription_data()
        if not affected:
            return
        if HEROKU_MODE:
            for uid in affected:
                dyno_rec = await db.get_user_dyno(uid)
                if dyno_rec and dyno_rec.get('dyno_name'):
                    try:
                        await heroku_manager.kill_dyno(dyno_rec['dyno_name'])
                    except Exception:
                        pass
                await db.clear_user_dyno(uid)
    except Exception as e:
        config.logger.error(f"Watchdog cleanup error: {e}", exc_info=True)


async def watchdog_loop(bot_client=None) -> None:
    """Runs forever inside the always-on bot dyno (main.py).

    `bot_client` is optional only for backwards compatibility with any
    external caller that still invokes this with no arguments — when it's
    None the periodic bot-registry refresh is simply skipped for that tick
    (auto-resume and expiry cleanup are unaffected either way).
    """
    config.logger.info(
        f"🐕 Watchdog online — auto-resume every {config.WATCHDOG_INTERVAL}s, "
        f"expiry cleanup every {config.CLEANUP_INTERVAL}s, "
        f"registry refresh every {config.REGISTRY_REFRESH_INTERVAL}s"
    )
    elapsed_since_cleanup = 0
    elapsed_since_registry = 0
    while True:
        try:
            await auto_resume_stale_tasks()

            elapsed_since_cleanup += config.WATCHDOG_INTERVAL
            if elapsed_since_cleanup >= config.CLEANUP_INTERVAL:
                await cleanup_expired_subscriptions()
                elapsed_since_cleanup = 0

            elapsed_since_registry += config.WATCHDOG_INTERVAL
            if bot_client is not None and elapsed_since_registry >= config.REGISTRY_REFRESH_INTERVAL:
                await bot_balancer.refresh_registry(bot_client)
                elapsed_since_registry = 0
        except Exception as e:
            config.logger.error(f"Watchdog loop error: {e}", exc_info=True)
        await asyncio.sleep(config.WATCHDOG_INTERVAL)
