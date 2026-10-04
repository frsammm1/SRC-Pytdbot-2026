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
     Once a user's subscription is no longer valid, their checkpoint, task,
     dyno record, TDLib session archive, phone and password are wiped so
     Mongo doesn't keep growing. Active subscribers are never touched.
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
    #  - 'running'/'pending' but silent → dyno crashed/OOM-killed/never booted
    #  - 'retry_pending', due  → worker exited cleanly but gave up early
    #    (5 consecutive failures, preflight issue, internal crash)
    tasks = await db.get_stale_running_tasks(config.DYNO_STALE_THRESHOLD)
    tasks += await db.get_due_retry_tasks()

    resumed_users = set()   # ONE resume per user per sweep — never two dynos

    for task in tasks:
        user_id     = task.get('user_id')
        old_task_id = task.get('task_id')
        try:
            # Same user queued twice (e.g. two failed tasks both due)? Keep
            # the FIRST (oldest → most-behind) and supersede the rest —
            # spawning both would open the same TDLib session twice
            # (AUTH_KEY_DUPLICATED) and kill both dynos.
            if user_id in resumed_users:
                await db.update_task_status(old_task_id, 'superseded')
                continue

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

            # Atomic claim: if the status changed underneath us (user /stop,
            # another sweep, a manual restart), back off immediately.
            if not await db.claim_stale_task_for_resume(old_task_id):
                continue
            resumed_users.add(user_id)

            is_valid, _, _ = await db.check_user(user_id)
            if not is_valid:
                # Subscription lapsed mid-transfer — leave it for the expiry
                # cleanup sweep rather than resuming a job that's no longer paid for.
                await db.update_task_status(old_task_id, 'expired')
                continue

            checkpoint = await db.get_transfer_checkpoint(user_id)
            task_data  = task.get('data') or {}

            # A checkpoint only helps if it describes THIS transfer (same
            # source/dest/end). A dyno that died before its first checkpoint
            # (or with a stale checkpoint from an older run) resumes from the
            # task's own range instead of being marked failed and leaving the
            # user stuck on "Transfer Started!" forever.
            def _ckpt_matches(ckpt) -> bool:
                try:
                    return (
                        ckpt
                        and str(ckpt.get('source_id')) == str(task_data.get('source_id'))
                        and str(ckpt.get('dest_id'))   == str(task_data.get('dest_id'))
                        and int(ckpt.get('end_msg', -1)) == int(task_data.get('end_msg', -2))
                    )
                except Exception:
                    return False

            ckpt      = checkpoint if _ckpt_matches(checkpoint) else None
            base      = ckpt if ckpt else task_data
            start_msg = (int(ckpt.get('current_msg', 0)) + 1) if ckpt \
                        else int(task_data.get('start_msg', 0))
            end_msg   = int(base.get('end_msg', 0))

            if not end_msg:
                await db.update_task_status(old_task_id, 'failed')
                continue
            if start_msg > end_msg:
                await db.update_task_status(old_task_id, 'done')
                if ckpt:
                    await db.clear_transfer_checkpoint(user_id)
                continue

            chat_id = base.get('chat_id') or task_data.get('chat_id')
            if not chat_id:
                config.logger.warning(
                    f"Watchdog: task {old_task_id} for user {user_id} has no chat_id — "
                    f"can't auto-resume safely, skipping."
                )
                await db.update_task_status(old_task_id, 'failed')
                continue

            # Checkpoints store source_id/dest_id as strings (safe for Mongo).
            # TDLib/Pyrogram treat a string chat-id as a USERNAME to resolve,
            # not a numeric peer ID. Numeric IDs MUST be ints.
            raw_source_id = base.get('source_id')
            raw_dest_id   = base.get('dest_id')
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

            # The stale dyno may be HUNG, not dead — still holding the user's
            # TDLib session. Kill it by name first and give Telegram a moment
            # to drop the old connection, otherwise the fresh dyno opens the
            # same auth key and both die with AUTH_KEY_DUPLICATED.
            old_dyno_name = (current_dyno or {}).get('dyno_name')
            if old_dyno_name:
                try:
                    await heroku_manager.kill_dyno(old_dyno_name)
                    await asyncio.sleep(8)
                except Exception:
                    pass

            new_task_id = str(uuid.uuid4())
            new_task_data = {
                'chat_id':       chat_id,
                'source_id':     source_id,
                'dest_id':       dest_id,
                'start_msg':     start_msg,
                'end_msg':       end_msg,
                'session_id':    str(uuid.uuid4()),
                'log_channel':   base.get('log_channel') or task_data.get('log_channel'),
                'topic_id':      base.get('topic_id'),
                'dest_topic_id': base.get('dest_topic_id'),
                'settings':      base.get('settings', {}),
                'start_link':    base.get('source_link') or task_data.get('start_link'),
            }
            await db.create_transfer_task(new_task_id, user_id, new_task_data)
            dyno_data = await heroku_manager.spawn_user_dyno(user_id, new_task_id)

            if dyno_data and dyno_data.get('name'):
                await db.update_task_status(old_task_id, 'superseded')
                config.logger.info(
                    f"🔄 Watchdog auto-resumed user {user_id}: "
                    f"msg {start_msg}→{end_msg} on dyno {dyno_data['name']}"
                )
            else:
                # Spawn failed — don't strand the task: put it back on the
                # retry queue so the next sweep tries again.
                await db.delete_task(new_task_id)
                await db.schedule_task_retry(old_task_id, 60)
                config.logger.error(
                    f"Watchdog: failed to spawn resume dyno for user {user_id} "
                    f"— retry scheduled in 60s"
                )

        except Exception as e:
            config.logger.error(
                f"Watchdog auto-resume error for user {user_id}: {e}", exc_info=True
            )


async def cleanup_expired_subscriptions() -> None:
    try:
        # Kill leftover dynos BEFORE wiping their Mongo records, otherwise
        # purge deletes the dyno_name and we can never stop the one-off.
        if HEROKU_MODE:
            for rec in await db.get_all_dynos():
                uid = rec.get('user_id')
                if not uid:
                    continue
                is_valid, _, _ = await db.check_user(uid)
                if is_valid:
                    continue
                name = rec.get('dyno_name')
                if name:
                    try:
                        await heroku_manager.kill_dyno(name)
                    except Exception:
                        pass
        await db.cleanup_expired_subscription_data()
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
