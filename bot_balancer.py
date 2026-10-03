"""
bot_balancer.py — Cross-bot load balancing for brand-new purchases.

CONTEXT
  Every deployed bot's @username gets rotated (sometimes the bot is deleted
  and re-created entirely), but its MongoDB (config.MONGO_URI) is always
  kept — that's the one persistent thing across a rename. BOT_ID + BOT_TYPE
  (config.py) are small, stable operator-set labels that identify "this
  deployment slot" regardless of username churn.

WHAT THIS DOES
  Every running bot process periodically self-reports (bot_id, bot_type,
  current username, its own mongo_uri, current active-subscriber count)
  into the shared cross-bot registry (payments_db.bot_registry — a plain
  MongoDB collection in the SAME shared 'PaymentAPi' database already used
  for UTR dedup, but a completely separate collection; nothing here ever
  touches the dedup logic).

  When a NEW purchase is approved, the bot that took the payment asks the
  registry "which bot of MY OWN type (regular vs premium) currently has the
  fewest active subscribers?" —

    • If that's US → nothing changes, the duration is applied locally
      exactly like before (db.update_validity).

    • If it's a DIFFERENT bot → we combine whatever remaining validity this
      user already had on THIS bot with the newly purchased duration, write
      the resulting total straight into the target bot's own MongoDB
      'users' collection (reachable via its registered mongo_uri), then
      wipe the subscription locally so it's never double-counted on two
      bots at once. Any task/dyno this user had running here is stopped
      first, same as a manual /stop.

  This module never decides "should a load-balance happen" on its own
  outside of a purchase — the shift only ever happens at purchase time, per
  the requirement that a user is never bounced between bots mid-subscription.
"""

import time
import logging
import motor.motor_asyncio

import config
import database as db
import payments_db

logger = logging.getLogger(__name__)

# One AsyncIOMotorClient per distinct remote mongo_uri, cached for the life
# of this process — purchases happen often enough that reconnecting every
# single time would be wasteful and slow.
_remote_clients: dict = {}


def _get_remote_db(mongo_uri: str):
    """Return (and cache) a Motor database handle for ANOTHER bot's own,
    separate MongoDB — never this bot's own config.MONGO_URI."""
    client = _remote_clients.get(mongo_uri)
    if client is None:
        client = motor.motor_asyncio.AsyncIOMotorClient(
            mongo_uri, serverSelectionTimeoutMS=10000,
        )
        _remote_clients[mongo_uri] = client
    return client.get_default_database('telegram_bot_db')


async def _count_active_subscribers() -> int:
    try:
        users = await db.get_all_users()
        now = time.time()
        return sum(1 for u in users if u[1] and u[1] > now)
    except Exception:
        return 0


async def refresh_registry(bot_client) -> str | None:
    """
    Self-report this bot's current @username + active-subscriber count into
    the shared registry. Safe to call often (cheap upsert) — called once at
    startup, once again right after every purchase, and periodically from
    the watchdog loop so other bots never compare against badly stale data.

    Returns the current username (or None if it couldn't be fetched).
    """
    if not config.BOT_ID:
        logger.warning(
            "bot_balancer: BOT_ID not set — this bot will NOT participate "
            "in cross-bot load balancing (purchases stay local only)."
        )
        return None

    username = None
    try:
        me = await bot_client.getMe()
        if me and not isinstance(me, Exception):
            # TDLib User: primary username lives in .usernames.active_usernames
            usernames = getattr(me, "usernames", None)
            if usernames and getattr(usernames, "active_usernames", None):
                username = usernames.active_usernames[0]
            else:
                username = getattr(me, "username", None)
    except Exception as e:
        logger.warning(f"bot_balancer: get_me() failed: {e}")

    subscriber_count = await _count_active_subscribers()

    await payments_db.refresh_bot_registry(
        bot_id=config.BOT_ID,
        bot_type=config.BOT_TYPE,
        username=username,
        mongo_uri=config.MONGO_URI,
        subscriber_count=subscriber_count,
    )
    return username


def _cancel_local_sessions(user_id: int) -> None:
    """Stop any in-process transfer this user has running on THIS bot before
    their subscription moves elsewhere — mirrors handlers.cancel_existing_sessions."""
    to_delete = [
        sid for sid, data in config.active_sessions.items()
        if data.get('user_id') == user_id
    ]
    for sid in to_delete:
        task = config.active_sessions[sid].get('task_object')
        if task and not task.done():
            task.cancel()
        del config.active_sessions[sid]


async def place_new_purchase(user_id: int, duration_seconds: int, bot_client) -> dict:
    """
    Decide where a just-approved purchase should live, and apply it.

    Returns:
      {
        'moved':            bool,       # True if placed on a DIFFERENT bot
        'new_expiry':       float,      # resulting expiry timestamp
        'target_username':  str|None,   # set only when moved is True
        'target_bot_id':    str|None,
      }

    On any failure to reach the target bot's database, we never risk
    stranding a paid user's subscription — we fall back to applying it
    locally, exactly as before this feature existed.
    """
    # Refresh our own row first so this comparison uses a live count, not
    # whatever the last periodic watchdog tick happened to report.
    await refresh_registry(bot_client)

    target = await payments_db.get_least_loaded_bot(config.BOT_TYPE, config.BOT_ID)

    if not target or target.get("bot_id") == config.BOT_ID:
        new_expiry = await db.update_validity(user_id, duration_seconds)
        return {"moved": False, "new_expiry": new_expiry,
                "target_username": None, "target_bot_id": None}

    target_uri      = target.get("mongo_uri")
    target_username = target.get("username")
    target_bot_id   = target.get("bot_id")

    if not target_uri:
        logger.warning(
            f"bot_balancer: least-loaded bot {target_bot_id} has no mongo_uri "
            f"in its registry entry — keeping purchase local instead."
        )
        new_expiry = await db.update_validity(user_id, duration_seconds)
        return {"moved": False, "new_expiry": new_expiry,
                "target_username": None, "target_bot_id": None}

    now = time.time()
    local_doc = await db.get_user_doc(user_id)
    local_expiry = local_doc.get('validity_expiry', 0) if local_doc else 0
    remaining = max(local_expiry - now, 0) if local_expiry else 0
    total_duration = remaining + duration_seconds

    try:
        remote_db  = _get_remote_db(target_uri)
        remote_doc = await remote_db.users.find_one({'user_id': user_id})
        remote_expiry = remote_doc.get('validity_expiry', 0) if remote_doc else 0
        base = remote_expiry if remote_expiry > now else now
        new_expiry = base + total_duration

        await remote_db.users.update_one(
            {'user_id': user_id},
            {'$set': {'validity_expiry': new_expiry, 'is_admin': 0},
             '$setOnInsert': {
                 'joined_date':     now,
                 'first_name':      (local_doc or {}).get('first_name', 'User'),
                 'session_string':  None,
                 'phone':           None,
             }},
            upsert=True,
        )
    except Exception as e:
        logger.error(
            f"bot_balancer: remote write to {target_bot_id} failed ({e}) — "
            f"keeping purchase local so nothing is lost."
        )
        new_expiry = await db.update_validity(user_id, duration_seconds)
        return {"moved": False, "new_expiry": new_expiry,
                "target_username": None, "target_bot_id": None}

    # Now that the subscription safely lives on the target bot, stop
    # anything running here and wipe it locally so it's never double-counted.
    _cancel_local_sessions(user_id)
    try:
        dyno_rec = await db.get_user_dyno(user_id)
        if dyno_rec and dyno_rec.get('dyno_name'):
            from heroku_manager import heroku_manager
            await heroku_manager.kill_dyno(dyno_rec['dyno_name'])
    except Exception:
        pass
    await db.revoke_user(user_id)

    # Best-effort — reflect the count change immediately rather than waiting
    # for the next periodic watchdog tick.
    await refresh_registry(bot_client)

    return {
        "moved": True,
        "new_expiry": new_expiry,
        "target_username": target_username,
        "target_bot_id": target_bot_id,
    }
