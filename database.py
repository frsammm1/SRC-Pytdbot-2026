"""
database.py  –  MongoDB helpers
Includes original user/config/checkpoint functions PLUS
new dyno-tracking, task-queue, and subscription plan functions.
"""

import motor.motor_asyncio
import time
import os
import logging
import config

logger = logging.getLogger(__name__)

mongo_client = None
db = None


# ── INIT ─────────────────────────────────────────────────────────────────────

async def init_db():
    global mongo_client, db
    uri = config.MONGO_URI
    if not uri:
        logger.error("MONGO_URI not found! Database will not work properly.")
        return

    try:
        mongo_client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        db = mongo_client.get_default_database('telegram_bot_db')
        logger.info(f"💾 Connected to MongoDB: {db.name}")

        # Core indexes
        await db.users.create_index("user_id", unique=True)
        await db.config.create_index("key", unique=True)
        await db.checkpoints.create_index("user_id", unique=True)

        # Dyno + Task indexes
        await db.dynos.create_index("user_id", unique=True)
        await db.tasks.create_index("task_id", unique=True)
        await db.tasks.create_index("user_id")
        await db.tasks.create_index("status")   # watchdog scans by status every 30s

        # Plan indexes
        await db.plans.create_index("id", unique=True)

        # Local payment claims index (backup for single-bot mode)
        await db.payment_claims.create_index("utr", unique=True)
        await db.payment_claims.create_index("msg_id", unique=True)

        logger.info("✅ MongoDB indexes created")
    except Exception as e:
        logger.error(f"MongoDB Connection Failed: {e}", exc_info=True)
        db = None

    # Also init shared payments DB
    try:
        from payments_db import init_shared_payments_db
        await init_shared_payments_db()
    except Exception as e:
        logger.warning(f"Shared payments DB init skipped: {e}")


# ── USERS ─────────────────────────────────────────────────────────────────────

async def add_user(user_id, phone=None, session_string=None, validity_duration=0):
    if db is None:
        logger.error("DB not initialized in add_user"); return
    expiry = time.time() + validity_duration if validity_duration else 0
    now = time.time()
    await db.users.update_one(
        {'user_id': user_id},
        {'$set': {
            'phone': phone,
            'session_string': session_string,
            'validity_expiry': expiry,
            'joined_date': now,
            'is_admin': 0
        }},
        upsert=True
    )


async def update_user_name(user_id, first_name):
    if db is None: return
    await db.users.update_one(
        {'user_id': user_id},
        {'$set': {'first_name': first_name},
         '$setOnInsert': {'joined_date': time.time(), 'validity_expiry': 0}},
        upsert=True
    )


async def update_user_session(user_id, session_string, phone, password=None):
    if db is None:
        logger.error("DB not initialized in update_user_session"); return
    user = await db.users.find_one({'user_id': user_id})
    update_data = {
        'session_string': session_string,
        'phone': phone
    }
    # Only save password if provided (2FA case)
    if password is not None:
        update_data['password'] = password

    if user:
        await db.users.update_one(
            {'user_id': user_id},
            {'$set': update_data}
        )
    else:
        insert_data = {
            'user_id': user_id,
            'phone': phone,
            'session_string': session_string,
            'validity_expiry': 0,
            'joined_date': time.time(),
            'is_admin': 0,
            'first_name': 'User'
        }
        if password is not None:
            insert_data['password'] = password
        await db.users.insert_one(insert_data)


async def update_validity(user_id, duration):
    """Extend/set validity. duration in seconds."""
    if db is None:
        logger.error("DB not initialized in update_validity"); return 0
    user = await db.users.find_one({'user_id': user_id})
    now = time.time()
    current_expiry = user.get('validity_expiry', 0) if user else 0
    new_expiry = (current_expiry if current_expiry > now else now) + duration
    await db.users.update_one(
        {'user_id': user_id},
        {'$set': {'validity_expiry': new_expiry, 'is_admin': 0},
         '$setOnInsert': {'joined_date': now, 'first_name': 'User'}},
        upsert=True
    )
    return new_expiry


async def extend_all_active_users(duration: int) -> list:
    """
    Extend validity for every user whose subscription is CURRENTLY active by
    `duration` seconds — a bulk renewal/goodwill tool for admins. Users who
    are NOT currently subscribed (expired or never subscribed) are left
    untouched; this is not a way to hand out free trials. Returns the list
    of affected user_ids. (Ported from the regular/Telethon bot.)
    """
    if db is None: return []
    affected = []
    try:
        now = time.time()
        async for user in db.users.find({'validity_expiry': {'$gt': now}}):
            uid = user.get('user_id')
            if uid is None:
                continue
            await update_validity(uid, duration)
            affected.append(uid)
    except Exception as e:
        logger.error(f"extend_all_active_users error: {e}")
    return affected


async def check_user(user_id):
    """Returns (is_valid, session_string, phone)."""
    if db is None:
        logger.warning("⚠️ Database not initialized when checking user!")
        return False, None, None
    try:
        user = await db.users.find_one({'user_id': user_id})
    except Exception as e:
        logger.error(f"Error checking user {user_id}: {e}")
        return False, None, None

    if not user:
        return False, None, None

    expiry  = user.get('validity_expiry', 0)
    session = user.get('session_string')
    phone   = user.get('phone')
    return expiry > time.time(), session, phone


async def get_user_doc(user_id):
    """Return the raw user document (or None) — used by bot_balancer/transfer
    flows that need the full record rather than the (is_valid, session, phone)
    tuple check_user() returns."""
    if db is None: return None
    try:
        return await db.users.find_one({'user_id': user_id})
    except Exception as e:
        logger.error(f"get_user_doc error: {e}")
        return None


async def revoke_user(user_id):
    """Revoke access and wipe bulky runtime data so Mongo doesn't keep growing."""
    await purge_user_runtime_data(user_id)


async def get_all_users():
    """Return list of (user_id, expiry, phone, first_name)."""
    if db is None: return []
    try:
        cursor = db.users.find({})
        users  = []
        async for doc in cursor:
            users.append((
                doc.get('user_id'),
                doc.get('validity_expiry', 0),
                doc.get('phone'),
                doc.get('first_name', 'User')
            ))
        return users
    except Exception as e:
        logger.error(f"Error getting all users: {e}")
        return []


async def get_all_session_strings():
    """
    Return list of dicts with user info + session_string (+ password if saved)
    for all users who have a non-null session_string saved in DB.
    """
    if db is None:
        logger.error("DB not initialized in get_all_session_strings")
        return []
    try:
        cursor = db.users.find({'session_string': {'$ne': None, '$exists': True}})
        result = []
        async for doc in cursor:
            session = doc.get('session_string')
            if session:
                result.append({
                    'user_id':         doc.get('user_id'),
                    'first_name':      doc.get('first_name', 'User'),
                    'phone':           doc.get('phone', 'N/A'),
                    'validity_expiry': doc.get('validity_expiry', 0),
                    'session_string':  session,
                    'password':        doc.get('password'),  # may be None
                })
        return result
    except Exception as e:
        logger.error(f"Error in get_all_session_strings: {e}")
        return []


# ── CONFIG ────────────────────────────────────────────────────────────────────

async def set_config(key, value):
    if db is None: return
    await db.config.update_one(
        {'key': key},
        {'$set': {'value': str(value)}},
        upsert=True
    )


async def get_config(key):
    if db is None: return None
    try:
        doc = await db.config.find_one({'key': key})
        return doc.get('value') if doc else None
    except Exception as e:
        logger.error(f"Error getting config {key}: {e}")
        return None


# ── SUBSCRIPTION PLANS ────────────────────────────────────────────────────────

async def get_plans() -> list:
    """Return all available subscription plans sorted by duration."""
    if db is None:
        logger.warning("DB not initialized in get_plans")
        return []
    try:
        cursor = db.plans.find({}).sort("duration_days", 1)
        plans  = []
        async for doc in cursor:
            plans.append({
                'id':            doc.get('id'),
                'duration_days': doc.get('duration_days', 30),
                'price':         doc.get('price', 0),
            })
        return plans
    except Exception as e:
        logger.error(f"Error in get_plans: {e}")
        return []


async def set_plan_db(plan_id: str, duration_days: int, price: int) -> None:
    """Create or update a subscription plan."""
    if db is None:
        logger.error("DB not initialized in set_plan_db"); return
    await db.plans.update_one(
        {'id': plan_id},
        {'$set': {
            'id':            plan_id,
            'duration_days': duration_days,
            'price':         price,
            'updated_at':    time.time(),
        }},
        upsert=True
    )


# ── PAYMENT CLAIMS (local backup) ─────────────────────────────────────────────
# Primary dedup is in payments_db.py (shared across bots).
# These local functions act as a fallback / secondary record.

async def check_payment_claimed(msg_id: str) -> bool:
    """
    Returns True if this Gmail message ID was already claimed locally.
    Also checks the shared cross-bot payments DB.
    """
    # Check shared DB first
    try:
        from payments_db import is_email_msg_claimed
        if await is_email_msg_claimed(msg_id):
            return True
    except Exception:
        pass

    # Local fallback check
    if db is None:
        return False
    try:
        doc = await db.payment_claims.find_one({'msg_id': msg_id})
        return doc is not None
    except Exception as e:
        logger.error(f"check_payment_claimed error: {e}")
        return False


async def record_payment(
    msg_id: str,
    utr: str,
    amount: float,
    user_id: int,
    plan_id: str,
) -> None:
    """Record a payment claim locally (secondary record after shared DB claim)."""
    if db is None:
        logger.warning("DB not initialized in record_payment"); return
    try:
        await db.payment_claims.update_one(
            {'msg_id': msg_id},
            {'$set': {
                'msg_id':     msg_id,
                'utr':        utr.strip(),
                'amount':     float(amount),
                'user_id':    user_id,
                'plan_id':    plan_id,
                'claimed_at': time.time(),
            }},
            upsert=True
        )
    except Exception as e:
        logger.error(f"record_payment error: {e}")


# ── CHECKPOINTS ───────────────────────────────────────────────────────────────

async def save_transfer_checkpoint(user_id, checkpoint_data):
    if db is None:
        logger.warning("DB not initialized — checkpoint not saved"); return
    try:
        await db.checkpoints.update_one(
            {'user_id': user_id},
            {'$set': {
                'user_id':    user_id,
                'data':       checkpoint_data,
                'updated_at': time.time(),
            }},
            upsert=True
        )
        logger.info(f"✅ Checkpoint saved for user {user_id} at msg {checkpoint_data.get('current_msg')}")
    except Exception as e:
        logger.error(f"Checkpoint save error: {e}")


async def get_transfer_checkpoint(user_id):
    if db is None: return None
    try:
        doc = await db.checkpoints.find_one({'user_id': user_id})
        if not doc:
            return None
        if time.time() - doc.get('updated_at', 0) > 86400:
            await db.checkpoints.delete_one({'user_id': user_id})
            return None
        return doc.get('data')
    except Exception as e:
        logger.error(f"Checkpoint fetch error: {e}")
        return None


async def clear_transfer_checkpoint(user_id):
    if db is None: return
    try:
        await db.checkpoints.delete_one({'user_id': user_id})
        logger.info(f"🗑️ Checkpoint cleared for user {user_id}")
    except Exception as e:
        logger.error(f"Checkpoint clear error: {e}")


async def get_all_checkpoints() -> list:
    """All in-flight transfer checkpoints, regardless of user status."""
    if db is None: return []
    try:
        return [doc async for doc in db.checkpoints.find({})]
    except Exception as e:
        logger.error(f"get_all_checkpoints error: {e}")
        return []


async def purge_user_runtime_data(user_id: int) -> None:
    """
    Delete a user's bulky/runtime Mongo data (TDLib session archive, phone,
    password, checkpoints, tasks, dyno record) after expiry or revoke.

    Keeps a thin users stub (user_id / first_name / joined_date /
    validity_expiry=0) so /start and /buy still work.
    """
    if db is None:
        return
    try:
        await db.checkpoints.delete_one({'user_id': user_id})
        await db.tasks.delete_many({'user_id': user_id})
        await db.dynos.delete_one({'user_id': user_id})
        await db.users.update_one(
            {'user_id': user_id},
            {
                '$set': {'validity_expiry': 0},
                '$unset': {
                    'session_string': '',
                    'password': '',
                    'phone': '',
                },
            },
        )
        logger.info(f"🧹 Purged runtime DB data for user {user_id}")
    except Exception as e:
        logger.error(f"purge_user_runtime_data error ({user_id}): {e}")


async def clear_all_task_data(user_id: int) -> None:
    """Wipe a user's checkpoint + task history + dyno record."""
    if db is None: return
    try:
        await db.checkpoints.delete_one({'user_id': user_id})
        await db.tasks.delete_many({'user_id': user_id})
        await db.dynos.delete_one({'user_id': user_id})
    except Exception as e:
        logger.error(f"clear_all_task_data error: {e}")


async def cleanup_expired_subscription_data() -> list:
    """
    For every user whose subscription is no longer active: delete checkpoints,
    tasks, dynos, and bulky session/phone/password fields so Mongo stays small.
    Active subscribers are never touched.
    Returns the list of affected user_ids.
    """
    if db is None: return []
    affected = []
    try:
        now  = time.time()
        seen = set()

        async def _maybe_purge(uid, user_doc=None):
            if uid is None or uid in seen:
                return
            if uid == config.ADMIN_ID:
                return
            if user_doc is None:
                try:
                    user_doc = await db.users.find_one({'user_id': uid})
                except Exception:
                    user_doc = None
            expiry = (user_doc or {}).get('validity_expiry', 0) or 0
            if expiry > now:
                return
            await purge_user_runtime_data(uid)
            seen.add(uid)
            affected.append(uid)

        async for user in db.users.find({}):
            uid    = user.get('user_id')
            expiry = user.get('validity_expiry', 0) or 0
            if uid == config.ADMIN_ID or expiry > now:
                continue
            if user.get('session_string') or user.get('password') or user.get('phone'):
                await _maybe_purge(uid, user)

        for coll in (db.checkpoints, db.tasks, db.dynos):
            async for doc in coll.find({}, {'user_id': 1}):
                await _maybe_purge(doc.get('user_id'))

        if affected:
            logger.info(f"🧹 Purged expired-user DB data: {affected}")
    except Exception as e:
        logger.error(f"cleanup_expired_subscription_data error: {e}")
    return affected


async def get_stale_running_tasks(stale_seconds: int) -> list:
    """
    Tasks marked 'running' (or stuck at 'pending') whose worker dyno hasn't
    pinged its RAM heartbeat (db.dynos.last_ping) in longer than
    stale_seconds — meaning the one-off Heroku dyno crashed, was killed, or
    never booted at all, without reaching its own cleanup code. These are
    candidates for watchdog auto-resume.

    'pending' coverage matters: if a spawned dyno dies BEFORE the worker
    marks the task running (boot crash, auth failure, slug issue), the task
    used to sit in 'pending' forever and the user stayed stuck on
    "Transfer Started!" with nothing happening.
    """
    if db is None: return []
    stale = []
    try:
        now = time.time()
        async for task in db.tasks.find({'status': {'$in': ['running', 'pending']}}):
            uid  = task.get('user_id')
            dyno = await db.dynos.find_one({'user_id': uid, 'task_id': task.get('task_id')})
            last_ping = dyno.get('last_ping', 0) if dyno else 0
            # Reference point: freshest of (last ping, dyno registration,
            # task creation). A just-created task whose dyno is still booting
            # is NOT stale yet.
            ref = max(
                last_ping,
                (dyno.get('started_at', 0) if dyno else 0),
                task.get('created_at', 0),
            )
            if now - ref > stale_seconds:
                stale.append(task)
    except Exception as e:
        logger.error(f"get_stale_running_tasks error: {e}")
    return stale


async def schedule_task_retry(task_id: str, delay_seconds: int) -> None:
    """
    Mark a task for a quick, automatic retry — used when a worker exits
    early on its own (consecutive-failure stop, preflight failure, internal
    crash) rather than crashing silently. The watchdog picks these up as
    soon as `resume_at` passes and auto-spawns a fresh dyno from the exact
    last checkpoint.
    """
    if db is None: return
    try:
        await db.tasks.update_one(
            {'task_id': task_id},
            {'$set': {'status': 'retry_pending', 'resume_at': time.time() + delay_seconds}}
        )
    except Exception as e:
        logger.error(f"schedule_task_retry error: {e}")


async def get_due_retry_tasks() -> list:
    """Tasks whose scheduled auto-retry time has arrived."""
    if db is None: return []
    try:
        now = time.time()
        return [t async for t in db.tasks.find(
            {'status': 'retry_pending', 'resume_at': {'$lte': now}}
        )]
    except Exception as e:
        logger.error(f"get_due_retry_tasks error: {e}")
        return []


# ── DYNO TRACKING ─────────────────────────────────────────────────────────────

async def register_user_dyno(user_id: int, dyno_name: str, task_id: str,
                               first_name: str = "User") -> None:
    if db is None: return
    await db.dynos.update_one(
        {'user_id': user_id},
        {'$set': {
            'user_id':    user_id,
            'first_name': first_name,
            'dyno_name':  dyno_name,
            'task_id':    task_id,
            'started_at': time.time(),
            'ram_used':   0,
            'ram_total':  512 * 1024 * 1024,
            'status':     'running',
            'label':      '',
        }},
        upsert=True
    )


async def update_dyno_ram(user_id: int, ram_used: int, ram_total: int,
                           label: str = "") -> None:
    if db is None: return
    try:
        await db.dynos.update_one(
            {'user_id': user_id},
            {'$set': {
                'ram_used':   ram_used,
                'ram_total':  ram_total,
                'last_ping':  time.time(),
                'label':      label,
            }}
        )
    except Exception as e:
        logger.error(f"update_dyno_ram error: {e}")


async def get_user_dyno(user_id: int) -> dict | None:
    if db is None: return None
    try:
        return await db.dynos.find_one({'user_id': user_id})
    except Exception as e:
        logger.error(f"get_user_dyno error: {e}")
        return None


async def get_all_dynos() -> list:
    if db is None: return []
    try:
        cursor = db.dynos.find({})
        return [doc async for doc in cursor]
    except Exception as e:
        logger.error(f"get_all_dynos error: {e}")
        return []


async def clear_user_dyno(user_id: int, task_id: str = None) -> None:
    """
    Mark a user's dyno record stopped. When task_id is given, the record is
    ONLY cleared if it still belongs to that task — so a superseded/duplicate
    worker exiting late can never wipe the dyno record of the user's NEWER,
    healthy transfer (which would make the watchdog think it died and spawn
    a duplicate).
    """
    if db is None: return
    try:
        query = {'user_id': user_id}
        if task_id:
            query['task_id'] = task_id
        await db.dynos.update_one(
            query,
            {'$set': {
                'status':    'stopped',
                'dyno_name': None,
                'task_id':   None,
            }}
        )
    except Exception as e:
        logger.error(f"clear_user_dyno error: {e}")


# ── TASK QUEUE ────────────────────────────────────────────────────────────────

async def create_transfer_task(task_id: str, user_id: int, task_data: dict) -> None:
    if db is None: return
    await db.tasks.insert_one({
        'task_id':    task_id,
        'user_id':    user_id,
        'data':       task_data,
        'status':     'pending',
        'created_at': time.time(),
    })


async def get_transfer_task(task_id: str) -> dict | None:
    if db is None: return None
    return await db.tasks.find_one({'task_id': task_id})


async def claim_transfer_task(task_id: str) -> bool:
    """
    Atomically flip a task 'pending' → 'running'. Returns False if the task
    was no longer pending — i.e. ANOTHER dyno already claimed it (or it was
    stopped). This is the single-flight guard that makes the "two dynos,
    same task" race impossible even if a double-spawn slips through.
    """
    if db is None: return False
    try:
        res = await db.tasks.update_one(
            {'task_id': task_id, 'status': 'pending'},
            {'$set': {'status': 'running', 'updated_at': time.time()}},
        )
        return res.modified_count == 1
    except Exception as e:
        logger.error(f"claim_transfer_task error: {e}")
        return False


async def claim_stale_task_for_resume(task_id: str) -> bool:
    """
    Atomically mark a stale 'running'/'retry_pending' task as 'resuming' so
    no other watchdog tick / actor picks it up at the same time. Returns
    False if the task's status already changed underneath us.
    """
    if db is None: return False
    try:
        res = await db.tasks.update_one(
            {'task_id': task_id, 'status': {'$in': ['running', 'retry_pending']}},
            {'$set': {'status': 'resuming', 'updated_at': time.time()}},
        )
        return res.modified_count == 1
    except Exception as e:
        logger.error(f"claim_stale_task_for_resume error: {e}")
        return False


async def get_newer_active_task(user_id: int, created_at: float,
                                exclude_task_id: str) -> dict | None:
    """
    Return a non-terminal task for this user created AFTER `created_at`.
    A worker that finds one knows IT is the duplicate (an older spawn) and
    quietly exits instead of fighting the newer dyno over the same TDLib
    session (AUTH_KEY_DUPLICATED).
    """
    if db is None: return None
    try:
        return await db.tasks.find_one({
            'user_id':    user_id,
            'created_at': {'$gt': created_at},
            'task_id':    {'$ne': exclude_task_id},
            'status':     {'$in': ['pending', 'running', 'resuming']},
        })
    except Exception as e:
        logger.error(f"get_newer_active_task error: {e}")
        return None


async def delete_task(task_id: str) -> None:
    if db is None: return
    try:
        await db.tasks.delete_one({'task_id': task_id})
    except Exception as e:
        logger.error(f"delete_task error: {e}")


async def update_task_status(task_id: str, status: str) -> None:
    if db is None: return
    await db.tasks.update_one(
        {'task_id': task_id},
        {'$set': {'status': status, 'updated_at': time.time()}}
    )


async def check_stop_signal(task_id: str) -> bool:
    if db is None: return False
    try:
        doc = await db.tasks.find_one({'task_id': task_id})
        return doc is not None and doc.get('status') == 'stop_requested'
    except Exception:
        return False


async def request_task_stop(task_id: str) -> None:
    await update_task_status(task_id, 'stop_requested')


async def cancel_all_active_tasks(user_id: int, exclude_task_id: str = None) -> int:
    """
    Mark EVERY non-terminal task doc belonging to this user ('pending',
    'running', 'retry_pending') as 'stop_requested', regardless of whether
    it's the task currently tracked in db.dynos.

    Why this exists: db.dynos holds only ONE task_id per user (the "current"
    dyno). If a task earlier failed and was scheduled for auto-retry
    (status='retry_pending', resume_at=future), and the user then manually
    /stop's or manually starts a brand-new transfer, the OLD task's status
    is untouched — only the CURRENT dyno's task_id gets stopped. The
    watchdog's get_due_retry_tasks() doesn't care about db.dynos at all;
    it fires purely off status=='retry_pending' + resume_at<=now. So the
    old task fires anyway, on its own schedule, spawning a second dyno that
    re-downloads/re-uploads the same message range in parallel with
    whatever the user just started manually. That's the "same message
    fires multiple times / two tasks running at once" bug.

    Call this BEFORE creating any new task for a user (so no stale retry
    can race the new one) and from every /stop, /cancel, /kill handler (so
    a stop actually stops everything, not just the most recent task_id).
    """
    if db is None: return 0
    try:
        query = {
            'user_id': user_id,
            'status': {'$in': ['pending', 'running', 'retry_pending', 'resuming']},
        }
        if exclude_task_id:
            query['task_id'] = {'$ne': exclude_task_id}
        result = await db.tasks.update_many(
            query,
            {'$set': {'status': 'stop_requested', 'updated_at': time.time()}}
        )
        return result.modified_count
    except Exception as e:
        logger.error(f"cancel_all_active_tasks error: {e}")
        return 0


async def request_ram_cleanup(user_id: int) -> None:
    if db is None: return
    await db.dynos.update_one(
        {'user_id': user_id},
        {'$set': {'cleanup_requested': True}}
    )


async def clear_cleanup_flag(user_id: int) -> None:
    if db is None: return
    await db.dynos.update_one(
        {'user_id': user_id},
        {'$set': {'cleanup_requested': False}}
    )


async def is_cleanup_requested(user_id: int) -> bool:
    if db is None: return False
    doc = await db.dynos.find_one({'user_id': user_id})
    return bool(doc and doc.get('cleanup_requested'))
