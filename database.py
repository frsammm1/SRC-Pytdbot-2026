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
    if db is None: return
    await db.users.update_one(
        {'user_id': user_id},
        {'$set': {'validity_expiry': 0, 'session_string': None}}
    )


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


async def clear_all_task_data(user_id: int) -> None:
    """Wipe a user's checkpoint + task history. Used on subscription expiry/revoke."""
    if db is None: return
    try:
        await db.checkpoints.delete_one({'user_id': user_id})
        await db.tasks.delete_many({'user_id': user_id})
    except Exception as e:
        logger.error(f"clear_all_task_data error: {e}")


async def cleanup_expired_subscription_data() -> list:
    """
    Delete checkpoint/task records for any user whose subscription is no
    longer active. Users with a valid subscription are never touched — their
    real-time progress record stays exact so a failure never loses their spot.
    Returns the list of affected user_ids.
    """
    if db is None: return []
    affected = []
    try:
        now = time.time()
        async for doc in db.checkpoints.find({}):
            uid = doc.get('user_id')
            if uid is None:
                continue
            user   = await db.users.find_one({'user_id': uid})
            expiry = user.get('validity_expiry', 0) if user else 0
            if not expiry or expiry <= now:
                await clear_all_task_data(uid)
                affected.append(uid)
        if affected:
            logger.info(f"🧹 Cleared checkpoint/task data for expired users: {affected}")
    except Exception as e:
        logger.error(f"cleanup_expired_subscription_data error: {e}")
    return affected


async def get_stale_running_tasks(stale_seconds: int) -> list:
    """
    Tasks still marked 'running' whose worker dyno hasn't pinged its RAM
    heartbeat (db.dynos.last_ping) in longer than stale_seconds — meaning the
    one-off Heroku dyno almost certainly crashed/was killed without reaching
    its own cleanup code. These are candidates for watchdog auto-resume.
    """
    if db is None: return []
    stale = []
    try:
        now = time.time()
        async for task in db.tasks.find({'status': 'running'}):
            uid  = task.get('user_id')
            dyno = await db.dynos.find_one({'user_id': uid, 'task_id': task.get('task_id')})
            last_ping = dyno.get('last_ping', 0) if dyno else 0
            if now - last_ping > stale_seconds:
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


async def clear_user_dyno(user_id: int) -> None:
    if db is None: return
    try:
        await db.dynos.update_one(
            {'user_id': user_id},
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
            'status': {'$in': ['pending', 'running', 'retry_pending']},
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
