"""
payments_db.py  –  Cross-bot shared payment deduplication + global dynamic pricing.

PREMIUM BOT VERSION — v6.1 PRICE ISOLATION FIX:

  PROBLEM: Regular (Telethon) bots aur Premium (Pyrofork) bot dono same shared
           MongoDB use karte the. 'plan_prices' collection shared hoti thi,
           jisse regular bot ke seeded prices premium bot pe bhi dikhte the.

  FIX:
    • Regular bots  → 'plan_prices'         collection (₹40–₹50 for 15d, etc.)
    • Premium bot   → 'premium_plan_prices'  collection (₹50–₹60 for 15d, etc.)
    • 'claimed_payments' → SHARED across ALL bots (UTR global dedup)
    • regular bots  → 'global_state'
    • Premium bot   → 'premium_global_state'

  This ensures:
    1. Premium prices are always ₹10 higher than regular, independently
    2. One UTR still cannot be used on ANY bot (global dedup preserved)
    3. Regular bot price rotations don't affect premium bot prices
"""

import motor.motor_asyncio
import logging
import os
import time
import random

logger = logging.getLogger(__name__)

# ── HARDCODED SHARED URI ──────────────────────────────────────────────────────
_HARDCODED_SHARED_URI = os.environ.get("SHARED_MONGO_URI", "")
_SHARED_URI = os.environ.get("SHARED_MONGO_URI") or _HARDCODED_SHARED_URI
_BOT_NAME   = os.environ.get("BOT_NAME", "premium_bot")

_shared_client = None
_shared_db     = None

# ── COLLECTION NAMES (Premium bot uses separate collections) ──────────────────
# UTR dedup is SHARED — one UTR = blocked everywhere (regular + premium)
_DEDUP_COLLECTION       = "claimed_payments"       # SHARED with regular bots
_PLAN_PRICES_COLLECTION = "premium_plan_prices"    # PREMIUM ONLY
_GLOBAL_STATE_KEY       = "premium_approval_count" # PREMIUM ONLY (separate counter)
_BOT_REGISTRY_COLLECTION = "bot_registry"          # SHARED across ALL bots (regular + premium)
                                                    # — used for cross-bot load balancing only,
                                                    # completely separate from UTR dedup above.

# ── PREMIUM PLAN DEFINITIONS ───────────────────────────────────────────────────
# Format: plan_id → (duration_days, min_price, max_price)
# 21-Jul-2026: +₹20 across every plan (all day-tiers), same flat bump applied
# uniformly. Old range kept in the inline comment for reference.
PLAN_DEFINITIONS = {
    "plan_15d": (15,  70,  80),   # was 50-60
    "plan_30d": (30, 105, 115),   # was 85-95
    "plan_45d": (45, 135, 145),   # was 115-125
    "plan_60d": (60, 165, 175),   # was 145-155
}

PLAN_ORDER = ["plan_15d", "plan_30d", "plan_45d", "plan_60d"]


def _random_price(min_p: int, max_p: int) -> int:
    return random.randint(min_p, max_p)


async def init_shared_payments_db() -> None:
    """
    Called once at startup (from database.init_db).
    Connects to shared payment MongoDB.
    
    Premium bot uses SEPARATE plan_prices collection to avoid price contamination
    from regular bots. UTR dedup collection remains shared.
    """
    global _shared_client, _shared_db

    if not _SHARED_URI:
        logger.error("❌ No shared payment DB URI! Cross-bot dedup will NOT work.")
        return

    try:
        _shared_client = motor.motor_asyncio.AsyncIOMotorClient(
            _SHARED_URI,
            serverSelectionTimeoutMS=10000,
        )
        _shared_db = _shared_client.get_default_database("PaymentAPi")

        # ── SHARED: UTR dedup (works across ALL bots — regular + premium) ──
        await _shared_db[_DEDUP_COLLECTION].create_index("utr",    unique=True)
        await _shared_db[_DEDUP_COLLECTION].create_index("msg_id", unique=True)

        # ── PREMIUM ONLY: Separate global state counter ────────────────────
        await _shared_db.premium_global_state.create_index("key", unique=True)

        # ── PREMIUM ONLY: Separate plan prices collection ─────────────────
        await _shared_db[_PLAN_PRICES_COLLECTION].create_index("plan_id", unique=True)

        # ── SHARED: bot registry for cross-bot load balancing ──────────────
        await _shared_db[_BOT_REGISTRY_COLLECTION].create_index("bot_id", unique=True)
        await _shared_db[_BOT_REGISTRY_COLLECTION].create_index("bot_type")

        # Seed premium prices (independent of regular bot prices)
        await _seed_premium_prices()

        logger.info(
            f"✅ Shared payments DB connected → {_shared_db.name} "
            f"(bot: {_BOT_NAME}) [PREMIUM MODE]"
        )
        logger.info(
            f"   Dedup collection: {_DEDUP_COLLECTION} (shared with regular bots)"
        )
        logger.info(
            f"   Plan prices collection: {_PLAN_PRICES_COLLECTION} (premium only)"
        )
    except Exception as e:
        logger.error(f"❌ Shared payments DB connection failed: {e}", exc_info=True)
        _shared_db = None


async def _seed_premium_prices() -> None:
    """
    Seed premium plan prices independently of regular bot prices.
    
    If a price exists but is below premium minimum (was contaminated by regular
    bot), it gets upgraded to the correct premium range.
    """
    for plan_id, (days, min_p, max_p) in PLAN_DEFINITIONS.items():
        try:
            existing = await _shared_db[_PLAN_PRICES_COLLECTION].find_one(
                {"plan_id": plan_id}
            )
            if not existing:
                # First time — seed premium price
                price = _random_price(min_p, max_p)
                await _shared_db[_PLAN_PRICES_COLLECTION].insert_one({
                    "plan_id":       plan_id,
                    "duration_days": days,
                    "price":         price,
                    "updated_at":    time.time(),
                    "tier":          "premium",
                })
                logger.info(f"💰 Seeded premium plan {plan_id}: ₹{price}")
            else:
                # If existing price is below premium minimum → correct it
                current_price = existing.get("price", 0)
                if current_price < min_p:
                    new_price = _random_price(min_p, max_p)
                    await _shared_db[_PLAN_PRICES_COLLECTION].update_one(
                        {"plan_id": plan_id},
                        {"$set": {
                            "price":      new_price,
                            "updated_at": time.time(),
                            "tier":       "premium",
                        }},
                    )
                    logger.info(
                        f"💰 Corrected premium plan {plan_id}: "
                        f"₹{current_price} → ₹{new_price} (was below premium minimum)"
                    )
        except Exception as e:
            logger.warning(f"_seed_premium_prices skip ({plan_id}): {e}")

    # Seed premium approval counter if missing
    try:
        existing = await _shared_db.premium_global_state.find_one(
            {"key": _GLOBAL_STATE_KEY}
        )
        if not existing:
            await _shared_db.premium_global_state.insert_one({
                "key":   _GLOBAL_STATE_KEY,
                "value": 0,
            })
    except Exception as e:
        logger.warning(f"_seed_premium_approval_counter skip: {e}")


def _is_db_ready() -> bool:
    if _shared_db is None:
        logger.error("🚨 Shared payment DB NOT connected!")
        return False
    return True


# ── GLOBAL PLAN PRICES (Premium) ──────────────────────────────────────────────

async def get_global_plans() -> list[dict]:
    """
    Return current PREMIUM plan prices from shared DB.
    Uses 'premium_plan_prices' collection — completely independent of regular bots.
    Falls back to premium midpoint prices if DB unavailable.
    """
    if not _is_db_ready():
        return [
            {"plan_id": pid, "duration_days": d, "price": (mn + mx) // 2}
            for pid, (d, mn, mx) in PLAN_DEFINITIONS.items()
        ]

    try:
        results = {}
        async for doc in _shared_db[_PLAN_PRICES_COLLECTION].find({}):
            results[doc["plan_id"]] = doc

        plans = []
        for plan_id in PLAN_ORDER:
            if plan_id in results:
                doc = results[plan_id]
                plans.append({
                    "plan_id":       plan_id,
                    "duration_days": doc["duration_days"],
                    "price":         doc["price"],
                })
            else:
                d, mn, mx = PLAN_DEFINITIONS[plan_id]
                plans.append({
                    "plan_id":       plan_id,
                    "duration_days": d,
                    "price":         (mn + mx) // 2,
                })
        return plans
    except Exception as e:
        logger.error(f"get_global_plans error: {e}")
        return []


async def _rotate_premium_prices() -> None:
    """
    Rotate premium plan prices independently of regular bot rotations.
    Called after every 2nd premium approval.
    """
    if not _is_db_ready():
        return
    try:
        for plan_id, (days, min_p, max_p) in PLAN_DEFINITIONS.items():
            new_price = _random_price(min_p, max_p)
            await _shared_db[_PLAN_PRICES_COLLECTION].update_one(
                {"plan_id": plan_id},
                {"$set": {
                    "price":      new_price,
                    "updated_at": time.time(),
                    "tier":       "premium",
                }},
                upsert=True,
            )
            logger.info(f"🔄 Premium price rotated: {plan_id} → ₹{new_price}")
    except Exception as e:
        logger.error(f"_rotate_premium_prices error: {e}")


async def _increment_premium_approval_counter() -> int:
    """
    Increment PREMIUM-ONLY approval counter (separate from regular bot counter).
    Returns the new counter value.
    """
    if not _is_db_ready():
        return 0
    try:
        result = await _shared_db.premium_global_state.find_one_and_update(
            {"key": _GLOBAL_STATE_KEY},
            {"$inc": {"value": 1}},
            upsert=True,
            return_document=True,
        )
        new_val = result.get("value", 1) if result else 1
        logger.info(f"📊 Premium approval counter: {new_val}")
        return new_val
    except Exception as e:
        logger.error(f"_increment_premium_approval_counter error: {e}")
        return 0


# ── PAYMENT DEDUP (SHARED across ALL bots) ────────────────────────────────────
# These functions use 'claimed_payments' collection which is SHARED with regular bots.
# One UTR = blocked on ALL bots (regular + premium) — globally.

async def is_utr_claimed(utr: str) -> bool:
    """Check if UTR was already claimed on ANY bot (regular or premium)."""
    if not _is_db_ready():
        return True
    try:
        doc = await _shared_db[_DEDUP_COLLECTION].find_one({"utr": utr.strip()})
        return doc is not None
    except Exception as e:
        logger.error(f"is_utr_claimed error: {e}")
        return True


async def is_email_msg_claimed(msg_id: str) -> bool:
    """Check if Gmail message was already claimed on ANY bot."""
    if not _is_db_ready():
        return True
    try:
        doc = await _shared_db[_DEDUP_COLLECTION].find_one({"msg_id": msg_id})
        return doc is not None
    except Exception as e:
        logger.error(f"is_email_msg_claimed error: {e}")
        return True


async def claim_payment(
    utr: str,
    msg_id: str,
    amount: float,
    user_id: int,
    plan_id: str,
) -> tuple[bool, str]:
    """
    Atomically claim a payment in the SHARED dedup DB.
    
    Dedup is global — UTR claimed on regular bot blocks it on premium bot too.
    On success, increments PREMIUM-ONLY approval counter and rotates premium prices.
    
    Returns:
        (True,  "")           – claimed, proceed with activation
        (False, reason_str)   – already claimed or DB error
    """
    if not _is_db_ready():
        return False, (
            "❌ Payment verification service is temporarily unavailable.\n"
            "Please contact admin."
        )

    record = {
        "utr":        utr.strip(),
        "msg_id":     msg_id,
        "amount":     float(amount),
        "user_id":    user_id,
        "plan_id":    plan_id,
        "bot_name":   _BOT_NAME,
        "bot_tier":   "premium",
        "claimed_at": time.time(),
    }

    try:
        await _shared_db[_DEDUP_COLLECTION].insert_one(record)
        logger.info(
            f"✅ Premium payment claimed: UTR={utr} user={user_id} "
            f"plan={plan_id} bot={_BOT_NAME}"
        )

        # Increment PREMIUM-ONLY counter & rotate premium prices every 2 approvals
        new_count = await _increment_premium_approval_counter()
        if new_count % 2 == 0:
            logger.info(f"🔄 {new_count} premium approvals — rotating premium prices!")
            await _rotate_premium_prices()

        return True, ""

    except Exception as e:
        err_str = str(e)

        if "duplicate key" in err_str.lower() or "E11000" in err_str:
            try:
                existing = await _shared_db[_DEDUP_COLLECTION].find_one(
                    {"$or": [{"utr": utr.strip()}, {"msg_id": msg_id}]}
                )
            except Exception:
                existing = None

            if existing:
                bot      = existing.get("bot_name", "another bot")
                uid      = existing.get("user_id", "unknown")
                bot_tier = existing.get("bot_tier", "")
                tier_str = f" ({bot_tier})" if bot_tier else ""
                reason = (
                    f"❌ This UTR was already used to activate a subscription "
                    f"(claimed on **{bot}**{tier_str} by user `{uid}`).\n\n"
                    "Each payment UTR can only be used once across all bots."
                )
            else:
                reason = (
                    "❌ This UTR has already been claimed.\n"
                    "Each payment UTR can only be used once."
                )

            logger.warning(
                f"🚫 Duplicate claim blocked: UTR={utr} user={user_id} bot={_BOT_NAME}"
            )
            return False, reason

        logger.error(f"claim_payment insert error: {e}", exc_info=True)
        return False, "❌ Database error — please try again in a moment or contact admin."


# ── BOT REGISTRY (cross-bot load balancing) ───────────────────────────────────
#
# One shared collection, used by BOTH regular and premium bots. Each running
# bot process periodically upserts its own row (keyed by the operator's
# stable BOT_ID — never the @username, which gets rotated/recreated). This
# lets any bot instance look up "which bot of my own type currently has the
# fewest active subscribers" and, on a brand-new purchase, place the
# resulting subscription directly into THAT bot's own persistent MongoDB
# (via the stored mongo_uri) — completely independent of the UTR dedup
# system above, which is never touched by any of this.

async def refresh_bot_registry(
    bot_id: str,
    bot_type: str,
    username: str,
    mongo_uri: str,
    subscriber_count: int,
) -> None:
    """Self-report this bot's identity + current load into the shared registry."""
    if not _is_db_ready() or not bot_id:
        return
    try:
        await _shared_db[_BOT_REGISTRY_COLLECTION].update_one(
            {"bot_id": bot_id},
            {"$set": {
                "bot_id":           bot_id,
                "bot_type":         bot_type,
                "username":         username or "",
                "mongo_uri":        mongo_uri,
                "subscriber_count": subscriber_count,
                "bot_name":         _BOT_NAME,
                "updated_at":       time.time(),
            }},
            upsert=True,
        )
    except Exception as e:
        logger.error(f"refresh_bot_registry error: {e}")


async def get_least_loaded_bot(bot_type: str, self_bot_id: str) -> dict | None:
    """
    Return the registry entry (bot_id, username, mongo_uri, subscriber_count)
    for the bot with the fewest active subscribers among all bots sharing
    `bot_type` ('R' or 'P') — including this bot itself, so the caller can
    tell "stay here" apart from "move to bot X" by comparing bot_id.

    Entries not refreshed in the last 20 minutes are treated as possibly
    dead and ignored — UNLESS every entry is stale, in which case we fall
    back to the freshest one rather than refusing to place the purchase
    anywhere.

    Returns None if the registry has nothing for this bot_type yet (e.g. no
    bot has been updated to this feature) — callers should treat that as
    "just apply the purchase locally, exactly as before".
    """
    if not _is_db_ready() or not self_bot_id:
        return None
    try:
        entries = [
            doc async for doc in _shared_db[_BOT_REGISTRY_COLLECTION].find(
                {"bot_type": bot_type}
            )
        ]
        if not entries:
            return None

        now = time.time()
        FRESH_WINDOW = 20 * 60  # 20 minutes
        fresh = [e for e in entries if now - e.get("updated_at", 0) <= FRESH_WINDOW]
        pool = fresh if fresh else entries

        pool.sort(key=lambda e: (e.get("subscriber_count", 0), e.get("bot_id", "")))
        return pool[0]
    except Exception as e:
        logger.error(f"get_least_loaded_bot error: {e}")
        return None
