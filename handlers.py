"""
handlers.py  –  All Telegram bot handlers  (v7.0 — TDLib / pytdbot)

v7.0 changes (Pyrofork → TDLib):
  • pyrogram decorators/filters → pytdbot on_message / on_updateNewCallbackQuery
    with custom filters (pytdbot ships only filters.create).
  • StopPropagation → pytdbot.StopHandlers.
  • FloodWait e.x → session_manager.FloodWaitError(.x) — TDLib reports 429s
    as plain Error objects, parsed in session_manager/config.
  • phone_code_hash no longer exists (TDLib tracks auth internally) — the
    LOGIN_STATES field is kept so the flow shape is unchanged.
  • Thumbnail file_id is now the TDLib REMOTE file id of the largest photo
    size (transfer.py resolves it via getRemoteFile → downloadFile).
  • Every user-facing text, button label and flow is byte-identical to the
    Pyrofork version.
"""

import asyncio
import re
import uuid
import time
import datetime
import os

import pytdbot
from pytdbot.exception import StopHandlers as _StopHandlers
from pytdbot import filters as _filters

import config
from keyboards import (
    get_settings_keyboard, get_confirm_keyboard,
    get_skip_keyboard, get_clone_info_keyboard,
    get_progress_keyboard, make_url_button,
)
from transfer import transfer_process
import database as db
from session_manager import (
    session_manager, SessionPasswordNeeded, PhoneCodeInvalid,
    PhoneCodeExpired, PhoneNumberInvalid, PasswordHashInvalid,
    FloodWaitError,
)
from heroku_manager import heroku_manager
from utils import (
    human_readable_size, time_formatter,
    extract_link_info, message_plain_text, message_html_text, _cname,
)


# ── MODULE-LEVEL STATE ────────────────────────────────────────────────────────

LOGIN_STATES: dict = {}

TUTORIAL_CHANNEL_BLOCK = (
    "\n\n━━━━━━━━━━━━━━━━━━━━\n"
    "📢🔔 **SRC Updates/Tutorial Channel Join Karke Rakhna Bahot Jaruri Hai!** ✅\n"
    "👉 https://t.me/+P3gz8MUClj9kMDE1 👈"
)
HEROKU_MODE = bool(os.environ.get("HEROKU_API_TOKEN") and os.environ.get("HEROKU_APP_NAME"))

DYNO_RAM_LABELS = {
    "free":          "512 MB RAM",
    "eco":           "512 MB RAM",
    "basic":         "512 MB RAM",
    "standard-1x":  "512 MB RAM",
    "standard-2x":  "1 GB RAM",
    "performance-m": "2.5 GB RAM",
    "performance-l": "14 GB RAM",
}

def get_dyno_label() -> str:
    size = os.environ.get("WORKER_DYNO_SIZE", "standard-1x").lower().strip()
    return DYNO_RAM_LABELS.get(size, size)

DYNOS_PER_PAGE = 5


# ── CUSTOM FILTERS (pytdbot has no built-ins) ─────────────────────────────────

def f_private():
    async def _f(_, message) -> bool:
        return getattr(message, 'chat_id', 0) > 0
    return _filters.create(_f)


def f_command(name: str):
    async def _f(_, message) -> bool:
        if _cname(getattr(message, 'content', None)) != 'MessageText':
            return False
        text = message_plain_text(message).strip()
        if not text.startswith('/'):
            return False
        cmd = text.split()[0].split('@')[0].lstrip('/')
        return cmd.lower() == name.lower()
    return _filters.create(_f)


def f_not_command(commands: list):
    async def _f(_, message) -> bool:
        if _cname(getattr(message, 'content', None)) != 'MessageText':
            return True   # media etc. can carry state (e.g. thumbnail step)
        text = message_plain_text(message).strip()
        if not text.startswith('/'):
            return True
        cmd = text.split()[0].split('@')[0].lstrip('/').lower()
        return cmd not in [c.lower() for c in commands]
    return _filters.create(_f)


def f_and(*flts):
    async def _f(client, message) -> bool:
        for flt in flts:
            fn = flt.func
            ok = await fn(client, message) if asyncio.iscoroutinefunction(fn) else fn(client, message)
            if not ok:
                return False
        return True
    return _filters.create(_f)


def f_cb_regex(pattern: str):
    rx = re.compile(pattern)
    async def _f(_, query) -> bool:
        try:
            return bool(rx.match(query.text or ""))
        except Exception:
            return False
    return _filters.create(_f)


def _cb_data(query) -> str:
    try:
        return query.text or ""
    except Exception:
        return ""


def _cb_match(query, pattern: str):
    return re.match(pattern, _cb_data(query))


# ── PAYMENT REMINDER ──────────────────────────────────────────────────────────

async def _schedule_payment_reminder(
    bot_client, user_id: int, chat_id: int,
    duration_days: int, price: int, login_states: dict
):
    await asyncio.sleep(300)
    state = login_states.get(user_id)
    if not state or state.get("state") != "WAIT_UTR":
        return
    try:
        await bot_client.sendTextMessage(
            chat_id,
            "⏰ **Payment Reminder**\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Your payment of **₹{price}** for the **{duration_days}-day plan** "
            "is still pending.\n\n"
            "If you're facing issues, contact admin:\n\n"
            "👤 @AlsoRaOne\n"
            "🤖 @DMtoSam1Bot\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "_If you already paid, send your **UTR** here._"
        )
    except Exception as e:
        config.logger.error(f"Reminder send failed for {user_id}: {e}")


# ── REGISTER ALL HANDLERS ─────────────────────────────────────────────────────

def register_handlers(bot_client):

    # ── Shared utilities ──────────────────────────────────────────────────────

    async def get_user_status(user_id: int) -> str:
        if user_id == config.ADMIN_ID:
            return "ADMIN"
        is_valid, _, _ = await db.check_user(user_id)
        return "PAID" if is_valid else "FREE"

    async def get_active_subscriber_count() -> int:
        try:
            users = await db.get_all_users()
            now   = time.time()
            return sum(1 for u in users if u[1] and u[1] > now)
        except Exception:
            return 0

    def format_expiry_ist(expiry_ts: float) -> str:
        utc_dt = datetime.datetime.fromtimestamp(expiry_ts, datetime.timezone.utc)
        ist_dt = utc_dt + datetime.timedelta(hours=5, minutes=30)
        return ist_dt.strftime('%d %b %Y, %I:%M %p IST')

    async def get_sender_name(client, user_id: int) -> str:
        try:
            u = await client.getUser(user_id=user_id)
            if not config.is_error(u) and u:
                return getattr(u, 'first_name', None) or "User"
        except Exception:
            pass
        return "User"

    def find_session_for_user(user_id: int):
        for sid, data in config.active_sessions.items():
            if data.get('user_id') == user_id:
                return sid
        return None

    def cancel_existing_sessions(user_id: int) -> int:
        to_delete = [
            sid for sid, data in config.active_sessions.items()
            if data.get('user_id') == user_id
        ]
        for sid in to_delete:
            task = config.active_sessions[sid].get('task_object')
            if task and not task.done():
                task.cancel()
            del config.active_sessions[sid]
        return len(to_delete)

    async def _kill_user_dyno(user_id: int) -> str:
        dyno_rec = await db.get_user_dyno(user_id)
        cancelled_count = await db.cancel_all_active_tasks(user_id)
        if not dyno_rec:
            if cancelled_count:
                return "🛑 Cancelled a pending auto-resume."
            return ""
        dyno_name = dyno_rec.get('dyno_name')
        killed    = False
        if dyno_name and HEROKU_MODE:
            killed = await heroku_manager.kill_dyno(dyno_name)
        await db.clear_user_dyno(user_id)
        if killed:
            return f"🖥️ Dyno `{dyno_name}` killed."
        elif dyno_name:
            return f"⚠️ Dyno API call failed but DB cleared. ({dyno_name})"
        return "🛑 Task stopped."

    def _make_ram_bar(ram_used: int, ram_total: int) -> str:
        if not ram_total:
            return "🧠 RAM: Waiting for data..."
        pct      = ram_used / ram_total * 100
        used_mb  = ram_used  / (1024 * 1024)
        total_mb = ram_total / (1024 * 1024)
        filled   = min(10, int(pct / 10))
        bar      = "█" * filled + "░" * (10 - filled)
        icon     = "✅" if pct < 60 else ("⚠️" if pct < 80 else "🔴")
        return f"🧠 {bar} {pct:.1f}% | `{used_mb:.0f}/{total_mb:.0f} MB` {icon}"

    def _build_dynos_page(dynos: list, page: int):
        from pytdbot import types
        now         = time.time()
        total       = len(dynos)
        total_pages = max(1, (total + DYNOS_PER_PAGE - 1) // DYNOS_PER_PAGE)
        page        = max(0, min(page, total_pages - 1))
        start       = page * DYNOS_PER_PAGE
        end         = start + DYNOS_PER_PAGE
        page_dynos  = dynos[start:end]
        active_count = sum(
            1 for d in dynos
            if d.get('status') == 'running' and d.get('last_ping') and now - d['last_ping'] < 30
        )
        msg  = f"🖥️ **Dyno Panel** | Page {page+1}/{total_pages}\n"
        msg += f"Active: **{active_count}** / Total: **{total}**\n"
        msg += "━━━━━━━━━━━━━━━━\n\n"
        for rec in page_dynos:
            uid        = rec.get('user_id', '?')
            fname      = rec.get('first_name', 'User')
            raw_tid    = rec.get('task_id')
            task_short = (raw_tid[:8] + "…") if raw_tid else "–"
            dyno_name  = rec.get('dyno_name') or '–'
            status     = rec.get('status', 'unknown')
            started_at = rec.get('started_at', 0)
            last_ping  = rec.get('last_ping', 0)
            ram_used   = rec.get('ram_used', 0)
            ram_total  = rec.get('ram_total', 0)
            label      = rec.get('label', '')
            alive      = (last_ping and now - last_ping < 30)
            if status == 'running' and alive:       status_icon = "🟢 Running"
            elif status == 'running' and not alive: status_icon = "🟡 Stale (no ping >30s)"
            else:                                   status_icon = "⚫ Stopped"
            running_sec = int(now - started_at) if started_at else 0
            running_fmt = time_formatter(running_sec) if running_sec else "–"
            ram_line    = _make_ram_bar(ram_used, ram_total) if ram_total else "🧠 RAM: Waiting..."
            msg += (
                f"👤 [{fname}](tg://user?id={uid}) (`{uid}`)\n"
                f"🖥️ `{dyno_name}` | {status_icon}\n"
                f"{ram_line}\n"
                f"📊 `{label}`\n"
                f"⏱️ Uptime: `{running_fmt}` | Task: `{task_short}`\n"
                f"🛑 `/kill_dyno {dyno_name}`\n\n"
            )
        if not HEROKU_MODE:
            msg += "\n⚠️ HEROKU_API_TOKEN / HEROKU_APP_NAME not set."

        def _nav_btn(text, data):
            return types.InlineKeyboardButton(
                text=text,
                type=types.InlineKeyboardButtonTypeCallback(data=data.encode()),
            )
        nav = []
        if page > 0:
            nav.append(_nav_btn("◀️ Prev",    f"dynos_page_{page-1}"))
        nav.append(    _nav_btn("🔄 Refresh", f"dynos_page_{page}"))
        if page < total_pages - 1:
            nav.append(_nav_btn("Next ▶️",    f"dynos_page_{page+1}"))
        markup = types.ReplyMarkupInlineKeyboard(rows=[nav]) if nav else None
        return msg, markup

    async def _safe_cb_edit(query, text: str, markup=None):
        """Edit the callback message, catching all exceptions."""
        try:
            await query.edit_message_text(text, reply_markup=markup, parse_mode="markdown")
        except Exception as e:
            config.logger.error(f"_safe_cb_edit failed: {e}")

    # ── THUMBNAIL HELPER ──────────────────────────────────────────────────────

    def _get_settings_kb(sid: str):
        """Get settings keyboard with current thumbnail status from session."""
        session       = config.active_sessions.get(sid, {})
        dest_topic_id = session.get('dest_topic_id')
        thumbnail_set = session.get('settings', {}).get('thumbnail_set', False)
        return get_settings_keyboard(sid, dest_topic_id=dest_topic_id, thumbnail_set=thumbnail_set)

    async def _reply(message, text: str, reply_markup=None):
        """Reply with markdown parse mode; never raise on TDLib errors."""
        res = await message.reply_text(text, parse_mode="markdown", reply_markup=reply_markup)
        return res

    # ═══════════════════════════════════════════════════════════════════════════
    # COMMAND HANDLERS
    # ═══════════════════════════════════════════════════════════════════════════

    @bot_client.on_message(filters=f_command("id"))
    async def id_handler(client, message):
        await _reply(message, f"🆔 Chat ID: `{message.chat_id}`")

    @bot_client.on_message(filters=f_and(f_command("start"), f_private()))
    async def start_handler(client, message):
        user_id    = message.from_id
        first_name = await get_sender_name(client, user_id)
        await db.update_user_name(user_id, first_name)
        status = await get_user_status(user_id)

        if status == "ADMIN":
            await _reply(message,
                "👑 **Admin Panel**\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "**Users:** `/add_user ID DUR` · `/add_user all DUR` · `/revoke ID` · `/users` · `/paid_users`\n"
                "**Dynos:** `/dynos` · `/kill_dyno NAME`\n"
                "**Plans:** `/setplan ID DAYS PRICE`\n"
                "**Export:** `/extract_string`\n"
                "**Broadcast:** Reply `/broadcast` to any message\n"
                "**System:** `/set_log CHANNEL_ID` · `/login` · `/clone`\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "**Shared with paid users (admin can use too):**\n"
                "`/stop` · `/kill` · `/cancel` · `/dyno_status` · `/cleanup_ram` · `/id` · `/help`"
            )
            return

        if status == "PAID":
            _, session, _ = await db.check_user(user_id)
            login_status  = "✅ Logged In" if session else "❌ Not Logged In"
            users         = await db.get_all_users()
            expiry_ts     = next((u[1] for u in users if u[0] == user_id), 0)
            expiry_str    = format_expiry_ist(expiry_ts) if expiry_ts else "Unknown"
            remaining_sec = max(0, int(expiry_ts - time.time())) if expiry_ts else 0
            rem_days      = remaining_sec // 86400
            rem_hrs       = (remaining_sec % 86400) // 3600
            if rem_days > 0:   rem_str = f"{rem_days}d {rem_hrs}h remaining"
            elif rem_hrs > 0:  rem_str = f"{rem_hrs}h remaining ⚠️"
            else:              rem_str = "Expiring soon! ⚠️"
            dyno_rec  = await db.get_user_dyno(user_id)
            dyno_line = ""
            if dyno_rec and dyno_rec.get('status') == 'running':
                dyno_line = f"\n🖥️ Active Dyno: `{dyno_rec.get('dyno_name', 'Unknown')}`"
            await _reply(message,
                f"🚀 **Content Saver Bot**\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 **{first_name}** | {login_status}{dyno_line}\n"
                f"📅 Expires: `{expiry_str}` _(⏳ {rem_str})_\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"`/login` — Connect your Telegram account\n"
                f"`/clone` — Start a new file transfer\n"
                f"`/stop` — Stop transfer & kill dyno\n"
                f"`/kill` — Force stop if transfer is stuck\n"
                f"`/dyno_status` — Check RAM usage\n"
                f"`/cleanup_ram` — Free up RAM during transfer\n"
                f"`/buy` — Renew subscription\n"
                f"`/transfer` — Move remaining validity to another account\n"
                f"`/cancel` — Cancel a running login/UTR/transfer step\n"
                f"`/resend_otp` — Resend login OTP\n"
                f"`/id` — Get a chat/user's Telegram ID\n"
                f"`/logout` — Logout\n"
                f"`/help` — Full usage guide"
                + TUTORIAL_CHANNEL_BLOCK,
                reply_markup=get_clone_info_keyboard()
            )
        else:
            active_subs = await get_active_subscriber_count()
            await _reply(message,
                f"⚡ **Fastest SRC bot on Telegram**\n"
                f"👋 **Welcome to Save Restricted Content Bot!**\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Kisi bhi **private ya public** Telegram channel/group se files forward karo —\n"
                f"chahe **forwarding band** ho tab bhi. **Topics/Threads** bhi supported hain.\n\n"
                f"✅ Dedicated RAM/Space Per User\n"
                f"✅ Forward-restricted channel/group supported\n"
                f"✅ Topics/Threads groups supported\n"
                f"✅ 2GB+ files · Smart caption & filename editing\n"
                f"✅ Custom transfer thumbnail support\n\n"
                f"👥 **{active_subs}** active subscribers\n\n"
                f"Access lene ke liye 👉 `/buy` command send karo"
            )

    @bot_client.on_message(filters=f_and(f_command("help"), f_private()))
    async def help_handler(client, message):
        user_id = message.from_id
        if user_id == config.ADMIN_ID:
            await _reply(message,
                "👑 **Admin Guide**\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "**Plans:** `/setplan plan_30d 30 199` → 30 days ₹199\n"
                "**Users:** `/add_user ID DUR` (e.g. `30d`, `1h`, `7d`) · `/add_user all DUR` (extends every active user) · `/revoke ID`\n"
                "**Lists:** `/users` (all) · `/paid_users` (active only)\n"
                "**Dynos:** `/dynos` · `/kill_dyno NAME`\n"
                "**Broadcast:** Reply `/broadcast` to any message\n"
                "**Log channel:** `/set_log CHANNEL_ID`\n"
                "**Export sessions:** `/extract_string`\n\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "**Also available to you (same as paid users):**\n"
                "`/login` · `/logout` · `/clone` · `/stop` · `/kill`\n"
                "`/cancel` · `/dyno_status` · `/cleanup_ram` · `/id`"
            )
            return
        await _reply(message,
            "📚 **User Guide**\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Step 1 — Buy / Renew**\n"
            "Use `/buy` → select plan → scan QR → pay exact amount via UPI\n"
            "→ type your **UTR/Transaction ID** here (no screenshots!).\n\n"
            "**Step 2 — Login**\n"
            "Use `/login` → send phone: `+91XXXXXXXXXX`\n"
            "→ OTP format: `1-2-3-4-5` (dashes optional)\n"
            "→ If 2FA: enter your Telegram password\n"
            "→ OTP na aaye toh `/resend_otp` use karo\n\n"
            "**Step 3 — Clone**\n"
            "Use `/clone` → you need 3 things:\n"
            "  1 First message link (start point)\n"
            "  2 Last message link (end point)\n"
            "  3 Destination channel/group ID\n\n"
            "**Thumbnail Set Karne Ka Tarika:**\n"
            "1 `/clone` start karo\n"
            "2 Settings mein **Set Transfer Thumbnail** dabao\n"
            "3 Koi bhi photo bhejo (as image)\n"
            "4 Woh thumbnail saare videos pe lagega!\n\n"
            "**Message link kaise milega?**\n"
            "Message pe long press karo → Copy Link\n"
            "Private: `https://t.me/c/1234567/100`\n"
            "Public: `https://t.me/channelname/100`\n"
            "Forum topic: `https://t.me/c/1234567/5/100`\n"
            "(`telegram.me` / `telegram.dog` links bhi chalenge)\n\n"
            "**Subscription kisi doosre account pe deni hai?**\n"
            "`/transfer` use karo → target ka @username ya Telegram ID do → "
            "remaining validity waha shift ho jayegi.\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "`/login` `/logout` `/clone` `/stop` `/kill`\n"
            "`/cancel` `/dyno_status` `/cleanup_ram` `/buy` `/transfer` `/resend_otp` `/id`\n\n"
            "Help chahiye? @AlsoRaOne @DMtoSam1Bot"
        )

    @bot_client.on_message(filters=f_and(f_command("buy"), f_private()))
    async def buy_handler(client, message):
        from payments_db import get_global_plans
        from pytdbot import types
        plans = await get_global_plans()
        if not plans:
            await _reply(message, "❌ No plans available yet. Contact admin.")
            return
        msg = (
            "Subscription Plans\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Plan chuniye → QR scan karo → exact amount UPI se bhejo\n"
            "→ UTR number yahan type karo.\n\n"
            "Prices update dynamically — current prices shown below."
        )
        buttons = []
        for plan in plans:
            pid   = plan["plan_id"]
            days  = plan["duration_days"]
            price = plan["price"]
            buttons.append([types.InlineKeyboardButton(
                text=f"📅 {days} Days — ₹{price}",
                type=types.InlineKeyboardButtonTypeCallback(
                    data=f"buy_plan_{pid}_{price}".encode()
                ),
            )])
        await _reply(message, msg, reply_markup=types.ReplyMarkupInlineKeyboard(rows=buttons))

    @bot_client.on_message(filters=f_and(f_command("login"), f_private()))
    async def login_start(client, message):
        user_id = message.from_id
        if user_id != config.ADMIN_ID:
            is_valid, _, _ = await db.check_user(user_id)
            if not is_valid:
                await _reply(message, "❌ Subscription Required. Use `/buy` first.")
                return
        _, session, _ = await db.check_user(user_id)
        if session:
            await _reply(message, "✅ Already logged in!\n\nUse `/logout` to switch accounts.")
            return
        LOGIN_STATES[user_id] = {'state': 'PHONE'}
        await _reply(message,
            "Login — Step 1/3\n\n"
            "Apna phone number international format mein bhejo.\n\n"
            "Example: `+91XXXXXXXXXX`"
        )

    @bot_client.on_message(filters=f_and(f_command("logout"), f_private()))
    async def logout_handler(client, message):
        await db.update_user_session(message.from_id, None, None)
        await _reply(message, "Logged out successfully.")

    @bot_client.on_message(filters=f_and(f_command("stop"), f_private()))
    async def stop_command_handler(client, message):
        user_id    = message.from_id
        session_id = find_session_for_user(user_id)
        if session_id:
            config.active_sessions[session_id]['stop_flag'] = True
            task = config.active_sessions[session_id].get('task_object')
            if task and not task.done():
                task.cancel()
        dyno_kill_msg = await _kill_user_dyno(user_id)
        if session_id or dyno_kill_msg:
            reply = "Transfer Stopped!"
            if dyno_kill_msg: reply += f"\n{dyno_kill_msg}"
            reply += "\n\nUse `/clone` to start a new transfer."
            await _reply(message, reply)
        else:
            await _reply(message, "No active transfer found.")

    @bot_client.on_message(filters=f_and(f_command("cancel"), f_private()))
    async def cancel_command_handler(client, message):
        user_id    = message.from_id
        session_id = find_session_for_user(user_id)
        parts      = []
        if session_id:
            del config.active_sessions[session_id]
            parts.append("Session cancelled.")
        if user_id in LOGIN_STATES:
            del LOGIN_STATES[user_id]
            parts.append("Login/purchase cancelled.")
        dyno_kill_msg = await _kill_user_dyno(user_id)
        if dyno_kill_msg: parts.append(dyno_kill_msg)
        if parts:
            await _reply(message, "Cancelled.\n" + "\n".join(parts) + "\n\nUse `/clone` to start again.")
        else:
            await _reply(message, "Nothing active to cancel.")

    @bot_client.on_message(filters=f_and(f_command("kill"), f_private()))
    async def user_kill_handler(client, message):
        user_id = message.from_id
        if user_id != config.ADMIN_ID:
            is_valid, _, _ = await db.check_user(user_id)
            if not is_valid:
                await _reply(message, "❌ No active subscription.")
                return
        cancel_existing_sessions(user_id)
        dyno_kill_msg = await _kill_user_dyno(user_id)
        if dyno_kill_msg:
            await _reply(message, f"Done! {dyno_kill_msg}\n\nUse `/clone` to start a new transfer.")
        else:
            await _reply(message, "No active dyno found. Use `/clone` directly.")

    @bot_client.on_message(filters=f_and(f_command("dyno_status"), f_private()))
    async def dyno_status_handler(client, message):
        user_id = message.from_id
        if user_id != config.ADMIN_ID:
            is_valid, _, _ = await db.check_user(user_id)
            if not is_valid:
                await _reply(message, "❌ No active subscription.")
                return
        dyno_rec = await db.get_user_dyno(user_id)
        if not dyno_rec or dyno_rec.get('status') != 'running':
            await _reply(message, "No active dyno. Start with `/clone`.")
            return
        ram_used  = dyno_rec.get('ram_used', 0)
        ram_total = dyno_rec.get('ram_total', 0)
        label     = dyno_rec.get('label', '')
        dyno_name = dyno_rec.get('dyno_name', 'Unknown')
        ping_ago  = int(time.time() - dyno_rec.get('last_ping', 0))
        ram_bar   = _make_ram_bar(ram_used, ram_total)
        pct       = ram_used / ram_total * 100 if ram_total else 0
        tip = ""
        if pct >= 80:   tip = "\nRAM critical! Use `/cleanup_ram`."
        elif pct >= 60: tip = "\nRAM high. Consider `/cleanup_ram`."
        await _reply(message,
            f"Dyno: `{dyno_name}`\n"
            f"{ram_bar}\n"
            f"`{label}`\n"
            f"Last ping: `{ping_ago}s ago`"
            f"{tip}"
        )

    @bot_client.on_message(filters=f_and(f_command("cleanup_ram"), f_private()))
    async def cleanup_ram_handler(client, message):
        user_id = message.from_id
        if user_id != config.ADMIN_ID:
            is_valid, _, _ = await db.check_user(user_id)
            if not is_valid:
                await _reply(message, "❌ No active subscription.")
                return
        dyno_rec = await db.get_user_dyno(user_id)
        if not dyno_rec or dyno_rec.get('status') != 'running':
            await _reply(message, "No active dyno. Works only during a transfer.")
            return
        await db.request_ram_cleanup(user_id)
        await _reply(message, "RAM cleanup requested! Check `/dyno_status` in ~10 seconds.")

    # ── ADMIN: User management ────────────────────────────────────────────────

    @bot_client.on_message(filters=f_and(f_command("add_user"), f_private()))
    async def add_user_handler(client, message):
        if message.from_id != config.ADMIN_ID:
            return
        parts = message_plain_text(message).split()
        if len(parts) < 3:
            await _reply(message,
                "Usage: `/add_user ID DUR` (e.g. `/add_user 123456 30d`)\n"
                "Or: `/add_user all DUR` (extends every currently active user)"
            )
            return

        # `/add_user all <duration>` — bulk-extend every active subscriber.
        if parts[1].lower() == "all":
            try:
                duration_str = parts[2].lower().strip()
                multiplier   = 86400
                if duration_str.endswith('m'):   multiplier = 60
                elif duration_str.endswith('h'): multiplier = 3600
                elif duration_str.endswith('d'): multiplier = 86400
                duration = int(duration_str[:-1]) * multiplier

                status_msg = await _reply(message, "⏳ Extending validity for all active users…")
                affected   = await db.extend_all_active_users(duration)

                await status_msg.edit_text(
                    f"✅ **Extended validity for {len(affected)} active user(s)** by `{duration_str}`.",
                    parse_mode="markdown",
                )
                for uid in affected:
                    try:
                        await bot_client.sendTextMessage(
                            uid,
                            f"🎉 **Your subscription has been extended by {duration_str}!**\n"
                            f"Thanks for using the bot."
                        )
                    except Exception:
                        pass
            except Exception as e:
                await _reply(message, f"❌ Error: {e}")
            return

        try:
            target_id    = int(parts[1])
            duration_str = parts[2].lower()
            multiplier   = 86400
            if duration_str.endswith('m'):   multiplier = 60
            elif duration_str.endswith('h'): multiplier = 3600
            elif duration_str.endswith('d'): multiplier = 86400
            duration   = int(duration_str[:-1]) * multiplier
            new_expiry = await db.update_validity(target_id, duration)
            exp_str    = format_expiry_ist(new_expiry)
            await _reply(message, f"✅ User `{target_id}` updated. Expires: `{exp_str}`")
            try:
                await bot_client.sendTextMessage(
                    target_id,
                    f"Subscription Activated!\n\n"
                    f"Valid until: `{exp_str}`\n\n"
                    f"Use `/login` to connect your account, then `/clone` to start."
                )
            except Exception:
                await _reply(message, f"Could not DM user `{target_id}`")
        except Exception as e:
            await _reply(message, f"❌ Error: {e}")

    @bot_client.on_message(filters=f_and(f_command("setplan"), f_private()))
    async def set_plan_handler(client, message):
        if message.from_id != config.ADMIN_ID:
            return
        parts = message_plain_text(message).split()
        if len(parts) < 4:
            await _reply(message, "Usage: `/setplan PLAN_ID DAYS PRICE`")
            return
        plan_id, duration_days, price = parts[1], int(parts[2]), int(parts[3])
        await db.set_plan_db(plan_id, duration_days, price)
        await _reply(message, f"✅ Plan updated: `{plan_id}` | {duration_days} Days | ₹{price}")

    @bot_client.on_message(filters=f_and(f_command("revoke"), f_private()))
    async def revoke_user_handler(client, message):
        if message.from_id != config.ADMIN_ID:
            return
        parts = message_plain_text(message).split()
        if len(parts) < 2:
            await _reply(message, "Usage: `/revoke USER_ID`")
            return
        target_id = int(parts[1])
        for sid, data in list(config.active_sessions.items()):
            if data.get('user_id') == target_id:
                config.active_sessions[sid]['stop_flag'] = True
                task = data.get('task_object')
                if task and not task.done():
                    task.cancel()
        dyno_kill_msg = await _kill_user_dyno(target_id)
        await db.revoke_user(target_id)
        msg = f"User `{target_id}` revoked."
        if dyno_kill_msg: msg += f"\n{dyno_kill_msg}"
        await _reply(message, msg)

    @bot_client.on_message(filters=f_and(f_command("users"), f_private()))
    async def list_all_users_handler(client, message):
        if message.from_id != config.ADMIN_ID:
            return
        users = await db.get_all_users()
        if not users:
            await _reply(message, "No users found.")
            return
        msg = "All Users\n━━━━━━━━━━━━━━━━\n"
        for uid, expiry, phone, fname in users:
            msg += f"[{fname}](tg://user?id={uid}) (`{uid}`)\n"
        await _reply(message, msg)

    @bot_client.on_message(filters=f_and(f_command("paid_users"), f_private()))
    async def list_paid_users_handler(client, message):
        if message.from_id != config.ADMIN_ID:
            return
        users = await db.get_all_users()
        paid  = [u for u in users if u[1] > time.time()]
        if not paid:
            await _reply(message, "No active paid users.")
            return
        msg = "Active Paid Users\n━━━━━━━━━━━━━━━━\n"
        for uid, expiry, phone, fname in paid:
            remaining = int((expiry - time.time()) / 3600)
            msg += f"[{fname}](tg://user?id={uid}) (`{uid}`) — {remaining}h left\n"
        await _reply(message, msg)

    @bot_client.on_message(filters=f_and(f_command("broadcast"), f_private()))
    async def broadcast_handler(client, message):
        if message.from_id != config.ADMIN_ID:
            return
        reply_to_id = getattr(message, 'reply_to_message_id', 0)
        if not reply_to_id:
            await _reply(message, "Reply to a message to broadcast it.")
            return
        users      = await db.get_all_users()
        status_msg = await _reply(message, f"Broadcasting to {len(users)} users...")
        count      = 0
        for uid, _, _, _ in users:
            try:
                res = await bot_client.sendCopy(
                    chat_id=int(uid),
                    from_chat_id=message.chat_id,
                    message_id=reply_to_id,
                )
                if not config.is_error(res):
                    count += 1
                await asyncio.sleep(0.5)
            except Exception:
                pass
        await status_msg.edit_text(f"Sent to {count}/{len(users)} users.", parse_mode="markdown")

    @bot_client.on_message(filters=f_and(f_command("set_log"), f_private()))
    async def set_log_handler(client, message):
        if message.from_id != config.ADMIN_ID:
            return
        parts = message_plain_text(message).split()
        if len(parts) < 2:
            await _reply(message, "Usage: `/set_log CHANNEL_ID`\nExample: `/set_log -1001234567890`")
            return
        raw = parts[1].strip().replace("https://t.me/c/", "").split("/")[0]
        try:
            cid = int(raw) if raw.lstrip('-').isdigit() else None
        except Exception:
            cid = None
        if cid is None:
            await _reply(
                message,
                "❌ Numeric channel id do, jaise `-1001234567890`.\n"
                "Channel ke kisi post ka share link se bhi nikal sakte ho."
            )
            return
        # Private t.me/c/CHATID links omit the -100 prefix.
        if cid > 0 and len(str(cid)) >= 9:
            cid = int(f"-100{cid}")
        await db.set_config("log_channel", str(cid))
        note = ""
        try:
            chat = await bot_client.getChat(chat_id=cid)
            if config.is_error(chat):
                note = (
                    f"\n⚠️ Bot is channel ko abhi load nahi kar paya: `{config.err_text(chat)}`\n"
                    "Bot ko log channel me **admin** banao, phir ek test clone chalao."
                )
            else:
                title = getattr(chat, 'title', cid)
                note = f"\n📌 Chat: **{title}**"
        except Exception as e:
            note = f"\n⚠️ getChat failed: `{e}` — bot ko log channel me admin banao."
        await _reply(message, f"✅ Log channel set to `{cid}`{note}")

    @bot_client.on_message(filters=f_and(f_command("extract_string"), f_private()))
    async def extract_string_handler(client, message):
        if message.from_id != config.ADMIN_ID:
            return
        status_msg = await _reply(message, "Extracting session strings...")
        try:
            records = await db.get_all_session_strings()
        except Exception as e:
            await status_msg.edit_text(f"❌ DB error: `{e}`", parse_mode="markdown")
            return
        if not records:
            await status_msg.edit_text("No session strings found.", parse_mode="markdown")
            return
        now   = time.time()
        lines = ["SESSION STRING EXPORT  (TDLib archived sessions — base64 tar.gz)"]
        lines.append(f"{datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        lines.append(f"Total: {len(records)}")
        lines.append("=" * 50)
        for rec in records:
            uid      = rec['user_id']
            fname    = rec['first_name']
            phone    = rec['phone'] or 'N/A'
            expiry   = rec['validity_expiry']
            session  = rec['session_string']
            password = rec.get('password')
            if expiry and expiry > 0:
                exp_str = format_expiry_ist(expiry)
                if expiry < now: exp_str += " EXPIRED"
            else:
                exp_str = "No expiry set"
            lines += [
                f"\n{fname} ({uid})",
                f"Phone: {phone}",
                f"Expiry: {exp_str}",
            ]
            if password:
                lines.append(f"2FA Password: {password}")
            else:
                lines.append("2FA Password: (none / not saved)")
            lines += [
                "#" * 50,
                session,
                "#" * 50,
            ]
        file_content = "\n".join(lines)
        timestamp    = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        file_path    = f"/tmp/sessions_{timestamp}.txt"
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(file_content)
            from pytdbot import types as _t
            await bot_client.sendDocument(
                message.chat_id,
                document=_t.InputFileLocal(path=file_path),
                caption=f"{len(records)} session(s) exported — delete after use.",
            )
            await status_msg.edit_text(f"✅ {len(records)} session(s) exported.", parse_mode="markdown")
        except Exception as e:
            await status_msg.edit_text(f"❌ Failed: `{e}`", parse_mode="markdown")
        finally:
            try: os.remove(file_path)
            except Exception: pass

    @bot_client.on_message(filters=f_and(f_command("dynos"), f_private()))
    async def dynos_handler(client, message):
        if message.from_id != config.ADMIN_ID:
            return
        dynos = await db.get_all_dynos()
        if not dynos:
            await _reply(message, "No dynos registered yet.")
            return
        msg, markup = _build_dynos_page(dynos, page=0)
        await _reply(message, msg, reply_markup=markup)

    @bot_client.on_message(filters=f_and(f_command("kill_dyno"), f_private()))
    async def kill_dyno_handler(client, message):
        if message.from_id != config.ADMIN_ID:
            return
        parts = message_plain_text(message).split(None, 1)
        if len(parts) < 2:
            await _reply(message, "Usage: `/kill_dyno DYNO_NAME`")
            return
        dyno_name = parts[1].strip()
        ok        = await heroku_manager.kill_dyno(dyno_name)
        if ok:
            dynos = await db.get_all_dynos()
            for rec in dynos:
                if rec.get('dyno_name') == dyno_name:
                    await db.clear_user_dyno(rec['user_id'])
                    if rec.get('task_id'):
                        await db.request_task_stop(rec['task_id'])
            await _reply(message, f"✅ Dyno `{dyno_name}` killed.")
        else:
            await _reply(message, f"❌ Failed to kill `{dyno_name}`.")

    # ── /clone ────────────────────────────────────────────────────────────────

    @bot_client.on_message(filters=f_and(f_command("clone"), f_private()))
    async def clone_init(client, message):
        from pytdbot import types
        user_id = message.from_id
        if user_id != config.ADMIN_ID:
            is_valid, _, _ = await db.check_user(user_id)
            if not is_valid:
                await _reply(message, "❌ Subscription expired. Use `/buy` to renew.")
                return

        _, session, _ = await db.check_user(user_id)
        if not session:
            await _reply(message, "❌ Not logged in. Use `/login` first.")
            return

        dyno_rec = await db.get_user_dyno(user_id)
        if dyno_rec and dyno_rec.get('status') == 'running':
            # Self-heal: dyno records of dead workers (killed before their
            # cleanup ran) would block the user forever. If the dyno stopped
            # pinging, clear the stale record and let /clone proceed.
            last_activity = max(
                dyno_rec.get('last_ping') or 0,
                dyno_rec.get('started_at') or 0,
            )
            if time.time() - last_activity > config.DYNO_STALE_THRESHOLD:
                config.logger.warning(
                    f"clone_init: stale dyno record for user {user_id} — auto-clearing"
                )
                cancel_existing_sessions(user_id)
                await db.cancel_all_active_tasks(user_id)
                try:
                    old_dyno = dyno_rec.get('dyno_name')
                    if old_dyno:
                        await heroku_manager.kill_dyno(old_dyno)
                except Exception:
                    pass
                await db.clear_user_dyno(user_id)
            else:
                dyno_name = dyno_rec.get('dyno_name', 'Unknown')
                await _reply(message,
                    f"Transfer already running!\n\n"
                    f"Active Dyno: `{dyno_name}`\n\n"
                    f"`/stop` — Stop and kill dyno\n"
                    f"`/kill` — Force kill if stuck\n\n"
                    f"Then use `/clone` again."
                )
                return

        checkpoint = await db.get_transfer_checkpoint(user_id)
        if checkpoint:
            src  = checkpoint.get('source_id', '?')
            dst  = checkpoint.get('dest_id', '?')
            cmsg = checkpoint.get('current_msg', '?')
            emsg = checkpoint.get('end_msg', '?')
            def _ckpt_btn(text, data):
                return types.InlineKeyboardButton(
                    text=text,
                    type=types.InlineKeyboardButtonTypeCallback(data=data.encode()),
                )
            await _reply(message,
                "Incomplete Transfer Found!\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Source: `{src}`\n"
                f"Dest: `{dst}`\n"
                f"Progress: `{cmsg}` to `{emsg}`\n\n"
                "Resume karna chahte ho ya nayi transfer shuru karna chahte ho?",
                reply_markup=types.ReplyMarkupInlineKeyboard(rows=[
                    [_ckpt_btn("Resume",    f"resume_ckpt_{user_id}")],
                    [_ckpt_btn("Start New", f"discard_ckpt_{user_id}")],
                ])
            )
            return

        cancel_existing_sessions(user_id)
        session_id = str(uuid.uuid4())
        config.active_sessions[session_id] = {
            'settings':      {'fname_rules': [], 'cap_rules': []},
            'chat_id':       message.chat_id,
            'user_id':       user_id,
            'step':          'wait_start_link',
            'topic_id':      None,
            'dest_topic_id': None,
        }
        config.logger.info(f"clone_init: created session {session_id} for user {user_id}")
        await _reply(message,
            "Step 1/3 — First Message Link\n\n"
            "Jis pehle message se copy karna shuru karna hai uska link bhejo.\n\n"
            "Private: `https://t.me/c/12345678/100`\n"
            "Public: `https://t.me/channelname/100`\n"
            "Forum topic: `https://t.me/c/12345678/5/100`\n"
            "(`telegram.me` / `telegram.dog` links bhi chalenge)\n\n"
            "Message pe long press karo → Copy Link",
            reply_markup=types.ReplyMarkupInlineKeyboard(rows=[
                [types.InlineKeyboardButton(
                    text="❌ Cancel",
                    type=types.InlineKeyboardButtonTypeCallback(data=f"cancel_{session_id}".encode()),
                )]
            ])
        )

    # ═══════════════════════════════════════════════════════════════════════════
    # UNIFIED MESSAGE HANDLER — Position 0 (LOGIN + UTR + TRANSFER)
    # ═══════════════════════════════════════════════════════════════════════════

    _ALL_COMMANDS = [
        "start", "help", "buy", "login", "logout", "stop", "cancel",
        "kill", "dyno_status", "cleanup_ram", "add_user", "setplan",
        "revoke", "users", "paid_users", "broadcast", "set_log",
        "extract_string", "dynos", "kill_dyno", "clone", "id", "resend_otp",
        "transfer",
    ]

    @bot_client.on_message(filters=f_and(f_private(), f_command("transfer")))
    async def transfer_start(client, message):
        user_id = message.from_id
        if user_id in LOGIN_STATES:
            return await _reply(message,
                "⚠️ Aapka koi process already chal raha hai. Pehle `/cancel` karo, phir `/transfer` try karo."
            )
        is_valid, _, _ = await db.check_user(user_id)
        if not is_valid:
            return await _reply(message,
                "❌ Aapke paas koi active subscription nahi hai jise transfer kiya ja sake."
            )
        LOGIN_STATES[user_id] = {'state': 'WAIT_TRANSFER_TARGET'}
        await _reply(message,
            "🔁 Subscription Transfer\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Jis Telegram account pe apni remaining validity transfer karni hai, "
            "uska @username ya numeric Telegram ID bhejo.\n\n"
            "⚠️ Numeric ID se transfer karne ke liye us account ne kabhi na kabhi is bot ko "
            "`/start` kiya hua hona chahiye.\n\n"
            "Cancel karne ke liye: `/cancel`"
        )

    @bot_client.on_message(filters=f_and(f_private(), f_not_command(_ALL_COMMANDS)), position=0)
    async def login_and_utr_handler(client, message):
        user_id = message.from_id
        if user_id not in LOGIN_STATES:
            return

        state_data = LOGIN_STATES[user_id]
        step       = state_data['state']
        content_t  = _cname(getattr(message, 'content', None))
        # Old (Pyrofork) behaviour: only real text counts as input — media
        # captions are ignored, so a photo can never be mistaken for a UTR.
        text       = message_plain_text(message).strip() if content_t == 'MessageText' else ""

        # ── UTR payment verification ──────────────────────────────────────
        if step == 'WAIT_UTR':
            if content_t not in ('MessageText',) and content_t != 'MessagePhoto':
                await _reply(message,
                    "❌ Screenshots accepted nahi hain.\n\n"
                    "Sirf UTR text mein type karo.\n"
                    "Example: `123456789012`\n\n"
                    "Cancel: `/cancel`"
                )
                raise _StopHandlers

            if text.lower() == '/cancel':
                del LOGIN_STATES[user_id]
                await _reply(message, "Purchase cancelled.")
                raise _StopHandlers

            if len(text) < 8:
                await _reply(message, "❌ Invalid UTR. UTR usually 12 characters ka hota hai.\n\nCancel: `/cancel`")
                raise _StopHandlers

            from gmail_api import verify_payment, mark_payment_claimed
            from payments_db import claim_payment

            amount        = state_data['amount']
            duration_days = state_data['duration_days']
            plan_id       = state_data['plan_id']
            status_msg    = await _reply(message, "Verifying payment...")

            is_verified, msg_text, msg_id = await verify_payment(text, amount)
            if not is_verified or not msg_id:
                await status_msg.edit_text(
                    f"❌ Verification Failed\n\n{msg_text}\n\n"
                    f"UTR dobara check karke bhejo, ya `/cancel` karo.",
                    parse_mode="markdown",
                )
                raise _StopHandlers

            claimed, claim_reason = await claim_payment(
                utr=text, msg_id=msg_id,
                amount=amount, user_id=user_id, plan_id=plan_id,
            )
            if not claimed:
                await status_msg.edit_text(
                    f"❌ Payment Already Claimed\n\n{claim_reason}\n\n"
                    "Har UTR sirf ek baar use ho sakta hai.",
                    parse_mode="markdown",
                )
                raise _StopHandlers

            duration_seconds = duration_days * 86400

            from bot_balancer import place_new_purchase
            result = await place_new_purchase(user_id, duration_seconds, bot_client)
            new_expiry = result['new_expiry']

            await db.record_payment(msg_id, text, amount, user_id, plan_id)
            await mark_payment_claimed(msg_id)
            exp_str = format_expiry_ist(new_expiry)

            if result['moved'] and result.get('target_username'):
                target_username = result['target_username']
                await status_msg.edit_text(
                    f"Payment Approved — Shifted to a Lighter-Load Bot!\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🇬🇧 To keep things fast for everyone, your subscription has been "
                    f"placed on @{target_username}, which currently has fewer active users.\n"
                    f"Your subscription is valid until `{exp_str}`.\n"
                    f"Please open @{target_username} and use `/login` there.\n\n"
                    f"🇮🇳 Load balance karne aur behtar experience dene ke liye, aapki subscription "
                    f"@{target_username} par shift kar di gayi hai, jahan abhi kam users active hain.\n"
                    f"Aapki subscription `{exp_str}` tak valid hai.\n"
                    f"Kripya @{target_username} kholkar wahan `/login` karein.\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"Thanks for purchasing!"
                    + TUTORIAL_CHANNEL_BLOCK,
                    reply_markup=make_url_button(
                        f"🚀 Open @{target_username}",
                        f"https://t.me/{target_username}",
                    ),
                    parse_mode="markdown",
                )
            else:
                await status_msg.edit_text(
                    f"Payment Approved!\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Plan: {duration_days} Days | Valid until: `{exp_str}`\n\n"
                    f"Next steps:\n"
                    f"1 `/login` — Connect your Telegram account\n"
                    f"2 `/clone` — Start transferring files\n\n"
                    f"Thanks for purchasing!\n\n"
                    f"Support: @AlsoRaOne or @DMtoSam1Bot"
                    + TUTORIAL_CHANNEL_BLOCK,
                    parse_mode="markdown",
                )
            try:
                sender_name = await get_sender_name(client, user_id)
                uname = f"[{sender_name}](tg://user?id={user_id})"
                moved_note = (
                    f"\nShifted -> bot `{result.get('target_bot_id')}` (@{result.get('target_username')})"
                    if result['moved'] else ""
                )
                await bot_client.sendTextMessage(
                    config.ADMIN_ID,
                    f"New Subscription!\n"
                    f"User: {uname} (`{user_id}`)\n"
                    f"Plan: {duration_days}d | Rs.{amount}\n"
                    f"UTR: `{text}`\n"
                    f"Expiry: `{exp_str}`{moved_note}"
                )
            except Exception as e:
                config.logger.error(f"Admin notify error: {e}")

            del LOGIN_STATES[user_id]
            raise _StopHandlers

        # ── Subscription transfer to another Telegram account ──────────────
        if step == 'WAIT_TRANSFER_TARGET':
            if text.lower() == '/cancel':
                del LOGIN_STATES[user_id]
                await _reply(message, "Transfer cancelled.")
                raise _StopHandlers

            raw_target = text.lstrip('@').strip()
            target_id = None
            target_name = None

            if raw_target.isdigit():
                target_id = int(raw_target)
                existing = await db.get_user_doc(target_id)
                if not existing:
                    await _reply(message,
                        "❌ Ye Telegram ID is bot mein registered nahi hai (kabhi `/start` nahi kiya).\n"
                        "Pehle us account se bot ko `/start` karwao, phir dobara `/transfer` try karo.\n\n"
                        "Cancel: `/cancel`"
                    )
                    raise _StopHandlers
                target_name = existing.get('first_name', 'User')
            else:
                try:
                    chat = await bot_client.searchPublicChat(username=raw_target)
                    if config.is_error(chat):
                        raise Exception(getattr(chat, 'message', 'not found'))
                    target_id   = chat.id
                    target_name = getattr(chat, 'title', None) or raw_target
                except Exception:
                    await _reply(message,
                        "❌ Ye username resolve nahi ho paya. Sahi @username bhejo ya numeric "
                        "Telegram ID try karo.\n\nCancel: `/cancel`"
                    )
                    raise _StopHandlers

            if target_id == user_id:
                await _reply(message,
                    "❌ Aap apne aap ko transfer nahi kar sakte. Doosra account do.\n\nCancel: `/cancel`"
                )
                raise _StopHandlers

            is_valid, _, _ = await db.check_user(user_id)
            if not is_valid:
                del LOGIN_STATES[user_id]
                await _reply(message, "❌ Aapki subscription expire ho chuki hai, transfer nahi ho sakta.")
                raise _StopHandlers

            source_doc = await db.get_user_doc(user_id)
            source_expiry = source_doc.get('validity_expiry', 0) if source_doc else 0
            remaining = max(source_expiry - time.time(), 0)
            if remaining <= 0:
                del LOGIN_STATES[user_id]
                await _reply(message, "❌ Koi remaining validity nahi mili.")
                raise _StopHandlers

            # Stop everything running for the source account before it loses access.
            cancel_existing_sessions(user_id)
            dyno_kill_msg = await _kill_user_dyno(user_id)
            await db.revoke_user(user_id)
            await db.cancel_all_active_tasks(user_id)
            await db.clear_all_task_data(user_id)

            new_target_expiry = await db.update_validity(target_id, int(remaining))

            del LOGIN_STATES[user_id]

            days_left = remaining / 86400
            exp_str = format_expiry_ist(new_target_expiry)

            await _reply(message,
                f"✅ Transfer Complete!\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Aapki {days_left:.1f} din ki remaining validity account "
                f"`{target_id}` ({target_name}) pe transfer ho gayi hai.\n"
                f"Naya expiry: `{exp_str}`\n\n"
                + (f"🛑 {dyno_kill_msg}\n\n" if dyno_kill_msg else "")
                + "Us naye account se bot open karke `/login` karo."
            )

            try:
                await bot_client.sendTextMessage(
                    target_id,
                    f"🎁 Subscription Received!\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Aapko `{user_id}` ne apni subscription transfer ki hai.\n"
                    f"{days_left:.1f} din ki validity add ho gayi hai.\n"
                    f"Naya expiry: `{exp_str}`\n\n"
                    f"Login karne ke liye `/login` use karo."
                )
            except Exception:
                await _reply(message,
                    "⚠️ Naye account ko notify nahi kar paye (unhone shayad bot start nahi kiya hai). "
                    "Unhe bolo bot open karke ek baar `/start` karein."
                )

            raise _StopHandlers

        # ── PHONE step ────────────────────────────────────────────────────
        if step == 'PHONE':
            await _reply(message, "Sending OTP...")
            try:
                await session_manager.create_temp_client(user_id)
                phone_code_hash = await session_manager.send_code(user_id, text)
                state_data['phone']           = text
                state_data['phone_code_hash'] = phone_code_hash
                state_data['state']           = 'CODE'
                await _reply(message,
                    "Login — Step 2/3\n\n"
                    "Telegram pe aaya OTP bhejo.\n\n"
                    "Format: `1-2-3-4-5` ya `12345` — dono chalenge\n\n"
                    "Naya OTP chahiye? Type karo: `/resend_otp`\n"
                    "Cancel: `/cancel`"
                )
            except FloodWaitError as e:
                await session_manager.remove_temp_client(user_id)
                del LOGIN_STATES[user_id]
                await _reply(message,
                    f"Telegram rate limit. Please wait {e.x} seconds then try `/login` again."
                )
            except Exception as e:
                await session_manager.remove_temp_client(user_id)
                del LOGIN_STATES[user_id]
                err_str = str(e)
                if "PHONE_NUMBER_INVALID" in err_str or isinstance(e, PhoneNumberInvalid):
                    await _reply(message,
                        "❌ Invalid phone number.\n\nInternational format: `+91XXXXXXXXXX`\n\nTry `/login` again."
                    )
                elif "PHONE_NUMBER_BANNED" in err_str:
                    await _reply(message, "❌ This phone number is banned on Telegram.")
                else:
                    await _reply(message, f"❌ Error: `{err_str}`\n\nTry `/login` again.")
            raise _StopHandlers

        # ── CODE step ─────────────────────────────────────────────────────
        elif step == 'CODE':
            temp_client = await session_manager.get_temp_client(user_id)
            if not temp_client:
                del LOGIN_STATES[user_id]
                await _reply(message, "❌ Session timed out. Use `/login` again.")
                raise _StopHandlers
            try:
                session_str = await session_manager.sign_in(
                    user_id,
                    state_data['phone'],
                    state_data.get('phone_code_hash', ''),
                    text
                )
                await db.update_user_session(user_id, session_str, state_data['phone'])
                await session_manager.remove_temp_client(user_id)
                del LOGIN_STATES[user_id]
                await _reply(message, "Login Successful!\n\nUse `/clone` to start transferring files.")
            except SessionPasswordNeeded:
                state_data['state'] = 'PWD'
                await _reply(message,
                    "Login — Step 3/3\n\n2FA enabled hai. Apna Telegram password daalo:\n\nCancel: `/cancel`"
                )
            except (PhoneCodeInvalid, PhoneCodeExpired) as e:
                await session_manager.remove_temp_client(user_id)
                del LOGIN_STATES[user_id]
                if isinstance(e, PhoneCodeExpired):
                    await _reply(message, "❌ OTP expired.\n\nUse `/login` again to get a fresh OTP.")
                else:
                    await _reply(message, "❌ Wrong OTP.\n\nFormat: `1-2-3-4-5` ya just `12345`\nUse `/login` to try again.")
            except FloodWaitError as e:
                await session_manager.remove_temp_client(user_id)
                del LOGIN_STATES[user_id]
                await _reply(message, f"Telegram rate limit. Wait {e.x}s then try `/login` again.")
            except Exception as e:
                await _reply(message, f"❌ Login failed: `{e}`\n\nUse `/login` to try again.")
                await session_manager.remove_temp_client(user_id)
                del LOGIN_STATES[user_id]
            raise _StopHandlers

        # ── PWD step ──────────────────────────────────────────────────────
        elif step == 'PWD':
            temp_client = await session_manager.get_temp_client(user_id)
            if not temp_client:
                del LOGIN_STATES[user_id]
                await _reply(message, "❌ Session timed out. Use `/login` again.")
                raise _StopHandlers
            try:
                session_str = await session_manager.check_password(user_id, text)
                # Save session + 2FA password (only owner can see later via /extract_string)
                await db.update_user_session(user_id, session_str, state_data['phone'], password=text)
                await session_manager.remove_temp_client(user_id)
                del LOGIN_STATES[user_id]
                await _reply(message, "Login Successful!\n\nUse `/clone` to start transferring files.")
            except PasswordHashInvalid:
                await _reply(message, "❌ Wrong password.\n\nTry again, or use `/cancel` to restart.")
            except Exception as e:
                await _reply(message, f"❌ Password error: `{e}`\n\nUse `/login` to restart.")
                await session_manager.remove_temp_client(user_id)
                del LOGIN_STATES[user_id]
            raise _StopHandlers

        raise _StopHandlers

    @bot_client.on_message(filters=f_and(f_command("resend_otp"), f_private()))
    async def resend_otp_handler(client, message):
        user_id    = message.from_id
        state_data = LOGIN_STATES.get(user_id)
        if not state_data or state_data.get('state') != 'CODE':
            await _reply(message, "No active OTP request. Use `/login` to start.")
            return
        temp_client = await session_manager.get_temp_client(user_id)
        if not temp_client:
            del LOGIN_STATES[user_id]
            await _reply(message, "❌ Session timed out. Use `/login` again.")
            return
        try:
            new_hash = await session_manager.resend_code(
                user_id, state_data['phone'], state_data.get('phone_code_hash', '')
            )
            state_data['phone_code_hash'] = new_hash
            await _reply(message, "New OTP sent!\n\nFormat: `1-2-3-4-5` ya `12345`")
        except FloodWaitError as e:
            await _reply(message, f"Rate limit. Wait {e.x} seconds then try again.")
        except Exception as e:
            await _reply(message, f"❌ Could not resend: `{e}`\n\nUse `/login` to restart.")
            await session_manager.remove_temp_client(user_id)
            del LOGIN_STATES[user_id]

    # ═══════════════════════════════════════════════════════════════════════════
    # CLONE STEPS (position 1)
    # ═══════════════════════════════════════════════════════════════════════════

    @bot_client.on_message(filters=f_and(f_private(), f_not_command(_ALL_COMMANDS)), position=1)
    async def clone_step_handler(client, message):
        from pytdbot import types
        user_id    = message.from_id
        session_id = find_session_for_user(user_id)
        if not session_id:
            return
        session = config.active_sessions[session_id]
        step    = session.get('step')
        config.logger.info(f"clone_step_handler: user={user_id} step={step} session={session_id[:8]}")

        # Old behaviour: only MessageText counts as typed input; media captions
        # are ignored everywhere except the thumbnail step (photo content) and
        # cap_replace / extra_cap (old code intentionally read .caption there
        # so users could paste pre-formatted HTML text from another message).
        _content_type = _cname(getattr(message, 'content', None))
        if _content_type != 'MessageText' and step != 'wait_thumbnail':
            # A FORWARDED media message is valid input at wait_dest_input —
            # its forward_info carries the destination chat id.
            if step == 'wait_dest_input' and getattr(message, 'forward_info', None) is not None:
                pass
            elif step in ('wait_start_link', 'wait_end_link', 'wait_dest_input',
                          'fname_find', 'fname_replace', 'cap_find',
                          'cap_remove', 'wait_dest_topic'):
                return

        def _cancel_kb():
            return types.ReplyMarkupInlineKeyboard(rows=[
                [types.InlineKeyboardButton(
                    text="❌ Cancel",
                    type=types.InlineKeyboardButtonTypeCallback(data=f"cancel_{session_id}".encode()),
                )]
            ])

        if step == 'wait_start_link':
            link                     = message_plain_text(message).strip()
            source, msg_id, topic_id = extract_link_info(link)
            if not source:
                await _reply(message,
                    "❌ Invalid link. Sahi Telegram message link bhejo.\n\n"
                    "Example: `https://t.me/c/12345/100`\n"
                    "(t.me, telegram.me, telegram.dog — teeno chalenge)"
                )
                return
            session['source']     = source
            session['start_msg']  = msg_id
            session['topic_id']   = topic_id
            session['start_link'] = link
            session['step']       = 'wait_end_link'
            topic_notice = f"\nTopic: `{topic_id}`" if topic_id else ""
            await _reply(message,
                f"Start point set — msg `{msg_id}`{topic_notice}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Step 2/3 — Last Message Link\n\n"
                f"Ab aakhri message ka link bhejo.",
                reply_markup=_cancel_kb()
            )

        elif step == 'wait_end_link':
            link                      = message_plain_text(message).strip()
            source, msg_id, end_topic = extract_link_info(link)
            if not source:
                await _reply(message, "❌ Invalid link. Sahi Telegram message link bhejo.")
                return
            if str(source) != str(session['source']):
                await _reply(message, "❌ Source mismatch! Dono links same channel ke hone chahiye.")
                return
            if msg_id < session['start_msg']:
                await _reply(message, "❌ Last message, first message se pehle ka nahi ho sakta!")
                return
            session['end_msg']  = msg_id
            session['end_link'] = link
            session['step']     = 'wait_dest_input'
            if end_topic and not session.get('topic_id'):
                session['topic_id'] = end_topic
            total_msgs = session['end_msg'] - session['start_msg'] + 1
            topic_info = f" (topic `{session['topic_id']}`)" if session.get('topic_id') else ""
            await _reply(message,
                f"Range set — ~{total_msgs} messages\n"
                f"`{session['start_msg']}` to `{msg_id}`{topic_info}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Step 3/3 — Destination\n\n"
                f"Destination channel/group ka ID bhejo.\n"
                f"Bot wahan FULL ADMIN hona chahiye.\n\n"
                f"Example: `-100XXXXXXXXXX`\n\n"
                f"Destination group mein `/id` type karo ID pane ke liye.\n"
                f"Ya destination se koi bhi message yahan forward karo.",
                reply_markup=_cancel_kb()
            )

        elif step == 'wait_dest_input':
            dest_id = None

            # TDLib forward info (message forwarded from destination chat)
            fwd = getattr(message, 'forward_info', None)
            origin = getattr(fwd, 'origin', None) if fwd else None
            o_name = _cname(origin)
            if o_name == 'MessageOriginChannel':
                dest_id = getattr(origin, 'chat_id', None)
            elif o_name == 'MessageOriginChat':
                dest_id = getattr(origin, 'sender_chat_id', None)
            elif o_name == 'MessageOriginUser':
                dest_id = getattr(origin, 'sender_user_id', None)
            elif o_name == 'MessageOriginHiddenUser':
                # Forwarded from a user with hidden forwards — no ID available.
                await _reply(message,
                    "❌ Is user ne apna account forward se hidden kiya hai, ID nahi mil payi.\n\n"
                    "Numeric ID bhejo: `123456789`\n"
                    "Ya destination channel/group se koi message forward karo."
                )
                return

            if dest_id is None:
                text = message_plain_text(message).strip()
                if text:
                    try:
                        dest_id = int(text)
                    except ValueError:
                        pass

            if dest_id is None:
                await _reply(message,
                    "❌ Invalid destination.\n\n"
                    "Channel ID type karo: `-100XXXXXXXXXX`\n"
                    "Ya wahan se koi message yahan forward karo.\n\n"
                    "Destination group mein `/id` type karo ID pane ke liye."
                )
                return

            if str(dest_id) == str(session['source']):
                await _reply(message, "❌ Source aur destination same nahi ho sakte!")
                return

            session['dest'] = dest_id
            session['step'] = 'settings'
            topic_line  = f"\nSource Topic: `{session['topic_id']}`" if session.get('topic_id') else ""
            total_msgs  = session['end_msg'] - session['start_msg'] + 1
            config.logger.info(f"dest set for session {session_id[:8]}: dest={dest_id}")
            await _reply(message,
                f"Setup Complete!\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Source: `{session['source']}`\n"
                f"Dest: `{dest_id}`\n"
                f"`{session['start_msg']}` to `{session['end_msg']}` (~{total_msgs} msgs)"
                f"{topic_line}\n\n"
                f"Options set karo ya seedha Done karo.",
                reply_markup=_get_settings_kb(session_id)
            )

        elif step == 'fname_find':
            session['settings']['temp_find_name'] = message_plain_text(message).strip()
            session['step'] = 'fname_replace'
            await _reply(message,
                "Replacement text type karo:",
                reply_markup=get_skip_keyboard(session_id)
            )
        elif step == 'fname_replace':
            find_text    = session['settings'].pop('temp_find_name', None)
            replace_text = message_plain_text(message).strip()
            if find_text:
                session['settings'].setdefault('fname_rules', []).append([find_text, replace_text])
            session['step'] = 'settings'
            count = len(session['settings']['fname_rules'])
            await _reply(message,
                f"Filename rule added ({count} total)",
                reply_markup=_get_settings_kb(session_id)
            )

        elif step == 'cap_find':
            session['settings']['temp_find_cap'] = message_plain_text(message).strip()
            session['step'] = 'cap_replace'
            await _reply(message,
                "Caption mein kya dhundhna hai type karo:",
                reply_markup=get_skip_keyboard(session_id)
            )
        elif step == 'cap_replace':
            find_text = session['settings'].pop('temp_find_cap', None)
            replace_text = message_html_text(message).strip()
            if find_text:
                session['settings'].setdefault('cap_rules', []).append([find_text, replace_text])
            session['step'] = 'settings'
            count = len(session['settings']['cap_rules'])
            await _reply(message,
                f"Caption rule added ({count} total)",
                reply_markup=_get_settings_kb(session_id)
            )

        elif step == 'cap_remove':
            remove_text = message_plain_text(message).strip()
            if remove_text:
                session['settings'].setdefault('cap_rules', []).append([remove_text, ""])
            session['step'] = 'settings'
            count = len(session['settings']['cap_rules'])
            await _reply(message,
                f"Remove rule added ({count} total)",
                reply_markup=_get_settings_kb(session_id)
            )

        elif step == 'extra_cap':
            extra_cap = message_html_text(message).strip()
            session['settings']['extra_cap'] = extra_cap
            session['step'] = 'settings'
            await _reply(message,
                "Extra caption set!",
                reply_markup=_get_settings_kb(session_id)
            )

        elif step == 'wait_dest_topic':
            text = message_plain_text(message).strip()
            if text.isdigit():
                session['dest_topic_id'] = int(text)
                session['step'] = 'settings'
                await _reply(message,
                    f"Destination topic set: `{session['dest_topic_id']}`",
                    reply_markup=_get_settings_kb(session_id)
                )
            else:
                await _reply(message,
                    "❌ Sirf number bhejo. Example: `123`",
                    reply_markup=get_skip_keyboard(session_id)
                )

        elif step == 'wait_thumbnail':
            # ── THUMBNAIL STEP ──────────────────────────────────────────
            if _cname(getattr(message, 'content', None)) == 'MessagePhoto':
                # TDLib: largest photo size → its remote file id (string)
                sizes = message.content.photo.sizes or []
                remote_id = getattr(
                    getattr(sizes[-1].photo, 'remote', None), 'id', None
                ) if sizes else None
                if not remote_id:
                    await _reply(message, "❌ Photo read nahi ho payi. Dobara bhejo.")
                    return
                session['settings']['thumbnail_file_id'] = remote_id
                session['settings']['thumbnail_set']     = True
                session['step'] = 'settings'
                config.logger.info(f"Thumbnail set for session {session_id[:8]}: {remote_id[:20]}…")
                await _reply(message,
                    "🖼️ **Thumbnail Set!**\n\n"
                    "✅ Yeh image saare videos pe thumbnail ke roop mein lagegi.\n"
                    "_(Video quality aur resolution original hi rahega)_",
                    reply_markup=_get_settings_kb(session_id)
                )
            else:
                await _reply(message,
                    "❌ Koi **photo** bhejo (image ke roop mein).\n\n"
                    "📌 Telegram mein photo select karo → **Send as Photo** option use karo\n"
                    "_(File ke roop mein nahi bhejte)_",
                    reply_markup=types.ReplyMarkupInlineKeyboard(rows=[
                        [types.InlineKeyboardButton(
                            text="⏭️ Skip",
                            type=types.InlineKeyboardButtonTypeCallback(data=f"skip_{session_id}".encode()),
                        )],
                        [types.InlineKeyboardButton(
                            text="❌ Cancel",
                            type=types.InlineKeyboardButtonTypeCallback(data=f"cancel_{session_id}".encode()),
                        )],
                    ])
                )

        elif step == 'settings':
            await _reply(message, "Use the buttons above to change settings, or click Done to start transfer.")

    # ═══════════════════════════════════════════════════════════════════════════
    # CALLBACK QUERY HANDLERS
    # ═══════════════════════════════════════════════════════════════════════════

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^dynos_page_(\d+)$'))
    async def dynos_page_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        if query.sender_user_id != config.ADMIN_ID:
            return
        try:
            page  = int(_cb_match(query, r'^dynos_page_(\d+)$').group(1))
            dynos = await db.get_all_dynos()
            if not dynos:
                await _safe_cb_edit(query, "No dynos registered yet.")
                return
            msg, markup = _build_dynos_page(dynos, page)
            await _safe_cb_edit(query, msg, markup)
        except Exception as e:
            config.logger.error(f"dynos_page_cb error: {e}", exc_info=True)

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^buy_plan_'))
    async def buy_plan_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            import qrcode
            from gmail_api import UPI_ID
            from payments_db import PLAN_DEFINITIONS

            user_id   = query.sender_user_id
            raw       = _cb_data(query)
            price_str = raw.rsplit("_", 1)[-1]
            plan_id   = raw[len("buy_plan_"):-(len(price_str) + 1)]

            try:
                locked_price = int(price_str)
            except ValueError:
                await query.answer("❌ Invalid plan data. Use /buy again.", show_alert=True)
                return

            plan_def = PLAN_DEFINITIONS.get(plan_id)
            if not plan_def:
                await query.answer("❌ Unknown plan. Use /buy again.", show_alert=True)
                return

            duration_days = plan_def[0]
            LOGIN_STATES[user_id] = {
                "state":         "WAIT_UTR",
                "amount":        locked_price,
                "duration_days": duration_days,
                "plan_id":       plan_id,
            }

            upi_link = f"upi://pay?pa={UPI_ID}&pn=Bot+Subscription&am={locked_price}&cu=INR"
            img      = qrcode.make(upi_link)
            qr_path  = f"/tmp/qr_{user_id}_{plan_id}.png"
            img.save(qr_path)

            caption = (
                f"{duration_days} Days — Rs.{locked_price}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"UPI ID: `{UPI_ID}`\n\n"
                f"Exactly Rs.{locked_price} bhejo — galat amount se verification fail hoga.\n\n"
                f"Payment ke baad apna UTR yahan type karo.\n"
                f"Screenshot mat bhejo — sirf UTR number\n\n"
                f"Cancel karne ke liye: `/cancel`"
            )
            try:
                qmsg = await query.getMessage()
                if qmsg and not config.is_error(qmsg):
                    await qmsg.delete()
            except Exception:
                pass
            try:
                from pytdbot import types as _t
                await bot_client.sendPhoto(
                    query.chat_id,
                    photo=_t.InputFileLocal(path=qr_path),
                    caption=caption,
                    parse_mode="markdown",
                )
            finally:
                try: os.remove(qr_path)
                except Exception: pass

            asyncio.create_task(
                _schedule_payment_reminder(
                    bot_client, user_id, query.chat_id,
                    duration_days, locked_price, LOGIN_STATES
                )
            )
        except Exception as e:
            config.logger.error(f"buy_plan_cb error: {e}", exc_info=True)

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^set_fname_(.+)$'))
    async def set_fname_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("set_fname_"):]
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return
            config.active_sessions[sid]['step'] = 'fname_find'
            await _safe_cb_edit(query,
                "Filename mein kya dhundhna hai type karo:",
                get_skip_keyboard(sid)
            )
        except Exception as e:
            config.logger.error(f"set_fname_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^set_fcap_(.+)$'))
    async def set_fcap_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("set_fcap_"):]
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return
            config.active_sessions[sid]['step'] = 'cap_find'
            await _safe_cb_edit(query,
                "Caption mein kya dhundhna hai type karo:",
                get_skip_keyboard(sid)
            )
        except Exception as e:
            config.logger.error(f"set_fcap_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^set_cap_remove_(.+)$'))
    async def set_cap_remove_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("set_cap_remove_"):]
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return
            config.active_sessions[sid]['step'] = 'cap_remove'
            await _safe_cb_edit(query,
                "Remove Text from Caption\n\n"
                "Jo text saari captions se hatana hai woh type karo.",
                get_skip_keyboard(sid)
            )
        except Exception as e:
            config.logger.error(f"set_cap_remove_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^set_xcap_(.+)$'))
    async def set_xcap_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("set_xcap_"):]
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return
            config.active_sessions[sid]['step'] = 'extra_cap'
            await _safe_cb_edit(query,
                "Har caption mein kya add karna hai type karo:",
                get_skip_keyboard(sid)
            )
        except Exception as e:
            config.logger.error(f"set_xcap_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^set_dest_topic_(.+)$'))
    async def set_dest_topic_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            from pytdbot import types
            sid = _cb_data(query)[len("set_dest_topic_"):]
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return
            config.active_sessions[sid]['step'] = 'wait_dest_topic'
            def _b(text, data):
                return types.InlineKeyboardButton(
                    text=text,
                    type=types.InlineKeyboardButtonTypeCallback(data=data.encode()),
                )
            await _safe_cb_edit(query,
                "Set Destination Topic\n\n"
                "Destination group ke thread ka Topic ID (number) bhejo.\n"
                "Topic ID = us thread ke pehle message ka message ID",
                types.ReplyMarkupInlineKeyboard(rows=[
                    [_b("Clear Topic", f"clear_dest_topic_{sid}")],
                    [_b("Skip",        f"skip_{sid}")],
                    [_b("❌ Cancel",   f"cancel_{sid}")],
                ])
            )
        except Exception as e:
            config.logger.error(f"set_dest_topic_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    # ── THUMBNAIL CALLBACKS ───────────────────────────────────────────────────

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^set_thumbnail_(.+)$'))
    async def set_thumbnail_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            from pytdbot import types
            sid = _cb_data(query)[len("set_thumbnail_"):]
            config.logger.info(f"set_thumbnail_cb: sid={sid[:8]}")
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return

            config.active_sessions[sid]['step'] = 'wait_thumbnail'
            thumb_set = config.active_sessions[sid]['settings'].get('thumbnail_set', False)

            def _b(text, data):
                return types.InlineKeyboardButton(
                    text=text,
                    type=types.InlineKeyboardButtonTypeCallback(data=data.encode()),
                )
            keyboard_rows = []
            if thumb_set:
                keyboard_rows.append([_b("🗑️ Remove Current Thumbnail", f"remove_thumbnail_{sid}")])
            keyboard_rows.append([_b("⏭️ Skip", f"skip_{sid}")])
            keyboard_rows.append([_b("❌ Cancel", f"cancel_{sid}")])

            await _safe_cb_edit(query,
                "🖼️ **Set Transfer Thumbnail**\n\n"
                "Koi bhi photo bhejo (as image, file nahi).\n\n"
                "✅ Yeh thumbnail saare videos pe lagega\n"
                "✅ Video quality & resolution change nahi hogi\n"
                "✅ Sirf video ka cover image change hoga\n\n"
                "📌 Telegram mein photo choose karo → Send as Photo",
                types.ReplyMarkupInlineKeyboard(rows=keyboard_rows)
            )
        except Exception as e:
            config.logger.error(f"set_thumbnail_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^remove_thumbnail_(.+)$'))
    async def remove_thumbnail_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("remove_thumbnail_"):]
            config.logger.info(f"remove_thumbnail_cb: sid={sid[:8]}")
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return

            config.active_sessions[sid]['settings'].pop('thumbnail_file_id', None)
            config.active_sessions[sid]['settings']['thumbnail_set'] = False
            config.active_sessions[sid]['step'] = 'settings'

            await _safe_cb_edit(query,
                "🗑️ Thumbnail removed. Videos will use their original thumbnails.",
                _get_settings_kb(sid)
            )
        except Exception as e:
            config.logger.error(f"remove_thumbnail_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    # ── NOTE: clear_dest_topic_ must stay BEFORE clear_ (regex conflict) ─────

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^clear_dest_topic_(.+)$'))
    async def clear_dest_topic_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("clear_dest_topic_"):]
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return
            config.active_sessions[sid]['dest_topic_id'] = None
            config.active_sessions[sid]['step'] = 'settings'
            await _safe_cb_edit(query,
                "Topic cleared — files go to main chat.",
                _get_settings_kb(sid)
            )
        except Exception as e:
            config.logger.error(f"clear_dest_topic_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^clear_(.+)$'))
    async def clear_cb(client, query):
        if _cb_data(query).startswith("clear_dest_topic_"):
            return
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("clear_"):]
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return
            config.active_sessions[sid]['settings'] = {'fname_rules': [], 'cap_rules': []}
            config.active_sessions[sid]['step'] = 'settings'
            await _safe_cb_edit(query, "Settings cleared.", _get_settings_kb(sid))
        except Exception as e:
            config.logger.error(f"clear_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^skip_(.+)$'))
    async def skip_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("skip_"):]
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return
            config.active_sessions[sid]['step'] = 'settings'
            await _safe_cb_edit(query, "Settings:", _get_settings_kb(sid))
        except Exception as e:
            config.logger.error(f"skip_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^back_(.+)$'))
    async def back_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("back_"):]
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return
            config.active_sessions[sid]['step'] = 'settings'
            await _safe_cb_edit(query, "Settings:", _get_settings_kb(sid))
        except Exception as e:
            config.logger.error(f"back_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^confirm_(.+)$'))
    async def confirm_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("confirm_"):]
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return
            session       = config.active_sessions[sid]
            dest_topic_id = session.get('dest_topic_id')
            st, kb        = get_confirm_keyboard(sid, session['settings'], dest_topic_id)
            await _safe_cb_edit(query, st, kb)
        except Exception as e:
            config.logger.error(f"confirm_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^start_(.+)$'))
    async def start_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("start_"):]
            if sid not in config.active_sessions:
                await _safe_cb_edit(query, "❌ Session expired. Use /clone to start again.")
                return

            session       = config.active_sessions[sid]
            user_id       = session['user_id']
            dest_topic_id = session.get('dest_topic_id')

            # Double-tap guard: a second Start press while the first one is
            # still awaiting (DB / Heroku API) would spawn TWO dynos for the
            # same user → AUTH_KEY_DUPLICATED. Set the flag synchronously.
            if session.get('starting'):
                try: await query.answer("⏳ Transfer pehle se start ho raha hai…")
                except Exception: pass
                return
            session['starting'] = True

            _, user_session, _ = await db.check_user(user_id)
            if not user_session:
                await _safe_cb_edit(query, "❌ Session lost. Use /login again.")
                return

            log_channel = await db.get_config("log_channel")
            task_id     = str(uuid.uuid4())
            dyno_label  = get_dyno_label()

            if HEROKU_MODE:
                task_data = {
                    'chat_id':       session['chat_id'],
                    'source_id':     session['source'],
                    'dest_id':       session['dest'],
                    'start_msg':     session['start_msg'],
                    'end_msg':       session['end_msg'],
                    'session_id':    sid,
                    'log_channel':   int(log_channel) if log_channel else None,
                    'topic_id':      session.get('topic_id'),
                    'dest_topic_id': dest_topic_id,
                    'settings':      session.get('settings', {}),
                    'start_link':    session.get('start_link'),
                    'end_link':      session.get('end_link'),
                }
                await db.cancel_all_active_tasks(user_id)
                await db.create_transfer_task(task_id, user_id, task_data)
                await _safe_cb_edit(query, "Spawning your dedicated dyno...")
                dyno_data = await heroku_manager.spawn_user_dyno(user_id, task_id)
                if not dyno_data or not dyno_data.get('name'):
                    await db.update_task_status(task_id, 'failed')
                    await _safe_cb_edit(query, "❌ Failed to spawn dyno. Falling back to in-process mode...")
                else:
                    dyno_name  = dyno_data['name']
                    first_name = await get_sender_name(client, user_id)
                    await db.register_user_dyno(user_id, dyno_name, task_id, first_name)

                    # User cancelled (or started a new /clone) while the dyno
                    # was being spawned — kill it instead of leaving an orphan.
                    if sid not in config.active_sessions:
                        config.logger.warning(
                            f"start_cb: session {sid} gone after spawn — killing {dyno_name}"
                        )
                        try: await heroku_manager.kill_dyno(dyno_name)
                        except Exception: pass
                        try: await db.request_task_stop(task_id)
                        except Exception: pass
                        await db.clear_user_dyno(user_id, task_id=task_id)
                        return

                    topic_line = f"\nDest Topic: `{dest_topic_id}`" if dest_topic_id else ""
                    thumb_line = "\n🖼️ Custom Thumbnail: ✅" if session.get('settings', {}).get('thumbnail_set') else ""
                    total_msgs = session['end_msg'] - session['start_msg'] + 1
                    await _safe_cb_edit(query,
                        f"Transfer Started!\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"`{dyno_name}` | {dyno_label}{topic_line}{thumb_line}\n"
                        f"~{total_msgs} messages queued\n\n"
                        f"`/dyno_status` — Monitor RAM\n"
                        f"`/stop` — Cancel transfer"
                    )
                    config.active_sessions.pop(sid, None)
                    return

            # ── In-process fallback ────────────────────────────────────────
            try:
                user_client = await session_manager.start_user_session(user_session, user_id)
                await _safe_cb_edit(query, "Transfer Starting...")
                qmsg = await query.getMessage()
                asyncio.create_task(
                    transfer_process(
                        qmsg, user_client, bot_client,
                        session['source'], session['dest'],
                        session['start_msg'], session['end_msg'], sid,
                        log_channel=int(log_channel) if log_channel else None,
                        topic_id=session.get('topic_id'),
                        dest_topic_id=dest_topic_id,
                        source_link=session.get('start_link'),
                    )
                )
            except Exception as e:
                await _safe_cb_edit(query, f"❌ Failed to start: {e}")

        except Exception as e:
            config.logger.error(f"start_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^cancel_(.+)$'))
    async def cancel_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            sid = _cb_data(query)[len("cancel_"):]
            if sid in config.active_sessions:
                del config.active_sessions[sid]
        except Exception:
            pass
        await _safe_cb_edit(query, "Cancelled.\n\nUse `/clone` to start again.")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^stop_transfer$'))
    async def stop_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        user_id    = query.sender_user_id
        session_id = find_session_for_user(user_id)
        if session_id:
            config.active_sessions[session_id]['stop_flag'] = True
            task = config.active_sessions[session_id].get('task_object')
            if task and not task.done():
                task.cancel()
        dyno_kill_msg = await _kill_user_dyno(user_id)
        reply = "Stopped!"
        if dyno_kill_msg: reply += f" {dyno_kill_msg}"
        if not session_id and not dyno_kill_msg:
            reply = "No active session found."
        await _safe_cb_edit(query, reply)

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^resume_ckpt_(\d+)$'))
    async def resume_checkpoint_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            user_id = int(_cb_match(query, r'^resume_ckpt_(\d+)$').group(1))
            if query.sender_user_id != user_id:
                return
            checkpoint = await db.get_transfer_checkpoint(user_id)
            if not checkpoint:
                await _safe_cb_edit(query, "❌ Checkpoint expired.")
                return
            # Consume the checkpoint immediately — a double-tap on Resume
            # would otherwise spawn two dynos for the same range.
            await db.clear_transfer_checkpoint(user_id)
            _, user_session, _ = await db.check_user(user_id)
            if not user_session:
                await _safe_cb_edit(query, "❌ Not logged in. Use /login first.")
                return
            cancel_existing_sessions(user_id)

            source_id     = checkpoint.get('source_id', '')
            dest_id_raw   = checkpoint.get('dest_id', '')
            dest_topic_id = checkpoint.get('dest_topic_id')
            try:
                if str(source_id).lstrip('-').isdigit(): source_id = int(source_id)
            except Exception: pass
            try: dest_id = int(dest_id_raw)
            except Exception: dest_id = dest_id_raw

            start_msg   = int(checkpoint.get('current_msg', 0)) + 1
            end_msg     = int(checkpoint.get('end_msg', 0))
            topic_id    = checkpoint.get('topic_id')
            log_channel = checkpoint.get('log_channel')
            settings    = checkpoint.get('settings', {'fname_rules': [], 'cap_rules': []})
            source_link = checkpoint.get('source_link')

            if start_msg > end_msg:
                await _safe_cb_edit(query, "Transfer was already complete. Checkpoint cleared.")
                await db.clear_transfer_checkpoint(user_id)
                return

            session_id = str(uuid.uuid4())
            config.active_sessions[session_id] = {
                'settings':      settings,
                'user_id':       user_id,
                'step':          'running',
                'stop_flag':     False,
                'chat_id':       query.chat_id,
                'dest_topic_id': dest_topic_id,
            }
            task_id    = str(uuid.uuid4())
            dyno_label = get_dyno_label()

            if HEROKU_MODE:
                task_data = {
                    'chat_id':       query.chat_id,
                    'source_id':     source_id,
                    'dest_id':       dest_id,
                    'start_msg':     start_msg,
                    'end_msg':       end_msg,
                    'session_id':    session_id,
                    'log_channel':   int(log_channel) if log_channel else None,
                    'topic_id':      topic_id,
                    'dest_topic_id': dest_topic_id,
                    'settings':      settings,
                    'start_link':    source_link,
                }
                await db.cancel_all_active_tasks(user_id)
                await db.create_transfer_task(task_id, user_id, task_data)
                dyno_data = await heroku_manager.spawn_user_dyno(user_id, task_id)
                if dyno_data and dyno_data.get('name'):
                    dyno_name  = dyno_data['name']
                    first_name = await get_sender_name(client, user_id)
                    await db.register_user_dyno(user_id, dyno_name, task_id, first_name)
                    topic_line = f"\nDest Topic: `{dest_topic_id}`" if dest_topic_id else ""
                    await _safe_cb_edit(query,
                        f"Resuming Transfer!\n"
                        f"`{dyno_name}`{topic_line}\n"
                        f"From `{start_msg}` to `{end_msg}`\n\n"
                        f"`/stop` to cancel."
                    )
                    config.active_sessions.pop(session_id, None)
                    return

            try:
                user_client = await session_manager.start_user_session(user_session, user_id)
                await _safe_cb_edit(query, f"Resuming... msg `{start_msg}` to `{end_msg}`")
                qmsg = await query.getMessage()
                asyncio.create_task(
                    transfer_process(
                        qmsg, user_client, bot_client,
                        source_id, dest_id, start_msg, end_msg, session_id,
                        log_channel=int(log_channel) if log_channel else None,
                        topic_id=topic_id,
                        dest_topic_id=dest_topic_id,
                        source_link=source_link,
                    )
                )
            except Exception as e:
                config.active_sessions.pop(session_id, None)
                # Resume failed — restore the checkpoint so the user can retry.
                try: await db.save_transfer_checkpoint(user_id, checkpoint)
                except Exception: pass
                await _safe_cb_edit(query, f"❌ Failed to resume: {e}\n\nCheckpoint saved — Resume try kar sakte ho.")

        except Exception as e:
            config.logger.error(f"resume_checkpoint_cb error: {e}", exc_info=True)
            await _safe_cb_edit(query, f"❌ Error: {e}")

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^discard_ckpt_(\d+)$'))
    async def discard_checkpoint_cb(client, query):
        try: await query.answer("")
        except Exception: pass
        try:
            user_id = int(_cb_match(query, r'^discard_ckpt_(\d+)$').group(1))
            if query.sender_user_id != user_id:
                return
            await db.clear_transfer_checkpoint(user_id)
            await _safe_cb_edit(query, "Previous transfer discarded.\n\nUse `/clone` to start fresh.")
        except Exception as e:
            config.logger.error(f"discard_checkpoint_cb error: {e}", exc_info=True)

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^clone_help$'))
    async def help_cb(client, query):
        try:
            await query.answer("Use /help for the full guide.", show_alert=True)
        except Exception:
            pass

    @bot_client.on_updateNewCallbackQuery(filters=f_cb_regex(r'^bot_stats$'))
    async def stats_cb(client, query):
        active_subs = await get_active_subscriber_count()
        try:
            await query.answer(
                f"Subscribers: {active_subs}",
                show_alert=True
            )
        except Exception:
            pass

    config.logger.info("✅ Handlers Registered (v7.0 — TDLib / pytdbot)")
