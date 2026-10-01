"""
transfer.py  –  Main transfer engine  (v7.0 — TDLib / pytdbot)

Behaviour is 1:1 with the Pyrofork v6.2 engine:
  • All sources (public + private) go through download → upload, so custom
    thumbnails, caption/filename manipulations and logging behave the same
    everywhere.
  • Videos re-upload as STREAMABLE videos (supports_streaming=True), audio as
    playable audio (duration preserved), images as viewable photos,
    animations/GIFs as streamable videos, stickers as real stickers.
  • EVERY file — small or big — goes via the TDLib bot client:
    download to disk (user client) → upload from disk (bot client).
    PTB is used ONLY for progress-bar message edits (separate HTTP
    rate-limit bucket), never for file payloads. > 2 GiB files are split
    into 2 GiB parts.
  • Real-time checkpointing + commit-pointer semantics unchanged — a failed
    message is NEVER skipped; the run stops and auto-resume retries it.

TDLIB MAPPING NOTES:
  • pyrogram get_messages(ids=batch) → client.getMessages(chat_id, ids)
    (TDLib returns a Messages object; deleted entries come back as null).
  • FloodWait exception → types.Error(code=429) → FloodWaitError(x) raised
    here so the exact same backoff logic applies.
  • download_media(msg, file_name=…) → message.download(synchronous=True)
    then move TDLib's cached file onto the requested path (so the re-upload
    carries the manipulated filename).
  • send_video/send_document blocking calls → sendVideo/sendDocument (which
    return a PENDING message immediately) + awaiting the
    updateMessageSendSucceeded bridge (progress.register_pending_send).
  • message_thread_id=dest_topic_id → topic_id=MessageTopicForum(...).
"""

import asyncio
import math
import os
import shutil
import time

import config
from utils import (
    human_readable_size, time_formatter,
    get_target_info, get_media_file_size, get_video_metadata,
    apply_filename_manipulations, apply_caption_manipulations,
    sanitize_filename, is_special_media, is_service_message,
    message_plain_text, message_html_text, _media_file, _cname,
)
from progress import (
    TransferProgress, watch_file, unwatch_file,
    register_pending_send, cancel_pending_send,
)
from keyboards import get_progress_keyboard
from session_manager import session_manager, FloodWaitError
import database as db


# ── CONSTANTS ─────────────────────────────────────────────────────────────────

SPLIT_THRESHOLD     = config.SPLIT_FILE_THRESHOLD   # exactly 2 GiB
GET_MESSAGES_BATCH  = 100    # TDLib getMessages() practical ceiling per call


# ── SMALL HELPERS ─────────────────────────────────────────────────────────────

def _ram_bar(ram_used: int, ram_total: int) -> str:
    if not ram_total:
        return ""
    pct      = ram_used / ram_total * 100
    used_mb  = ram_used  / (1024 * 1024)
    total_mb = ram_total / (1024 * 1024)
    filled   = min(10, int(pct / 10))
    bar      = "█" * filled + "░" * (10 - filled)
    status   = "✅ Safe" if pct < 60 else ("⚠️ High" if pct < 80 else "🔴 Critical")
    return (
        f"🧠 RAM: **{bar} {pct:.1f}%**\n"
        f"      `{used_mb:.0f}MB / {total_mb:.0f}MB` {status}\n"
    )


async def safe_edit_message(message, text: str, reply_markup=None):
    """Fire-and-forget message edit. Never raises."""
    async def _edit():
        try:
            if reply_markup is not None:
                await message.edit_text(text, reply_markup=reply_markup, parse_mode="markdown")
            else:
                await message.edit_text(text, parse_mode="markdown")
        except Exception:
            pass
    asyncio.create_task(_edit())


# ── TDLIB MESSAGE-ID ENCODING ────────────────────────────────────────────────
# TDLib encodes server (MTProto / t.me link) message ids by left-shifting 20
# bits: td_id = server_id << 20 (see MessageId::SERVER_ID_SHIFT in TDLib).
# Raw link ids (e.g. 1232) are NOT valid TDLib message ids — passing them to
# getMessages() fails with "400 Invalid message identifier" because the low 20
# bits must be zero for server messages. All link-parsed ids in this module
# therefore stay in the SERVER domain (exactly like the old PyroFork flow);
# conversion happens only at TDLib call boundaries, and every yielded message
# has its .id normalized back to the server domain.
# NOTE: forum topic ids (MessageTopicForum.forum_topic_id) are NOT shifted —
# TDLib returns them raw (verified live: topic 1228 stays 1228).
_TD_ID_SHIFT = 20

def _td_msg_id(server_id: int) -> int:
    """Server (t.me link) message id → TDLib message id."""
    return int(server_id) << _TD_ID_SHIFT

def _srv_msg_id(td_id: int) -> int:
    """TDLib message id → server (t.me link) message id."""
    return int(td_id) >> _TD_ID_SHIFT


def _is_in_topic(msg, topic_id: int) -> bool:
    """Return True if a TDLib message belongs to the given forum topic.
    Expects msg.id already normalized to the server domain."""
    if msg.id == topic_id:
        return True
    t = getattr(msg, 'topic_id', None)
    if _cname(t) == 'MessageTopicForum' and getattr(t, 'forum_topic_id', None) == topic_id:
        return True
    return False


def _topic_kw(dest_topic_id):
    """pytdbot topic kwarg for send helpers."""
    if not dest_topic_id:
        return {}
    from pytdbot import types
    return {'topic_id': types.MessageTopicForum(forum_topic_id=int(dest_topic_id))}


def _named_temp_path(user_id, msg_id, filename: str) -> str:
    """
    TDLib InputFileLocal has NO filename field — Telegram names the uploaded
    document after the local basename. So the original (manipulated) name
    MUST be the last path component, otherwise dest/logs show `tf_123_456_…`.
    """
    safe = sanitize_filename(filename or "file")
    safe = os.path.basename(safe).strip() or "file"
    if safe in ('.', '..'):
        safe = "file"
    root, ext = os.path.splitext(safe)
    if len(safe) > 180:
        safe = (root[: max(1, 180 - len(ext))] + ext) or "file"
    workdir = f"/tmp/tf_{user_id}_{msg_id}_{int(time.time())}"
    os.makedirs(workdir, exist_ok=True)
    return os.path.join(workdir, safe)


def _cleanup_temp(path):
    """Remove a temp file and its unique workdir if we created one."""
    if not path:
        return
    try:
        path = str(path)
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
            return
        if os.path.exists(path):
            os.remove(path)
        parent = os.path.dirname(path)
        if (
            parent.startswith('/tmp/tf_')
            and os.path.isdir(parent)
            and not os.listdir(parent)
        ):
            try:
                os.rmdir(parent)
            except Exception:
                pass
    except Exception:
        pass


def _as_sent_message(sent):
    if not sent or isinstance(sent, bool):
        return None
    if getattr(sent, 'id', None) is not None:
        return sent
    for m in (getattr(sent, 'messages', None) or []):
        if m is not None:
            return m
    return None


def _ptb_message_id(msg) -> int | None:
    raw = getattr(msg, 'id', None) if not isinstance(msg, int) else msg
    if not raw:
        return None
    raw = int(raw)
    if raw > 0 and (raw & ((1 << 20) - 1)) == 0:
        return raw >> 20
    return raw


def _remote_id_from_sent(sent_msg):
    c = getattr(sent_msg, 'content', None)
    name = _cname(c)
    try:
        f = None
        if name == 'MessageVideo':
            f = c.video.video
        elif name == 'MessageAudio':
            f = c.audio.audio
        elif name == 'MessageDocument':
            f = c.document.document
        elif name == 'MessageAnimation':
            f = c.animation.animation
        elif name == 'MessagePhoto':
            sizes = c.photo.sizes or []
            f = sizes[-1].photo if sizes else None
        elif name == 'MessageSticker':
            f = c.sticker.sticker
        if f is not None:
            return getattr(getattr(f, 'remote', None), 'id', None)
    except Exception:
        return None
    return None


def _raise_if_error(res, context: str = ""):
    """Convert a TDLib Error into the legacy exception vocabulary."""
    if not config.is_error(res):
        return
    retry = config.get_retry_after(res)
    if retry:
        raise FloodWaitError(retry)
    msg = (getattr(res, 'message', '') or '')
    up  = msg.upper()
    if 'CHAT_ADMIN_REQUIRED' in up or 'ADMINISTRATOR' in up and 'RIGHT' in up:
        raise PermissionError(
            "❌ **Bot lacks permission in destination channel.**\n"
            "Ensure the bot is **Full Admin** there.\n`CHAT_ADMIN_REQUIRED`"
        )
    if 'CHAT_WRITE_FORBIDDEN' in up or "NOT ENOUGH RIGHTS" in up or 'NOT_A_MEMBER' in up:
        raise PermissionError(
            f"❌ **Bot cannot post in destination channel.**\n`{msg}`"
        )
    if config.is_auth_error(res):
        from session_manager import SessionExpiredError
        raise SessionExpiredError(msg)
    raise Exception(f"TDLib error {getattr(res, 'code', '')}: {msg} {context}".strip())


# ── CHAT RESOLUTION ───────────────────────────────────────────────────────────
#
# ⚠️ TDLib PITFALL (the "Could not access the source channel/group" bug):
# getChat() is, for user accounts, an OFFLINE lookup — it only knows chats the
# local TDLib instance has already seen. Our transfer sessions are restored
# fresh from a tiny archived db (use_chat_info_database=False), so TDLib knows
# ZERO chats and getChat(-100…) fails with "400: Chat not found" even when the
# logged-in account IS a member of the chat (TDLib issue #2344).
#
# Fix ladder implemented by resolve_source_chat():
#   1. getChat()                      — fast path (warm cache)
#   2. getMessageLinkInfo(link)       — the SAME call official apps make when a
#      message link is tapped; resolves t.me/c/… links SERVER-side and loads
#      the chat into TDLib as a side effect. Works for private chats the local
#      instance has never seen.
#   3. getChats() with growing limits — makes TDLib stream the account's real
#      chat list from the server into the local cache, then retry getChat().

async def _try_get_chat(client, chat_id: int):
    """getChat() that returns the Chat on success, None otherwise."""
    try:
        chat = await client.getChat(chat_id=chat_id)
        if not config.is_error(chat):
            return chat
    except Exception:
        pass
    return None


async def _resolve_via_link(client, link: str):
    """
    Resolve a t.me message link through TDLib itself (server-side).
    Returns (chat_id, forum_topic_id) — either may be None.
    """
    if not link:
        return None, None
    try:
        info = await client.getMessageLinkInfo(url=link.strip())
    except Exception as e:
        config.logger.warning(f"getMessageLinkInfo({link!r}) failed: {e}")
        return None, None
    if config.is_error(info):
        config.logger.warning(
            f"getMessageLinkInfo({link!r}) → TDLib error: {config.err_text(info)}"
        )
        return None, None
    chat_id  = getattr(info, 'chat_id', 0) or None
    topic    = getattr(info, 'topic_id', None)
    topic_id = None
    if _cname(topic) == 'MessageTopicForum':
        topic_id = getattr(topic, 'forum_topic_id', None)
    return chat_id, topic_id


async def _load_chat_list(client, max_chats: int = 8000) -> None:
    """
    Stream the account's main (and archive) chat lists into TDLib's in-memory
    cache so getChat() can find them. Doubles the limit each round until the
    list is exhausted or max_chats is reached.
    """
    from pytdbot import types
    for chat_list in (types.ChatListMain(), types.ChatListArchive()):
        limit = 200
        while limit <= max_chats:
            try:
                res = await client.getChats(chat_list=chat_list, limit=limit)
                if config.is_error(res):
                    break
                ids = getattr(res, 'chat_ids', None) or []
                if len(ids) < limit:
                    break                       # list exhausted
            except Exception:
                break
            limit *= 2
            await asyncio.sleep(0.3)


async def resolve_source_chat(client, source, link: str = None):
    """
    Robustly resolve a source identifier (numeric -100… id or @username) to a
    TDLib Chat object — works even on a FRESH session that has never seen the
    chat. Raises the legacy-friendly exception if genuinely inaccessible.
    """
    # ── Public username ───────────────────────────────────────────────────
    if isinstance(source, str) and not source.lstrip('-').isdigit():
        username = source.lstrip('@')
        chat = await client.searchPublicChat(username=username)
        _raise_if_error(chat, f"(resolve @{username})")
        return chat

    chat_id = int(source)

    # ── 1. Warm cache fast path ───────────────────────────────────────────
    chat = await _try_get_chat(client, chat_id)
    if chat:
        return chat
    config.logger.info(f"resolve: getChat({chat_id}) cold — trying link resolution…")

    # ── 2. Server-side resolution through the original message link ───────
    link_chat, link_topic = await _resolve_via_link(client, link)
    if link_topic:
        config.logger.info(f"resolve: link confirms forum topic {link_topic}")
    if link_chat:
        if link_chat != chat_id:
            # e.g. a channel COMMENT link — the message actually lives in the
            # linked discussion group. Trust TDLib's chat_id.
            config.logger.info(
                f"resolve: link points to chat {link_chat} (parsed {chat_id}) — using TDLib's"
            )
        chat = await _try_get_chat(client, link_chat)
        if chat:
            config.logger.info(f"✅ resolve: link resolution OK ({getattr(chat, 'title', chat.id)!r})")
            return chat
        # getMessageLinkInfo already proved server-side access and loaded the
        # chat — even if getChat still hiccups, the id itself is usable for
        # getMessages(); degrade to a minimal stand-in instead of failing.
        from types import SimpleNamespace
        config.logger.info("✅ resolve: using link-resolved chat id directly")
        return SimpleNamespace(id=link_chat, title=str(link_chat))

    # ── 3. Pull the account's chat list into cache, then retry ────────────
    config.logger.info("resolve: loading account chat list from server…")
    await _load_chat_list(client)
    chat = await _try_get_chat(client, chat_id)
    if chat:
        config.logger.info(f"✅ resolve: found after chat-list load ({getattr(chat, 'title', chat.id)!r})")
        return chat

    # ── Genuinely inaccessible — surface TDLib's own error text ───────────
    final = await client.getChat(chat_id=chat_id)
    _raise_if_error(final, f"(resolve {source})")
    return final


# ── PREFLIGHT CHECK ───────────────────────────────────────────────────────────

async def _preflight_check(bot_client, dest_id) -> tuple:
    """Resolve the destination chat via TDLib. Returns (ok, error_message)."""
    try:
        chat = await bot_client.getChat(chat_id=dest_id)
        _raise_if_error(chat)
        config.logger.info(f"✅ Preflight OK: dest peer resolved ({dest_id})")
        return True, None
    except FloodWaitError as e:
        config.logger.warning(f"Preflight FloodWait {e.x}s — waiting…")
        await asyncio.sleep(e.x + 2)
        return True, None
    except PermissionError as e:
        return False, str(e)
    except Exception as e:
        config.logger.warning(f"Preflight warning (non-fatal): {type(e).__name__}: {e}")
        return True, None


# ── ROBUST MESSAGE ITERATION (TDLib getMessages / getForumTopicHistory) ───────

async def _fetch_topic_page(user_client, chat_id, topic_id, anchor_td):
    """
    Fetch one forward page of a forum topic via getForumTopicHistory.
    anchor_td is a TDLib-domain message id (server_id << 20). Handles
    FloodWait, transient errors, and deleted-anchor walk-back (up to 100
    server ids). Returns the raw TDLib result.
    """
    MAX_CONN_RETRIES = 5
    conn_retries = 0
    walkbacks    = 0
    while True:
        try:
            res = await user_client.getForumTopicHistory(
                chat_id=chat_id,
                forum_topic_id=int(topic_id),
                from_message_id=anchor_td,
                offset=-99,          # anchor + up to 99 NEWER messages
                limit=100,
            )
            if config.is_error(res):
                retry = config.get_retry_after(res)
                if retry:
                    raise FloodWaitError(retry)
                emsg = (getattr(res, 'message', '') or '')
                eup  = emsg.upper()
                if 'MESSAGE' in eup and ('NOT FOUND' in eup or 'INVALID' in eup):
                    # Anchor message was deleted — step back one server id
                    if walkbacks < 100:
                        walkbacks += 1
                        anchor_td -= (1 << _TD_ID_SHIFT)
                        continue
                    raise Exception(
                        "Couldn't locate any message near the start of your "
                        "range in this topic — the first ~100 messages of the "
                        "range may be deleted. Try a later start link."
                    )
                _raise_if_error(res, "(getForumTopicHistory)")
            return res
        except FloodWaitError as e:
            wait = e.x + 2
            config.logger.warning(f"FloodWait {e.x}s during topic scan — waiting {wait}s")
            await asyncio.sleep(wait)
        except Exception as e:
            from session_manager import SessionExpiredError
            if isinstance(e, SessionExpiredError):
                raise Exception(
                    "⚠️ User session expired during transfer.\n"
                    "Please use /login to reconnect and try again."
                )
            if "Couldn't locate" in str(e):
                raise
            conn_retries += 1
            config.logger.error(f"getForumTopicHistory error (attempt {conn_retries}): {e}")
            if conn_retries > MAX_CONN_RETRIES:
                raise Exception(f"Too many errors fetching topic messages: {e}")
            await asyncio.sleep(5 * conn_retries)


async def robust_iter_messages(user_client, source_id, start_msg: int,
                                end_msg: int, topic_id=None, source_link=None):
    """
    Async generator — yields pytdbot Messages in [start_msg, end_msg].

    start_msg/end_msg are SERVER-domain ids (as parsed from t.me links — same
    semantics as the old PyroFork flow). TDLib ids (server_id << 20) are used
    only at TDLib call boundaries; every yielded message has .id normalized
    back to the server domain so ranges, checkpoints, topic filters and
    user-facing progress behave exactly like the PyroFork version.

    Topic mode walks the topic natively via getForumTopicHistory (robust
    whether TDLib numbers topic messages per-topic or channel-wide, and far
    more efficient than scanning the whole channel). Non-topic mode batches
    getMessages with shifted ids.
    """
    chat      = await resolve_source_chat(user_client, source_id, link=source_link)
    source_id = chat.id

    # ── FORUM TOPIC MODE ──────────────────────────────────────────────────
    if topic_id is not None:
        anchor_td    = _td_msg_id(start_msg)
        last_td      = anchor_td - (1 << _TD_ID_SHIFT)
        zero_misses  = 0
        while True:
            res  = await _fetch_topic_page(user_client, source_id, topic_id, anchor_td)
            msgs = sorted([m for m in (getattr(res, 'messages', None) or []) if m],
                          key=lambda m: m.id)
            new_msgs = [m for m in msgs if m.id > last_td]

            if not new_msgs:
                zero_misses += 1
                if zero_misses >= config.ZERO_PROGRESS_RETRIES:
                    config.logger.warning(
                        f"iter(topic): {config.ZERO_PROGRESS_RETRIES} empty pages "
                        f"at server-id {_srv_msg_id(anchor_td)}. Concluding done."
                    )
                    return
                delay = config.ZERO_PROGRESS_DELAYS[
                    min(zero_misses - 1, len(config.ZERO_PROGRESS_DELAYS) - 1)
                ]
                await asyncio.sleep(delay)
                continue

            zero_misses = 0
            for msg in new_msgs:
                last_td = msg.id
                msg.id  = _srv_msg_id(msg.id)        # → server domain
                if msg.id < start_msg:
                    continue
                if msg.id > end_msg:
                    return
                if not _is_in_topic(msg, topic_id):  # safety net for channel-wide id spaces
                    continue
                yield msg

            anchor_td = last_td
            await asyncio.sleep(1)

    # ── REGULAR CHANNEL/GROUP MODE ────────────────────────────────────────
    current     = start_msg
    zero_misses = 0
    MAX_CONN_RETRIES = 5
    conn_retries     = 0

    while current <= end_msg:
        batch_end = min(current + GET_MESSAGES_BATCH - 1, end_msg)
        ids       = [_td_msg_id(i) for i in range(current, batch_end + 1)]

        try:
            res = await user_client.getMessages(chat_id=source_id, message_ids=ids)
            if config.is_error(res):
                _raise_if_error(res, "(getMessages)")
            conn_retries = 0
        except FloodWaitError as e:
            wait = e.x + 2
            config.logger.warning(f"FloodWait {e.x}s during source scan — waiting {wait}s")
            await asyncio.sleep(wait)
            continue
        except Exception as e:
            from session_manager import SessionExpiredError
            if isinstance(e, SessionExpiredError):
                raise Exception(
                    "⚠️ User session expired during transfer.\n"
                    "Please use /login to reconnect and try again."
                )
            msg = str(e)
            if 'CHAT_NOT_FOUND' in msg.upper() or 'PEER' in msg.upper():
                conn_retries += 1
                config.logger.warning(
                    f"Chat not found for source {source_id} (attempt {conn_retries}) — re-resolving…"
                )
                if conn_retries > MAX_CONN_RETRIES:
                    raise Exception(
                        f"Could not resolve source chat {source_id} after "
                        f"{MAX_CONN_RETRIES} attempts: {e}"
                    )
                try:
                    await resolve_source_chat(user_client, source_id, link=source_link)
                except Exception:
                    pass
                await asyncio.sleep(3 * conn_retries)
                continue
            conn_retries += 1
            config.logger.error(f"getMessages error (attempt {conn_retries}): {e}")
            if conn_retries > MAX_CONN_RETRIES:
                raise Exception(f"Too many errors fetching messages: {e}")
            await asyncio.sleep(5 * conn_retries)
            continue

        # ── Filter and yield valid messages ────────────────────────────────
        valid_in_batch = 0
        raw_msgs       = (getattr(res, 'messages', None) or [])
        sorted_msgs    = sorted([m for m in raw_msgs if m], key=lambda m: m.id)

        for msg in sorted_msgs:
            msg.id = _srv_msg_id(msg.id)             # → server domain
            if msg.id < start_msg or msg.id > end_msg:
                continue
            if topic_id is not None and not _is_in_topic(msg, topic_id):
                continue
            valid_in_batch += 1
            zero_misses     = 0
            yield msg

        # ── Zero-progress protection ───────────────────────────────────────
        if valid_in_batch == 0:
            zero_misses += 1
            if zero_misses >= config.ZERO_PROGRESS_RETRIES:
                config.logger.warning(
                    f"iter: {config.ZERO_PROGRESS_RETRIES} consecutive empty batches "
                    f"at msg {current}. Concluding done."
                )
                return
            delay = config.ZERO_PROGRESS_DELAYS[
                min(zero_misses - 1, len(config.ZERO_PROGRESS_DELAYS) - 1)
            ]
            config.logger.warning(
                f"iter: empty batch at {current}–{batch_end} "
                f"(retry {zero_misses}/{config.ZERO_PROGRESS_RETRIES} in {delay}s…)"
            )
            await asyncio.sleep(delay)
            continue

        current = batch_end + 1
        await asyncio.sleep(1)


# ── TDLIB DOWNLOAD HELPER ─────────────────────────────────────────────────────

async def td_download(client, message, dest_path: str, tracker=None) -> str:
    """
    Download a message's media via TDLib and place it at dest_path.

    Stall-resistant: instead of one blocking synchronous downloadFile call
    (which can hang FOREVER on a TDLib-side stall), we start the download
    asynchronously and poll downloaded_size every few seconds. If no bytes
    arrive for DOWNLOAD_STALL_TIMEOUT seconds, the download is re-triggered
    (TDLib resumes partial downloads). Raises after DOWNLOAD_MAX_ATTEMPTS.
    Returns dest_path.
    """
    from pytdbot import types

    media_file = _media_file(message)
    file_id    = getattr(media_file, 'id', None)
    if file_id is None:
        raise RuntimeError("message has no downloadable file")
    if tracker:
        watch_file(file_id, tracker, 'download')
    try:
        last_err = None
        for attempt in range(1, config.DOWNLOAD_MAX_ATTEMPTS + 1):
            try:
                res = await client.downloadFile(
                    file_id=file_id, priority=32, offset=0, limit=0,
                    synchronous=False,
                )
                _raise_if_error(res, "(downloadFile)")

                last_bytes    = -1
                last_progress = time.time()
                started       = time.time()
                local         = None

                while True:
                    f = await client.getFile(file_id=file_id)
                    _raise_if_error(f, "(getFile)")
                    local = getattr(f, 'local', None)
                    if getattr(local, 'is_downloading_completed', False):
                        break
                    cur = getattr(local, 'downloaded_size', 0) or 0
                    if tracker:
                        total = (
                            getattr(f, 'size', 0)
                            or getattr(f, 'expected_size', 0)
                            or 0
                        )
                        try:
                            await tracker.download_cb(cur, total)
                        except Exception:
                            pass
                    if cur != last_bytes:
                        last_bytes    = cur
                        last_progress = time.time()
                    elif time.time() - last_progress > config.DOWNLOAD_STALL_TIMEOUT:
                        raise TimeoutError(
                            f"download stalled at {cur} bytes for "
                            f"{config.DOWNLOAD_STALL_TIMEOUT}s"
                        )
                    if time.time() - started > 7200:   # absolute cap per attempt
                        raise TimeoutError("download exceeded 2h cap")
                    await asyncio.sleep(5)

                src_path = getattr(local, 'path', None)
                if not src_path or not os.path.exists(src_path):
                    raise RuntimeError("downloadFile completed but no local path")
                os.makedirs(os.path.dirname(dest_path) or '/tmp', exist_ok=True)
                if os.path.abspath(src_path) != os.path.abspath(dest_path):
                    shutil.copyfile(src_path, dest_path)
                if attempt > 1:
                    config.logger.info(f"✅ Download succeeded on attempt {attempt}")
                return dest_path

            except FloodWaitError:
                raise
            except Exception as e:
                last_err = e
                config.logger.warning(
                    f"⚠️ Download attempt {attempt}/{config.DOWNLOAD_MAX_ATTEMPTS} "
                    f"failed: {e} — re-triggering"
                )
                try:
                    await client.cancelDownloadFile(
                        file_id=file_id, only_if_pending=False
                    )
                except Exception:
                    pass
                await asyncio.sleep(min(10 * attempt, 30))
        raise RuntimeError(
            f"download failed after {config.DOWNLOAD_MAX_ATTEMPTS} attempts: {last_err}"
        )
    finally:
        if tracker:
            unwatch_file(file_id, tracker)


async def td_download_remote_id(client, remote_file_id: str, dest_path: str) -> str:
    """Download any file by its TDLib remote file id (thumbnails etc.)."""
    f = await client.getRemoteFile(remote_file_id=remote_file_id)
    _raise_if_error(f, "(getRemoteFile)")
    res = await asyncio.wait_for(
        client.downloadFile(
            file_id=f.id, priority=32, offset=0, limit=0, synchronous=True
        ),
        timeout=120,   # thumbnails are tiny — never let this hang a transfer
    )
    _raise_if_error(res, "(downloadFile)")
    src_path = getattr(getattr(res, 'local', None), 'path', None)
    if not src_path or not os.path.exists(src_path):
        raise RuntimeError("thumbnail download produced no file")
    if os.path.abspath(src_path) != os.path.abspath(dest_path):
        shutil.copyfile(src_path, dest_path)
    return dest_path


# ── LOG TRANSFER ──────────────────────────────────────────────────────────────

async def _warmup_log_channel(bot_client, log_channel):
    """
    Resolve / load the log channel on this TDLib instance.

    Worker dynos start with an empty TDLib chat cache, so forward/copy to a
    never-seen chat_id silently fails. We also re-read Mongo if the task
    payload didn't carry a log_channel (resume / old checkpoints).
    """
    if not log_channel:
        try:
            raw = await db.get_config("log_channel")
        except Exception:
            raw = None
        if not raw:
            config.logger.warning("📭 log_channel not set — use /set_log CHANNEL_ID")
            return None
        log_channel = raw
    try:
        log_id = int(str(log_channel).strip())
    except Exception:
        config.logger.error(f"📭 Invalid log_channel value: {log_channel!r}")
        return None
    try:
        chat = await bot_client.getChat(chat_id=log_id)
        if config.is_error(chat):
            config.logger.warning(
                f"📭 getChat(log {log_id}) failed: {config.err_text(chat)} "
                f"— will still try PTB copy"
            )
        else:
            config.logger.info(
                f"📭 Log channel ready: {getattr(chat, 'title', log_id)!r} ({log_id})"
            )
    except Exception as e:
        config.logger.warning(f"📭 Log channel warmup: {e}")
    return log_id


async def log_transfer(bot_client, log_channel, sent_message,
                        session_id, dest_id, file_name, part_num=None,
                        file_path=None, is_video=False, is_audio=False,
                        caption=None):
    """
    Copy the just-sent dest message into the admin log channel.

    Strategy (first success wins, never raises):
      1. PTB copyMessage / forwardMessage — HTTP Bot API does not depend on
         this dyno's TDLib chat cache, so it works on fresh worker dynos.
      2. TDLib sendCopy (and forwardMessages if the client exposes it).
      3. Reuse the already-uploaded file (InputFileId / InputFileRemote) or
         re-send from disk. Used when dest has restricted forwarding.
    """
    if not log_channel:
        return
    try:
        log_id = int(log_channel)
    except Exception:
        config.logger.error(f"📭 Invalid log_channel: {log_channel!r}")
        return

    sent = _as_sent_message(sent_message)
    tag  = str(file_name or "file")
    if part_num:
        tag = f"{tag} (part {part_num})"

    # ── 1) PTB copy/forward ───────────────────────────────────────────────
    ptb_mid = _ptb_message_id(sent) if sent else None
    if ptb_mid and dest_id:
        try:
            ptb = await config.get_ptb_bot()
            try:
                await ptb.copy_message(
                    chat_id=log_id,
                    from_chat_id=int(dest_id),
                    message_id=ptb_mid,
                )
                config.logger.info(f"📭 Logged (PTB copy): {tag}")
                return
            except Exception as e1:
                config.logger.warning(f"📭 PTB copy failed ({tag}): {e1}")
                try:
                    await ptb.forward_message(
                        chat_id=log_id,
                        from_chat_id=int(dest_id),
                        message_id=ptb_mid,
                    )
                    config.logger.info(f"📭 Logged (PTB forward): {tag}")
                    return
                except Exception as e2:
                    config.logger.warning(f"📭 PTB forward failed ({tag}): {e2}")
        except Exception as e:
            config.logger.warning(f"📭 PTB log path failed ({tag}): {e}")

    # ── 2) TDLib copy ─────────────────────────────────────────────────────
    if sent and getattr(sent, 'id', None) and dest_id:
        try:
            res = await bot_client.sendCopy(
                chat_id=int(log_id),
                from_chat_id=int(dest_id),
                message_id=int(sent.id),
            )
            if not config.is_error(res):
                config.logger.info(f"📭 Logged (TDLib copy): {tag}")
                return
            config.logger.warning(
                f"📭 TDLib sendCopy failed ({tag}): {config.err_text(res)}"
            )
        except Exception as e:
            config.logger.warning(f"📭 TDLib sendCopy error ({tag}): {e}")

        fwd = getattr(bot_client, 'forwardMessages', None)
        if callable(fwd):
            try:
                res = await fwd(
                    chat_id=int(log_id),
                    from_chat_id=int(dest_id),
                    message_ids=[int(sent.id)],
                    send_copy=True,
                )
                if not config.is_error(res):
                    config.logger.info(f"📭 Logged (TDLib forward): {tag}")
                    return
                config.logger.warning(
                    f"📭 TDLib forwardMessages failed ({tag}): {config.err_text(res)}"
                )
            except Exception as e:
                config.logger.warning(f"📭 TDLib forwardMessages error ({tag}): {e}")

    # ── 3) Reuse uploaded file / disk resend ──────────────────────────────
    try:
        from pytdbot import types
        infile = None
        local_fid  = _sent_file_id(sent) if sent else None
        remote_fid = _remote_id_from_sent(sent) if sent else None
        InputFileId = getattr(types, 'InputFileId', None)
        if local_fid is not None and InputFileId is not None:
            try:
                infile = InputFileId(id=int(local_fid))
            except Exception:
                infile = None
        if infile is None and remote_fid:
            infile = types.InputFileRemote(id=str(remote_fid))
        if infile is None and file_path and os.path.exists(str(file_path)):
            infile = types.InputFileLocal(path=str(file_path))
        if infile is None:
            config.logger.error(f"📭 Log skip — no message/file for {tag}")
            return

        cap    = caption or f"📦 {tag}"
        cap_ft = await _parse_caption_html(bot_client, cap)
        # Log archive is always a document so we never depend on video/audio
        # constructor defaults — the original filename is already on disk.
        content = types.InputMessageDocument(
            document=types.InputDocument(
                document=infile,
                disable_content_type_detection=True,
            ),
            caption=cap_ft,
        )
        res = await bot_client.sendMessage(
            chat_id=int(log_id),
            input_message_content=content,
        )
        if not config.is_error(res):
            config.logger.info(f"📭 Logged (file reuse/resend): {tag}")
            return
        config.logger.error(f"📭 Log resend failed ({tag}): {config.err_text(res)}")
    except Exception as e:
        config.logger.error(f"📭 Log error ({tag}): {e}")

# ── BOT_CLIENT DISK UPLOAD (ALL files, any size) via TDLib ───────────────────

async def _parse_caption_html(client, caption: str):
    """HTML caption string → TDLib FormattedText (plain-text fallback)."""
    from pytdbot import types
    caption = caption or ''
    if not caption:
        return types.FormattedText(text='', entities=[])
    try:
        ft = await client.parseText(caption, parse_mode='html')
        if ft is not None and not config.is_error(ft):
            return ft
    except Exception:
        pass
    return types.FormattedText(text=caption, entities=[])


def _sent_file_id(sent_msg):
    """Extract the TDLib file id from a pending sent message (for upload progress)."""
    c = getattr(sent_msg, 'content', None)
    name = _cname(c)
    try:
        if name == 'MessageVideo':
            return c.video.video.id
        if name == 'MessageAudio':
            return c.audio.audio.id
        if name == 'MessageDocument':
            return c.document.document.id
        if name == 'MessagePhoto':
            sizes = c.photo.sizes or []
            return sizes[-1].photo.id if sizes else None
    except Exception:
        pass
    return None


async def _bot_disk_upload(
    bot_client, dest_id: int,
    file_path: str, file_name: str, file_size: int,
    caption: str, is_video: bool, is_audio: bool,
    thumb_path, dest_topic_id,
    progress_tracker: TransferProgress,
    duration: int = 0, width: int = 0, height: int = 0,
) -> tuple:
    """
    Upload a file from disk via the TDLib bot client (MTProto).
    Supports files up to ~2 GiB per call.
    Returns (success: bool, sent_message).

    ⚠️ CRITICAL pytdbot SEMANTICS (the v7.4 "upload stalled" bug):
    pytdbot's sendVideo/sendDocument HELPERS do not return the pending
    message — they internally await updateMessageSendSucceeded
    (sendMessageWithContent → _create_request_future) and only return the
    FINAL message. Registering our bridge future after such a call means
    the completion update has ALREADY fired and been consumed → the future
    never resolves → every upload "stalled" past its timeout, got deleted
    and retried forever. So we call RAW sendMessage() instead: it returns
    the PENDING message instantly, letting us (1) register the completion
    future BEFORE the upload can finish and (2) watch the file's
    remote.uploaded_size for a live upload progress bar.
    """
    from pytdbot import types

    thumb = None
    if thumb_path and os.path.exists(str(thumb_path)):
        thumb = types.InputThumbnail(
            thumbnail=types.InputFileLocal(path=str(thumb_path)),
            width=width or 0,
            height=height or 0,
        )

    topic          = _topic_kw(dest_topic_id)
    upload_timeout = config.get_upload_timeout(file_size)
    caption_ft     = await _parse_caption_html(bot_client, caption)

    def _build_content():
        if is_video:
            return types.InputMessageVideo(
                video=types.InputVideo(
                    video=types.InputFileLocal(path=file_path),
                    thumbnail=thumb,
                    duration=duration or 0,
                    width=width or 0,
                    height=height or 0,
                    supports_streaming=True,
                ),
                caption=caption_ft,
            )
        if is_audio:
            return types.InputMessageAudio(
                audio=types.InputAudio(
                    audio=types.InputFileLocal(path=file_path),
                    album_cover_thumbnail=thumb,
                    duration=duration or 0,
                ),
                caption=caption_ft,
            )
        return types.InputMessageDocument(
            document=types.InputDocument(
                document=types.InputFileLocal(path=file_path),
                thumbnail=thumb,
                disable_content_type_detection=True,
            ),
            caption=caption_ft,
        )

    for attempt in range(config.MAX_RETRIES):
        res  = None
        fut  = None
        fid  = None
        try:
            # RAW sendMessage → returns the PENDING message instantly.
            res = await bot_client.sendMessage(
                chat_id=dest_id,
                input_message_content=_build_content(),
                **topic,
            )

            if config.is_error(res):
                retry = config.get_retry_after(res)
                if retry:
                    raise FloodWaitError(retry)
                _raise_if_error(res, "(send)")

            # Register the completion future BEFORE the upload can finish,
            # then start watching the file's upload progress.
            temp_id = getattr(res, 'id', None)
            fut = register_pending_send(temp_id)

            fid = _sent_file_id(res)
            if fid is not None and progress_tracker:
                watch_file(fid, progress_tracker, 'upload')

            try:
                final_msg = await asyncio.wait_for(fut, timeout=upload_timeout)
            finally:
                if fid is not None and progress_tracker:
                    unwatch_file(fid, progress_tracker)

            if final_msg is None:
                raise RuntimeError("send succeeded but final message missing")
            if progress_tracker:
                await progress_tracker.upload_cb(file_size, file_size, force=True)
            return True, final_msg

        except asyncio.TimeoutError:
            config.logger.warning(
                f"⏱️ Upload STALLED past {upload_timeout:.0f}s on attempt "
                f"{attempt+1}/{config.MAX_RETRIES} for {file_name} "
                f"({human_readable_size(file_size)}) — treating as failed "
                f"attempt, retrying with backoff instead of hanging forever."
            )
            # Best effort: cancel the pending message so a late success
            # doesn't duplicate the file after our retry.
            try:
                if fut is not None:
                    cancel_pending_send(getattr(res, 'id', 0))
                await bot_client.deleteMessages(
                    chat_id=dest_id, message_ids=[getattr(res, 'id', 0)], revoke=True
                )
            except Exception:
                pass
            backoff = min(10 * (2 ** attempt), 120)
            await asyncio.sleep(backoff)

        except FloodWaitError as e:
            wait = e.x + 5
            config.logger.warning(
                f"⏳ FloodWait {e.x}s on upload attempt {attempt+1} — "
                f"waiting {wait}s (not counted as failure)"
            )
            if fut is not None and res is not None:
                cancel_pending_send(getattr(res, 'id', 0))
            await asyncio.sleep(wait)

        except PermissionError:
            if fut is not None and res is not None:
                cancel_pending_send(getattr(res, 'id', 0))
            raise

        except asyncio.CancelledError:
            if fut is not None and res is not None:
                cancel_pending_send(getattr(res, 'id', 0))
            raise

        except Exception as e:
            backoff = min(10 * (2 ** attempt), 120)
            config.logger.error(
                f"Upload attempt {attempt+1}/{config.MAX_RETRIES} failed "
                f"for {file_name}: {type(e).__name__}: {e} — retrying in {backoff}s"
            )
            await asyncio.sleep(backoff)

    return False, None


async def _send_sticker(
    bot_client, dest_id: int, file_path: str,
    emoji: str, width: int, height: int, dest_topic_id,
) -> tuple:
    """
    Send a downloaded sticker file as a REAL sticker (webp/tgs/webm).
    Stickers are tiny, so the blocking pytdbot helper is fine here —
    no progress bar needed. Falls back to (False, None) on failure so the
    caller can retry as a plain document.
    """
    from pytdbot import types
    try:
        res = await bot_client.sendSticker(
            chat_id=dest_id,
            sticker=types.InputFileLocal(path=file_path),
            emoji=emoji or "👍",
            width=width or 512,
            height=height or 512,
            **_topic_kw(dest_topic_id),
        )
        if config.is_error(res):
            retry = config.get_retry_after(res)
            if retry:
                raise FloodWaitError(retry)
            config.logger.warning(f"sendSticker error: {config.err_text(res)}")
            return False, None
        return True, res
    except FloodWaitError:
        raise
    except Exception as e:
        config.logger.warning(f"sendSticker failed: {e} — will try as document")
        return False, None


# ── MAIN TRANSFER FUNCTION ────────────────────────────────────────────────────

async def transfer_process(
    event,
    user_client,
    bot_client,
    source_id,
    dest_id: int,
    start_msg: int,
    end_msg: int,
    session_id: str,
    log_channel=None,
    topic_id=None,
    dest_topic_id=None,
    source_link=None,
):
    from session_manager import SessionExpiredError

    session_data = config.active_sessions.get(session_id, {})
    settings     = session_data.get('settings', {})
    user_id      = session_data.get('user_id')
    task_id      = session_data.get('task_id')

    mode_text = "Standard"
    if topic_id      is not None: mode_text += f" | 🧵 Src Topic {topic_id}"
    if dest_topic_id is not None: mode_text += f" | 🎯 Dst Topic {dest_topic_id}"

    async def _event_respond(text, reply_markup=None):
        if hasattr(event, 'respond'):
            return await event.respond(text, reply_markup=reply_markup)
        return await event.reply_text(text, reply_markup=reply_markup)

    status_message = await _event_respond(
        f"🔍 **Checking permissions…**\n"
        f"⚡ Mode: {mode_text}\n"
        f"📍 Source: `{source_id}` → Dest: `{dest_id}`",
        reply_markup=get_progress_keyboard()
    )
    session_data['task_object'] = asyncio.current_task()

    # ── STEP 0: Preflight ─────────────────────────────────────────────────────
    preflight_ok, preflight_err = await _preflight_check(bot_client, dest_id)
    if not preflight_ok:
        await safe_edit_message(status_message, preflight_err)
        config.active_sessions.pop(session_id, None)
        return 'stopped_errors', config.RETRY_AFTER_FAILURE_SECONDS

    # ── STEP 0a: SOURCE preflight on the user client ──────────────────────────
    # Resolve BEFORE scanning so a genuinely inaccessible source gives the
    # clear "could not access" message instead of a generic crash + retries.
    try:
        src_chat  = await resolve_source_chat(user_client, source_id, link=source_link)
        source_id = src_chat.id
        config.logger.info(
            f"✅ Source preflight OK: {getattr(src_chat, 'title', source_id)!r} ({source_id})"
        )
    except FloodWaitError as e:
        config.logger.warning(f"Source preflight FloodWait {e.x}s — waiting…")
        await asyncio.sleep(e.x + 2)
    except Exception as e:
        await safe_edit_message(
            status_message,
            "❌ **Could not access the source channel/group.**\n\n"
            "Make sure your logged-in account is a member of the source chat, "
            "then run `/clone` again.\n\n"
            f"`{str(e)[:150]}`"
        )
        config.active_sessions.pop(session_id, None)
        return 'stopped_source', None

    # ── STEP 0b: Warm the PTB singleton (used ONLY for progress-bar edits;
    # every file payload travels via TDLib) ───────────────────────────────────
    try:
        await config.get_ptb_bot()
    except Exception as e:
        config.logger.warning(f"PTB init failed (progress edits fall back to TDLib): {e}")

    log_channel = await _warmup_log_channel(bot_client, log_channel)

    # ── STEP 0c: Download custom thumbnail once (if user set one) ─────────────
    custom_thumb_path = None
    thumbnail_file_id = settings.get('thumbnail_file_id')
    if thumbnail_file_id:
        try:
            custom_thumb_path = f"/tmp/custom_thumb_{user_id}_{int(time.time())}.jpg"
            await td_download_remote_id(bot_client, thumbnail_file_id, custom_thumb_path)
            config.logger.info(f"✅ Custom thumbnail ready (disk): {custom_thumb_path}")
        except Exception as e:
            config.logger.warning(f"Custom thumbnail download failed: {e}")
            custom_thumb_path = None

    thumb_note = " | 🖼️ Custom Thumb" if custom_thumb_path else ""
    await safe_edit_message(
        status_message,
        f"🚀 **Starting Transfer…**\n"
        f"⚡ Mode: {mode_text}{thumb_note}\n"
        f"📍 Source: `{source_id}` → Dest: `{dest_id}`",
        reply_markup=get_progress_keyboard()
    )

    # ── State vars ────────────────────────────────────────────────────────────
    total_success      = 0
    total_size         = 0
    total_skipped      = 0
    deleted_msgs       = 0
    consecutive_errors = 0
    idx                = 0
    last_seen_id       = start_msg - 1
    overall_start      = time.time()
    chat_id            = getattr(event, 'chat_id', None) or session_data.get('chat_id')
    stop_reason         = None

    # ── Resume pointer (identical semantics to the Pyrofork engine) ───────────
    last_committed_id   = start_msg - 1
    stuck_msg_id        = None
    stuck_attempts      = 0
    pending_retry_delay = None

    # Self-healing: trust a matching existing checkpoint over the handed-in
    # start_msg (see Pyrofork version for the full rationale).
    if user_id:
        try:
            _existing_ckpt = await db.get_transfer_checkpoint(user_id)
        except Exception:
            _existing_ckpt = None
        if (
            _existing_ckpt
            and str(_existing_ckpt.get('source_id')) == str(source_id)
            and str(_existing_ckpt.get('dest_id'))   == str(dest_id)
            and int(_existing_ckpt.get('end_msg', 0)) == int(end_msg)
        ):
            saved_current = int(_existing_ckpt.get('current_msg', last_committed_id))
            last_committed_id = saved_current
            stuck_msg_id      = _existing_ckpt.get('stuck_msg_id')
            stuck_attempts    = _existing_ckpt.get('stuck_attempts', 0)

    effective_start_msg = last_committed_id + 1
    last_seen_id         = last_committed_id

    async def _should_stop() -> bool:
        if config.global_stop_flag or session_id not in config.active_sessions:
            return True
        if session_data.get('stop_flag'):
            return True
        if task_id:
            return await db.check_stop_signal(task_id)
        return False

    def _progress_text(title: str, extra: str = "") -> str:
        ram_used  = session_data.get('ram_used',  0)
        ram_total = session_data.get('ram_total', 0)
        ram_line  = _ram_bar(ram_used, ram_total)
        return f"{title}\n{ram_line}{extra}".strip()

    async def _commit_or_hold(msg_id: int, ok: bool) -> bool:
        nonlocal last_committed_id, stuck_msg_id, stuck_attempts

        if msg_id <= last_committed_id:
            return False

        if ok:
            last_committed_id = msg_id
            stuck_msg_id, stuck_attempts = None, 0
            return False

        if stuck_msg_id == msg_id:
            stuck_attempts += 1
        else:
            stuck_msg_id, stuck_attempts = msg_id, 1
        return True

    def _stuck_backoff_seconds() -> int:
        return min(
            config.RETRY_AFTER_FAILURE_SECONDS * (2 ** max(stuck_attempts - 1, 0)),
            config.STUCK_BACKOFF_MAX,
        )

    async def _checkpoint_now():
        if not user_id:
            return
        try:
            await db.save_transfer_checkpoint(user_id, {
                'chat_id':        chat_id,
                'source_id':      str(source_id),
                'dest_id':        str(dest_id),
                'current_msg':    last_committed_id,
                'end_msg':        end_msg,
                'settings':       settings,
                'topic_id':       topic_id,
                'dest_topic_id':  dest_topic_id,
                'log_channel':    log_channel,
                'task_id':        task_id,
                'source_link':    source_link,
                'total_success':  total_success,
                'total_skipped':  total_skipped,
                'total_size':     total_size,
                'status':         'running',
                'stuck_msg_id':   stuck_msg_id,
                'stuck_attempts': stuck_attempts,
            })
        except Exception as ckpt_err:
            config.logger.warning(f"Checkpoint save failed: {ckpt_err}")

    async def _notify_retry(msg_id: int):
        if stuck_attempts in (1, 2, 5) or stuck_attempts % 10 == 0:
            wait_s = _stuck_backoff_seconds()
            await safe_edit_message(status_message, _progress_text(
                f"⏳ **Message {msg_id} failed (attempt {stuck_attempts}).**\n"
                f"It will NOT be skipped — auto-resume retries it in ~{wait_s}s."
            ))

    # ── MAIN LOOP ─────────────────────────────────────────────────────────────
    try:
        await safe_edit_message(status_message, "🔍 **Scanning messages…**",
                                reply_markup=get_progress_keyboard())

        async for message in robust_iter_messages(
            user_client, source_id, effective_start_msg, end_msg, topic_id,
            source_link=source_link,
        ):
            if await _should_stop():
                stop_reason = 'user_stop'
                await safe_edit_message(
                    status_message,
                    _progress_text("🚫 **Stopped by user/admin.**")
                )
                break

            if message.id > last_seen_id + 1:
                deleted_msgs += message.id - last_seen_id - 1
            last_seen_id = message.id
            idx         += 1

            # Skip service messages (pins, joins, etc.) — the ONLY things
            # that skip. Everything else commits-or-holds.
            if is_service_message(message):
                await _commit_or_hold(message.id, True)
                await _checkpoint_now()
                continue

            # ── Inter-file pacing ─────────────────────────────────────────
            await asyncio.sleep(config.SLEEP_BETWEEN_FILES)
            if idx % 10 == 0:
                await asyncio.sleep(config.SLEEP_EVERY_10)

            sent_message = None
            success      = False
            temp_path    = None
            thumb_path   = None
            progress_tracker = None
            is_video_mode    = False
            is_audio         = False
            modified_caption = None

            try:
                content_name = _cname(getattr(message, 'content', None))

                # ══ TEXT / WEB-PAGE ═══════════════════════════════════════
                has_web_page = content_name == 'MessageText' and (
                    getattr(message.content, 'link_preview', None) is not None
                )
                if content_name == 'MessageText' or content_name in ('', 'None'):
                    text_ok = True
                    plain = message_plain_text(message)
                    if plain:
                        try:
                            modified_text = apply_caption_manipulations(message, settings)
                            res = await bot_client.sendTextMessage(
                                dest_id, modified_text,
                                parse_mode="html",
                                **_topic_kw(dest_topic_id),
                            )
                            _raise_if_error(res, "(text)")
                            sent_message         = res
                            total_success      += 1
                            consecutive_errors  = 0
                            if log_channel:
                                await log_transfer(
                                    bot_client, log_channel, sent_message,
                                    session_id, dest_id, "text",
                                    caption=modified_text,
                                )
                        except FloodWaitError:
                            raise
                        except PermissionError:
                            raise
                        except Exception as txt_e:
                            config.logger.error(f"Text send failed: {txt_e}")
                            text_ok             = False
                            total_skipped      += 1
                            consecutive_errors += 1
                    should_stop = await _commit_or_hold(message.id, text_ok)
                    await _checkpoint_now()
                    if should_stop:
                        stop_reason = 'errors'
                        await _notify_retry(message.id)
                        break
                    continue

                # ══ SPECIAL MEDIA (polls, geo, contacts, dice…) ═══════════
                if is_special_media(message):
                    sm_ok = True
                    try:
                        res = await user_client.forwardMessages(
                            chat_id=dest_id,
                            from_chat_id=message.chat_id,
                            # message.id is server-domain here (normalized by
                            # robust_iter_messages) — forwardMessages needs the
                            # TDLib-domain id:
                            message_ids=[_td_msg_id(message.id)],
                            send_copy=True,
                            **_topic_kw(dest_topic_id),
                        )
                        _raise_if_error(res, "(special media copy)")
                        total_success      += 1
                        consecutive_errors  = 0
                    except FloodWaitError:
                        raise
                    except Exception as sm_e:
                        config.logger.error(f"Special media failed: {sm_e}")
                        sm_ok               = False
                        total_skipped      += 1
                        consecutive_errors += 1
                    should_stop = await _commit_or_hold(message.id, sm_ok)
                    await _checkpoint_now()
                    if should_stop:
                        stop_reason = 'errors'
                        await _notify_retry(message.id)
                        break
                    continue

                # ══ FILE INFO ═════════════════════════════════════════════
                file_name, mime_type, is_video_mode = get_target_info(message)
                if not file_name:
                    config.logger.warning(f"Msg {message.id}: cannot derive filename — skipping")
                    total_skipped += 1
                    continue

                file_name        = sanitize_filename(apply_filename_manipulations(file_name, settings))
                modified_caption = apply_caption_manipulations(message, settings)
                file_size        = get_media_file_size(message)

                is_photo = content_name == 'MessagePhoto'
                is_image = (not is_photo) and ("image" in (mime_type or ""))
                is_audio = bool(
                    content_name in ('MessageAudio', 'MessageVoiceNote') or
                    "audio" in (mime_type or "") or "voice" in (mime_type or "")
                )

                # ── Media metadata (used by both PTB and TDLib paths) ──────
                media_duration     = 0
                media_width        = 0
                media_height       = 0
                auto_thumb_remote  = None
                if is_video_mode:
                    media_duration, media_width, media_height, auto_thumb_remote = \
                        get_video_metadata(message)
                elif content_name == 'MessageVideoNote':
                    media_duration = getattr(message.content.video_note, 'duration', 0) or 0
                elif is_audio:
                    if content_name == 'MessageAudio':
                        media_duration = getattr(message.content.audio, 'duration', 0) or 0
                    elif content_name == 'MessageVoiceNote':
                        media_duration = getattr(message.content.voice_note, 'duration', 0) or 0

                start_time = time.time()

                # ══ PATH A: Photo / Image (TDLib only) ════════════════════
                # (stickers are 'image/webp' — intercepted by PATH S below)
                if (is_photo or is_image) and content_name != 'MessageSticker':
                    try:
                        temp_path  = f"/tmp/tf_img_{user_id}_{message.id}_{int(time.time())}.jpg"
                        downloaded = await td_download(user_client, message, temp_path)
                        if not downloaded or not os.path.exists(str(downloaded)):
                            raise RuntimeError("download returned empty path")
                        temp_path = str(downloaded)

                        from pytdbot import types as _t
                        res = await bot_client.sendPhoto(
                            chat_id=dest_id,
                            photo=_t.InputFileLocal(path=temp_path),
                            caption=modified_caption,
                            parse_mode="html",
                            **_topic_kw(dest_topic_id),
                        )
                        _raise_if_error(res, "(photo)")
                        sent_message = res
                        success      = True

                    except FloodWaitError:
                        raise
                    except PermissionError:
                        raise
                    except Exception as img_e:
                        config.logger.error(f"Image send failed: {img_e}")

                # ══ PATH S: Sticker → real sticker, fallback to document ══
                elif content_name == 'MessageSticker':
                    try:
                        sticker_obj = message.content.sticker
                        st_emoji    = getattr(sticker_obj, 'emoji', '') or '👍'
                        st_w        = getattr(sticker_obj, 'width', 0) or 512
                        st_h        = getattr(sticker_obj, 'height', 0) or 512

                        # Keep the real extension (.webp/.tgs/.webm) so TDLib
                        # detects the sticker type correctly on re-upload.
                        st_ext     = os.path.splitext(file_name)[1] or '.webp'
                        temp_path  = f"/tmp/tf_stk_{user_id}_{message.id}_{int(time.time())}{st_ext}"
                        downloaded = await td_download(user_client, message, temp_path)
                        if not downloaded or not os.path.exists(str(downloaded)):
                            raise RuntimeError("download returned empty path")
                        temp_path = str(downloaded)

                        success, sent_message = await _send_sticker(
                            bot_client, dest_id, temp_path,
                            st_emoji, st_w, st_h, dest_topic_id,
                        )
                        if not success:
                            # Fallback: deliver the raw sticker file as a document
                            success, sent_message = await _bot_disk_upload(
                                bot_client, dest_id,
                                temp_path, file_name, file_size,
                                modified_caption,
                                is_video=False, is_audio=False,
                                thumb_path=None,
                                dest_topic_id=dest_topic_id,
                                progress_tracker=None,
                            )

                    except FloodWaitError:
                        raise
                    except PermissionError:
                        raise
                    except Exception as stk_e:
                        config.logger.error(f"Sticker send failed: {stk_e}")

                # ══ PATH B: Non-image files (videos, docs, audio…) ════════
                # EVERY file, any size: TDLib user-client downloads to disk,
                # TDLib bot-client uploads from disk. No PTB for payloads.
                elif not (is_photo or is_image):
                    progress_tracker = TransferProgress(
                        file_name, file_size, status_message, session_data,
                        msg_id=message.id,
                    )
                    # Show the first download frame immediately — no dead air
                    # between "Scanning…" and the first throttled updateFile.
                    await progress_tracker.download_cb(0, file_size, force=True)

                    temp_path = _named_temp_path(user_id, message.id, file_name)

                    # Video's own thumbnail (custom thumb takes priority)
                    if is_video_mode and auto_thumb_remote and not custom_thumb_path:
                        try:
                            thumb_path = f"/tmp/thumb_{user_id}_{message.id}.jpg"
                            await td_download_remote_id(
                                user_client, auto_thumb_remote, thumb_path,
                            )
                        except Exception as thumb_e:
                            config.logger.warning(f"Auto-thumb fetch failed: {thumb_e}")
                            thumb_path = None

                    effective_thumb = custom_thumb_path or thumb_path

                    if file_size > SPLIT_THRESHOLD:
                        # ── Split upload for very large files ─────────
                        downloaded = await td_download(
                            user_client, message, temp_path,
                            tracker=progress_tracker,
                        )
                        actual_size = os.path.getsize(str(downloaded))
                        parts       = math.ceil(actual_size / SPLIT_THRESHOLD)
                        config.logger.info(f"✂️ Splitting into {parts} parts")

                        all_parts_ok = True
                        with open(str(downloaded), 'rb') as full_file:
                            for i in range(parts):
                                if await _should_stop():
                                    all_parts_ok = False
                                    break
                                part_num  = i + 1
                                part_name = (
                                    f"{os.path.splitext(file_name)[0]}"
                                    f".part{part_num:03d}"
                                    f"{os.path.splitext(file_name)[1]}"
                                )
                                part_path = _named_temp_path(
                                    user_id, f"{message.id}_p{i}", part_name
                                )
                                part_data = full_file.read(SPLIT_THRESHOLD)
                                with open(part_path, 'wb') as pf:
                                    pf.write(part_data)

                                part_cap = f"{modified_caption}\n\n(Part {part_num}/{parts})"
                                part_tracker = TransferProgress(
                                    part_name, len(part_data),
                                    status_message, session_data,
                                    msg_id=message.id,
                                )
                                part_tracker.reset_for_upload()
                                part_ok, part_sent = await _bot_disk_upload(
                                    bot_client, dest_id,
                                    part_path, part_name, len(part_data),
                                    part_cap,
                                    is_video=False, is_audio=False,
                                    thumb_path=None,
                                    dest_topic_id=dest_topic_id,
                                    progress_tracker=part_tracker,
                                )
                                if part_ok and log_channel:
                                    await log_transfer(
                                        bot_client, log_channel, part_sent,
                                        session_id, dest_id, part_name,
                                        part_num=part_num,
                                        file_path=part_path,
                                        caption=part_cap,
                                    )
                                part_tracker.invalidate()
                                _cleanup_temp(part_path)

                                if not part_ok:
                                    all_parts_ok = False
                                    break

                        try: _cleanup_temp(str(downloaded))
                        except Exception: pass
                        temp_path = None

                        if all_parts_ok:
                            success      = True
                            sent_message = True

                    else:
                        # ── Normal disk download → upload ──────────────
                        downloaded = await td_download(
                            user_client, message, temp_path,
                            tracker=progress_tracker,
                        )
                        if not downloaded or not os.path.exists(str(downloaded)):
                            raise RuntimeError("download returned empty path")
                        temp_path = str(downloaded)

                        # Instantly flip the bar to the upload phase so it
                        # never looks stuck on the last download frame.
                        progress_tracker.reset_for_upload()

                        ok, sent_message = await _bot_disk_upload(
                            bot_client, dest_id,
                            temp_path, file_name, file_size,
                            modified_caption,
                            is_video=is_video_mode,
                            is_audio=is_audio,
                            thumb_path=effective_thumb,
                            dest_topic_id=dest_topic_id,
                            progress_tracker=progress_tracker,
                            duration=media_duration,
                            width=media_width,
                            height=media_height,
                        )
                        success = ok

                # ══ OUTCOME ══════════════════════════════════════════════
                if success:
                    total_success      += 1
                    consecutive_errors  = 0
                    elapsed             = time.time() - start_time
                    total_size         += file_size

                    if log_channel and sent_message and not isinstance(sent_message, bool):
                        await log_transfer(
                            bot_client, log_channel, sent_message,
                            session_id, dest_id, file_name,
                            file_path=temp_path,
                            is_video=is_video_mode,
                            is_audio=is_audio,
                            caption=modified_caption,
                        )

                else:
                    config.logger.error(f"❌ Attempt failed, will retry via auto-resume: {file_name}")
                    total_skipped += 1
                    await safe_edit_message(
                        status_message,
                        _progress_text(
                            f"❌ **Failed:** `{file_name[:35]}`",
                            "\nThis file will NOT be skipped — auto-resume retries it."
                        ),
                        reply_markup=get_progress_keyboard(),
                    )

                should_stop = await _commit_or_hold(message.id, success)
                await _checkpoint_now()
                if should_stop:
                    stop_reason = 'errors'
                    await _notify_retry(message.id)
                    break

            except FloodWaitError as fw:
                # Session/account-level wait — honor Telegram's exact delay.
                wait_s = int(fw.x or 20)
                config.logger.warning(
                    f"⏳ FloodWait {wait_s}s on msg {message.id} — "
                    f"stopping run, auto-resume honors this wait exactly."
                )
                total_skipped += 1
                await _commit_or_hold(message.id, False)
                await _checkpoint_now()
                stop_reason          = 'errors'
                pending_retry_delay  = wait_s + 15
                await safe_edit_message(status_message, _progress_text(
                    f"⏳ **Telegram flood-wait hit** (message {message.id}).\n"
                    f"Nothing skipped — auto-resume continues in ~{pending_retry_delay}s."
                ))
                break

            except PermissionError as perm_e:
                await _commit_or_hold(message.id, False)
                await _checkpoint_now()
                stop_reason = 'errors'
                await safe_edit_message(status_message, str(perm_e))
                config.logger.error(f"Permission error — stopping: {perm_e}")
                break

            except asyncio.CancelledError:
                raise

            except MemoryError:
                config.logger.error(f"💥 OOM on msg {message.id} — will retry via auto-resume")
                total_skipped += 1
                should_stop = await _commit_or_hold(message.id, False)
                await _checkpoint_now()
                if should_stop:
                    stop_reason = 'errors'
                    await _notify_retry(message.id)
                    break

            except Exception as e:
                config.logger.error(f"❌ Error on msg {message.id}: {e}", exc_info=True)
                total_skipped += 1
                should_stop = await _commit_or_hold(message.id, False)
                await _checkpoint_now()
                if should_stop:
                    stop_reason = 'errors'
                    await _notify_retry(message.id)
                    break

            finally:
                if progress_tracker is not None:
                    try:
                        progress_tracker.invalidate()
                    except Exception:
                        pass
                for p in [temp_path, thumb_path]:
                    _cleanup_temp(p)

        # ── POST-LOOP ─────────────────────────────────────────────────────────
        if last_seen_id < end_msg and last_seen_id >= start_msg:
            deleted_msgs += end_msg - last_seen_id

        overall_time      = time.time() - overall_start
        avg_speed         = total_size / overall_time / (1024 * 1024) if overall_time > 0 else 0
        actually_complete = last_committed_id >= end_msg

        header  = "🏁 **Transfer Complete!**" if actually_complete else "🏁 **Transfer Finished**"
        summary = (
            f"{header}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Success:         `{total_success}`\n"
        )
        if deleted_msgs > 0:
            summary += f"🗑️ Not Found:       `{deleted_msgs}` _(deleted/restricted)_\n"
        label = "🔁 Retrying:" if not actually_complete else "⏭️ Skipped:"
        summary += (
            f"{label}         `{total_skipped}`\n"
            f"📦 Total Size:      `{human_readable_size(total_size)}`\n"
            f"⚡ Avg Speed:       `{avg_speed:.1f} MB/s`\n"
            f"⏱️ Time:            `{time_formatter(overall_time)}`"
        )
        if not actually_complete and stop_reason != 'user_stop':
            wait_s = pending_retry_delay or _stuck_backoff_seconds()
            summary += (
                f"\n\n🔄 *Nothing was skipped — auto-resume continues from the "
                f"exact last point in ~{wait_s}s.*"
            )
        await safe_edit_message(status_message, summary)

        if actually_complete and user_id:
            await db.clear_transfer_checkpoint(user_id)
            config.logger.info(f"✅ Checkpoint cleared for user {user_id}")
            return 'completed', None

        if stop_reason == 'user_stop':
            return 'stopped_by_user', None

        return 'stopped_errors', (pending_retry_delay or _stuck_backoff_seconds())

    except asyncio.CancelledError:
        await safe_edit_message(
            status_message,
            "🚫 **Task Forcefully Revoked**\n💡 Use /clone to start a new transfer."
        )
        return 'stopped_by_user', None

    except Exception as e:
        await safe_edit_message(
            status_message,
            f"💥 **Critical Error:**\n`{str(e)[:200]}`\n🔄 Auto-resume will retry shortly."
        )
        config.logger.error(f"Transfer crashed: {e}", exc_info=True)
        return 'stopped_errors', config.RETRY_AFTER_FAILURE_SECONDS

    finally:
        if custom_thumb_path and os.path.exists(custom_thumb_path):
            try: os.remove(custom_thumb_path)
            except Exception: pass

        config.active_sessions.pop(session_id, None)
        if not task_id:
            try:
                await session_manager.stop_user_session(user_client)
            except Exception:
                pass
        config.logger.info("✅ Transfer cleanup complete")
