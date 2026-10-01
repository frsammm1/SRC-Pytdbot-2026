"""
progress.py  –  Throttled progress callbacks + TDLib event bridges.

WHY THROTTLING IS CRITICAL:
  TDLib fires updateFile events continuously while a file downloads/uploads.
  Without throttling → hundreds of message edits → instant 429 → bot stuck.
  With 8-second throttle → ~6 edits in 50 seconds → safe for 30+ concurrent users.

WHY EDITS GO THROUGH PTB (HTTP Bot API) INSTEAD OF TDLIB (MTProto):
  PTB's HTTPS path has its own rate-limit bucket, completely separate from
  the TDLib connections doing the actual download/upload work. PTB is used
  ONLY for these status edits — every file itself travels via TDLib.

TDLIB WIRING:
  Pyrogram passed (current, total) into a progress callback. TDLib instead
  emits updateFile events carrying file.local.downloaded_size (download) and
  file.remote.uploaded_size (upload). The bridges at the bottom of this file
  listen to on_updateFile on each client and fan events out to every
  registered TransferProgress tracker — so the public interface here is
  UNCHANGED: tracker.download_cb(current, total) / upload_cb(current, total).
"""

import asyncio
import math
import time

import config
from utils import human_readable_size, time_formatter


# Only the LATEST tracker for a given status message is allowed to edit it.
# Previous-file in-flight edits used to win the race and freeze the bar on
# an old filename while the current download/upload kept going.
_status_owners: dict = {}   # (chat_id, raw_status_id) → TransferProgress


class TransferProgress:
    """
    Progress tracker for a single file's download + upload phases.

    status_msg is a pytdbot types.Message — only .chat_id / .id are read.
    """

    def __init__(
        self,
        file_name:    str,
        file_size:    int,
        status_msg,
        session_data: dict,
        msg_id:       int = 0,
    ):
        self.file_name    = file_name
        self.file_size    = file_size or 0
        self.status_msg   = status_msg
        self.session_data = session_data
        self.msg_id       = msg_id

        self._chat_id    = getattr(status_msg, 'chat_id', None)
        raw_id           = getattr(status_msg, 'id', None)
        self._raw_id     = raw_id
        # TDLib message ids are (server_id << 20); the Bot API needs the raw
        # server id. Local/unsent ids (low 20 bits non-zero, or negative) can
        # only be edited via TDLib — skip the PTB path for those.
        if raw_id and raw_id > 0 and (raw_id & ((1 << 20) - 1)) == 0:
            self._message_id = raw_id >> 20
        else:
            self._message_id = None

        self._start_time   = time.time()
        self._last_dl      = 0.0
        self._last_ul      = 0.0
        self._phase        = "download"
        self._alive        = True
        self._seq          = 0
        self._lock         = asyncio.Lock()

        owner_key = (self._chat_id, raw_id)
        prev = _status_owners.get(owner_key)
        if prev is not None and prev is not self:
            prev.invalidate()
        _status_owners[owner_key] = self

    # ── PUBLIC API ────────────────────────────────────────────────────────────

    def invalidate(self):
        """Stop this tracker from editing the status message (next file owns it)."""
        self._alive = False
        self._seq  += 1
        key = (self._chat_id, self._raw_id)
        if _status_owners.get(key) is self:
            _status_owners.pop(key, None)

    async def download_cb(self, current: int, total: int, force: bool = False):
        if not self._alive:
            return
        # A late updateFile after reset_for_upload must NOT flip the bar
        # back to "Downloading" (that's the frozen-on-old-frame bug).
        if self._phase == "upload":
            return
        now = time.time()
        if not force and now - self._last_dl < config.DOWNLOAD_PROGRESS_INTERVAL:
            return
        self._last_dl = now
        self._phase   = "download"
        text = self._build_text(current, total)
        self._seq += 1
        asyncio.create_task(self._safe_edit(text, self._seq))

    async def upload_cb(self, current: int, total: int, force: bool = False):
        if not self._alive:
            return
        now = time.time()
        if not force and now - self._last_ul < config.UPLOAD_PROGRESS_INTERVAL:
            return
        self._last_ul = now
        self._phase   = "upload"
        text = self._build_text(current, total)
        self._seq += 1
        asyncio.create_task(self._safe_edit(text, self._seq))

    def reset_for_upload(self):
        """Switch to the upload phase and IMMEDIATELY show an upload bar so
        the status never looks stuck on the last download frame."""
        if not self._alive:
            return
        self._start_time = time.time()
        self._last_ul    = 0.0
        self._phase      = "upload"
        # Bump seq FIRST so any in-flight "Downloading 100%" edit is dropped.
        self._seq       += 1
        text = self._build_text(0, self.file_size)
        asyncio.create_task(self._safe_edit(text, self._seq))

    # ── INTERNAL ──────────────────────────────────────────────────────────────

    def _build_text(self, current: int, total: int) -> str:
        total   = total or self.file_size or 1
        elapsed = time.time() - self._start_time
        speed   = current / elapsed if elapsed > 0 else 0
        eta     = (total - current) / speed if speed > 0 else 0
        pct     = min(100.0, current * 100 / total) if total else 0.0
        filled  = math.floor(pct / 100 * 12)
        bar     = "▓" * filled + "░" * (12 - filled)

        if self._phase == "download":
            icon  = "📥"
            label = "Downloading"
        else:
            icon  = "📤"
            label = "Uploading"

        name = (self.file_name or "file").replace("`", "'")
        name_display = (name[:38] + "…") if len(name) > 38 else name

        msg_line = f" • #{self.msg_id}" if self.msg_id else ""

        return (
            f"{icon} **{label}**{msg_line}\n"
            f"`{bar}` **{pct:.0f}%**\n"
            f"`{name_display}`\n"
            f"💾 `{human_readable_size(current)}/{human_readable_size(total)}`  "
            f"⚡`{human_readable_size(speed)}/s`  ⏱`{time_formatter(eta)}`"
        )

    async def _safe_edit(self, text: str, seq: int = 0):
        """Edit via PTB (HTTP Bot API); fall back to TDLib edit on failure.

        `seq` drops stale in-flight edits so an older file can never overwrite
        the current progress bar.
        """
        if not self._alive or (seq and seq != self._seq):
            return
        async with self._lock:
            if not self._alive or (seq and seq != self._seq):
                return
            edited = False
            if self._message_id is not None:
                try:
                    ptb_bot = await config.get_ptb_bot()
                    if not self._alive or (seq and seq != self._seq):
                        return
                    await ptb_bot.edit_message_text(
                        chat_id=self._chat_id,
                        message_id=self._message_id,
                        text=text,
                        parse_mode="markdown",
                    )
                    edited = True
                except Exception:
                    edited = False
            if not edited:
                try:
                    if not self._alive or (seq and seq != self._seq):
                        return
                    await self.status_msg.edit_text(text, parse_mode="markdown")
                    edited = True
                except Exception:
                    edited = False
            # If the edit failed, don't sit on the throttle — retry on the
            # next updateFile / poll instead of looking frozen for 8-16s.
            if not edited:
                if self._phase == "download":
                    self._last_dl = 0.0
                else:
                    self._last_ul = 0.0


# ── TDLIB updateFile → TransferProgress BRIDGE ────────────────────────────────
#
# Every TDLib client (main bot, worker bot, worker user) registers its
# on_updateFile here once at startup. transfer.py registers/unregisters a
# TransferProgress per active file_id for the duration of that download/upload.

_file_watchers: dict[int, list] = {}   # td file_id → [(tracker, phase)]


def watch_file(file_id: int, tracker: TransferProgress, phase: str) -> None:
    """Start feeding updateFile events of `file_id` into `tracker`."""
    if file_id is None:
        return
    _file_watchers.setdefault(file_id, []).append((tracker, phase))


def unwatch_file(file_id: int, tracker: TransferProgress = None) -> None:
    if file_id is None:
        return
    if tracker is None:
        _file_watchers.pop(file_id, None)
        return
    watchers = _file_watchers.get(file_id, [])
    _file_watchers[file_id] = [w for w in watchers if w[0] is not tracker]
    if not _file_watchers.get(file_id):
        _file_watchers.pop(file_id, None)


async def dispatch_update_file(file_obj) -> None:
    """
    Called from each client's on_updateFile handler.
    file_obj is pytdbot.types.File.
    """
    file_id = getattr(file_obj, 'id', None)
    if file_id is None:
        return
    watchers = _file_watchers.get(file_id)
    if not watchers:
        return
    local  = getattr(file_obj, 'local', None)
    remote = getattr(file_obj, 'remote', None)
    total  = getattr(file_obj, 'size', 0) or getattr(file_obj, 'expected_size', 0) or 0
    for tracker, phase in list(watchers):
        try:
            if not getattr(tracker, '_alive', True):
                continue
            if phase == 'download':
                current = getattr(local, 'downloaded_size', 0) or 0
                await tracker.download_cb(current, total)
            else:
                current = getattr(remote, 'uploaded_size', 0) or 0
                await tracker.upload_cb(current, total)
        except Exception:
            pass


# ── TDLIB MESSAGE-SEND TRACKING ───────────────────────────────────────────────
#
# transfer.py sends big files via the RAW TDLib sendMessage() call, which
# returns a PENDING message instantly (upload continues in the background).
# We register a future keyed by that pending (temporary) message id BEFORE the
# upload can finish, then await updateMessageSendSucceeded /
# updateMessageSendFailed — the same completion guarantee Pyrogram's blocking
# send_*() calls gave us, plus live upload progress via updateFile.
#
# ⚠️ Do NOT use pytdbot's sendVideo/sendDocument helpers for big files:
# they internally await updateMessageSendSucceeded themselves
# (sendMessageWithContent → _create_request_future), so by the time they
# return, the completion update has ALREADY fired and consumed — any future
# registered afterwards waits forever (the v7.4 "upload stalled" bug).

_pending_sends: dict[int, asyncio.Future] = {}   # temp message_id → Future


def register_pending_send(temp_message_id: int) -> asyncio.Future:
    fut = asyncio.get_event_loop().create_future()
    _pending_sends[temp_message_id] = fut
    return fut


def cancel_pending_send(temp_message_id: int) -> None:
    fut = _pending_sends.pop(temp_message_id, None)
    if fut and not fut.done():
        fut.cancel()


async def dispatch_send_succeeded(update) -> None:
    """on_updateMessageSendSucceeded — resolve pending future with the final Message."""
    old_id = getattr(update, 'old_message_id', None)
    fut = _pending_sends.pop(old_id, None)
    if fut and not fut.done():
        fut.set_result(getattr(update, 'message', None))


async def dispatch_send_failed(update) -> None:
    """on_updateMessageSendFailed — fail the pending future."""
    old_id = getattr(update, 'old_message_id', None)
    fut = _pending_sends.pop(old_id, None)
    if fut and not fut.done():
        err = getattr(update, 'error', None)
        fut.set_exception(Exception(
            f"TDLib send failed: {getattr(err, 'code', '')} {getattr(err, 'message', '')}"
            if err else "TDLib send failed"
        ))
