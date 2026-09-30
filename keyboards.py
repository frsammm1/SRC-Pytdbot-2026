"""
keyboards.py  –  Inline keyboards (v7.0 — TDLib / pytdbot)

v7.0 changes:
  • pyrogram.types.InlineKeyboardMarkup/Button →
    pytdbot.types.ReplyMarkupInlineKeyboard / InlineKeyboardButton.
  • Callback payload is InlineKeyboardButtonTypeCallback(data=b"...")
    (bytes, max 64 bytes — same Telegram limit as before).
  • URL buttons use InlineKeyboardButtonTypeUrl.
  • Every exported helper keeps the EXACT same name/signature as the
    Pyrofork version, so handlers.py logic is unchanged.
"""

from pytdbot import types


def _cb(text: str, data: str) -> types.InlineKeyboardButton:
    """Callback button (data as UTF-8 bytes)."""
    return types.InlineKeyboardButton(
        text=text,
        type=types.InlineKeyboardButtonTypeCallback(data=data.encode()),
    )


def _url_btn(text: str, url: str) -> types.InlineKeyboardButton:
    return types.InlineKeyboardButton(
        text=text,
        type=types.InlineKeyboardButtonTypeUrl(url=url),
    )


def _markup(rows: list) -> types.ReplyMarkupInlineKeyboard:
    return types.ReplyMarkupInlineKeyboard(rows=rows)


def get_settings_keyboard(
    session_id,
    dest_topic_id=None,
    thumbnail_set: bool = False,
) -> types.ReplyMarkupInlineKeyboard:
    """Main settings keyboard for file manipulation."""
    topic_label = (
        f"🧵 Dest Topic: {dest_topic_id} ✅"
        if dest_topic_id
        else "🧵 Set Destination Topic"
    )
    thumb_label = (
        "🖼️ Transfer Thumbnail: ✅ (tap to change)"
        if thumbnail_set
        else "🖼️ Set Transfer Thumbnail"
    )
    return _markup([
        [_cb("📝 Filename: Find & Replace",  f"set_fname_{session_id}")],
        [_cb("💬 Caption: Find & Replace",   f"set_fcap_{session_id}")],
        [_cb("✂️ Caption: Remove Text",      f"set_cap_remove_{session_id}")],
        [_cb("➕ Add Extra Caption",          f"set_xcap_{session_id}")],
        [_cb(topic_label,                    f"set_dest_topic_{session_id}")],
        [_cb(thumb_label,                    f"set_thumbnail_{session_id}")],
        [
            _cb("✅ Done - Start Transfer",  f"confirm_{session_id}"),
            _cb("❌ Cancel",                 f"cancel_{session_id}"),
        ],
    ])


def get_confirm_keyboard(session_id, settings, dest_topic_id=None):
    """
    Build a settings summary text + confirm keyboard.
    Returns (settings_text: str, ReplyMarkupInlineKeyboard).
    """
    settings_text = "**Current Settings:**\n\n"

    if settings.get('find_name'):
        settings_text += (
            f"📝 Filename:\n`{settings['find_name']}` → "
            f"`{settings.get('replace_name', '')}`\n\n"
        )

    if settings.get('find_cap'):
        settings_text += (
            f"💬 Caption:\n`{settings['find_cap']}` → "
            f"`{settings.get('replace_cap', '')}`\n\n"
        )

    if settings.get('extra_cap'):
        settings_text += f"➕ Extra Caption:\n`{settings['extra_cap'][:50]}...`\n\n"

    cap_rules = settings.get('cap_rules', [])
    if cap_rules:
        settings_text += f"📋 Caption Rules ({len(cap_rules)}):\n"
        for i, (find_str, replace_str) in enumerate(cap_rules[:5], 1):
            action = "REMOVE" if replace_str == "" else f"→ `{replace_str[:20]}`"
            settings_text += f"  {i}. `{find_str[:20]}` {action}\n"
        if len(cap_rules) > 5:
            settings_text += f"  …and {len(cap_rules) - 5} more\n"
        settings_text += "\n"

    fname_rules = settings.get('fname_rules', [])
    if fname_rules:
        settings_text += f"📋 Filename Rules ({len(fname_rules)}):\n"
        for i, (find_str, replace_str) in enumerate(fname_rules[:3], 1):
            settings_text += f"  {i}. `{find_str[:20]}` → `{replace_str[:20]}`\n"
        settings_text += "\n"

    if dest_topic_id:
        settings_text += f"🧵 Destination Topic ID: `{dest_topic_id}`\n\n"

    if settings.get('thumbnail_set'):
        settings_text += "🖼️ Transfer Thumbnail: **Set ✅** (applied to all videos)\n\n"

    if not any([
        settings.get('find_name'), settings.get('find_cap'),
        settings.get('extra_cap'), cap_rules, fname_rules,
        dest_topic_id, settings.get('thumbnail_set'),
    ]):
        settings_text += "⚠️ No modifications set\n\n"

    keyboard = _markup([
        [
            _cb("🔙 Back to Settings", f"back_{session_id}"),
            _cb("✅ Confirm & Start",  f"start_{session_id}"),
        ],
        [
            _cb("🗑️ Clear All Settings", f"clear_{session_id}"),
            _cb("❌ Cancel",              f"cancel_{session_id}"),
        ],
    ])
    return settings_text, keyboard


def get_skip_keyboard(session_id) -> types.ReplyMarkupInlineKeyboard:
    """Skip option keyboard."""
    return _markup([
        [_cb("⏭️ Skip",   f"skip_{session_id}")],
        [_cb("❌ Cancel", f"cancel_{session_id}")],
    ])


def get_progress_keyboard() -> types.ReplyMarkupInlineKeyboard:
    """Keyboard shown during active transfer."""
    return _markup([
        [_cb("🛑 Stop Transfer", "stop_transfer")],
    ])


def get_clone_info_keyboard() -> types.ReplyMarkupInlineKeyboard:
    """Info keyboard shown with /clone command."""
    return _markup([
        [_cb("ℹ️ How to use?", "clone_help")],
        [_cb("📊 Bot Stats",   "bot_stats")],
    ])


def make_url_button(text: str, url: str) -> types.ReplyMarkupInlineKeyboard:
    """Single URL button markup (used by the cross-bot purchase message)."""
    return _markup([[_url_btn(text, url)]])
