"""
utils.py  –  Pure helpers + TDLib (pytdbot) message inspection utilities.

v7.0 (Pyrofork → TDLib):
  • All message attribute access rewritten for pytdbot.types.Message, whose
    payload lives in message.content (MessageText / MessagePhoto /
    MessageVideo / MessageDocument / MessageAudio / MessageVoiceNote /
    MessageVideoNote / MessageAnimation / MessageSticker / …).
  • formatted_text_to_html() converts TDLib FormattedText (UTF-16 offsets!)
    to HTML so caption find/replace + resend via parse_mode="html" behaves
    exactly like Pyrogram's message.caption.html did.
"""

import os
import re
import unicodedata


def human_readable_size(size):
    """Convert bytes to human readable format."""
    if not size:
        return "0B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.2f}{unit}"
        size /= 1024.0
    return f"{size:.2f}TB"


def time_formatter(seconds):
    """Convert seconds to formatted time string."""
    if seconds is None or seconds < 0:
        return "..."
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes   = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    return f"{minutes}m {seconds}s"


# ── UNICODE-AWARE REPLACEMENT ──────────────────────────────────────────────────

def smart_replace(original_text, find_str, replace_str):
    """
    Replace text while handling fancy Unicode fonts (bold, italic, math, etc.).

    Telegram captions often use Unicode math chars like 𝙀𝙭𝙩𝙧𝙖𝙘𝙩𝙚𝙙 𝐊𝐔𝐍𝐀𝐋.
    Normal str.replace() fails because code points differ from ASCII.
    NFKC normalization maps each fancy Unicode letter to its ASCII equivalent.
    """
    if not find_str or original_text is None:
        return original_text or ""

    if find_str in original_text:
        return original_text.replace(find_str, replace_str)

    norm_original = unicodedata.normalize('NFKC', original_text)
    norm_find     = unicodedata.normalize('NFKC', find_str)

    if norm_find not in norm_original:
        return original_text

    norm_to_orig_start = []
    norm_to_orig_end   = []

    for orig_i, orig_char in enumerate(original_text):
        norm_chars = unicodedata.normalize('NFKC', orig_char)
        for _ in norm_chars:
            norm_to_orig_start.append(orig_i)
            norm_to_orig_end.append(orig_i + 1)

    norm_to_orig_start.append(len(original_text))
    norm_to_orig_end.append(len(original_text))

    result        = []
    norm_i        = 0
    prev_orig_end = 0
    find_len      = len(norm_find)
    norm_len      = len(norm_original)

    while norm_i <= norm_len - find_len:
        if norm_original[norm_i:norm_i + find_len] == norm_find:
            orig_start = norm_to_orig_start[norm_i]
            orig_end   = norm_to_orig_end[norm_i + find_len - 1]
            result.append(original_text[prev_orig_end:orig_start])
            result.append(replace_str)
            prev_orig_end = orig_end
            norm_i       += find_len
        else:
            norm_i += 1

    result.append(original_text[prev_orig_end:])
    return ''.join(result)


# ── LINK PARSING ──────────────────────────────────────────────────────────────

_TG_DOMAIN = r'(?:t\.me|telegram\.me|telegram\.dog)'


def extract_link_info(link):
    """
    Extract (source_identifier, message_id, topic_id) from a Telegram link.
    topic_id is None for non-topic messages.

    TDLib uses the exact same message IDs as t.me links, and private chat
    links t.me/c/CHATID/… map to TDLib chat_id -100CHATID — identical to
    Pyrogram, so this parser is unchanged.

    Supported formats:
      Private regular:  t.me/c/CHATID/MSGID          → (-100CHATID, MSGID, None)
      Private topic:    t.me/c/CHATID/TOPICID/MSGID  → (-100CHATID, MSGID, TOPICID)
      Public regular:   t.me/USERNAME/MSGID          → ('username', MSGID, None)
      Public topic:     t.me/USERNAME/TOPICID/MSGID  → ('username', MSGID, TOPICID)
    """
    if not link:
        return None, None, None

    link = link.strip()

    private_topic = re.search(_TG_DOMAIN + r'/c/(\d+)/(\d+)/(\d+)', link)
    if private_topic:
        topic_id = int(private_topic.group(2))
        msg_id   = int(private_topic.group(3))
        return int(f"-100{private_topic.group(1)}"), msg_id, topic_id

    private_regular = re.search(_TG_DOMAIN + r'/c/(\d+)/(\d+)', link)
    if private_regular:
        return int(f"-100{private_regular.group(1)}"), int(private_regular.group(2)), None

    public_topic = re.search(_TG_DOMAIN + r'/([a-zA-Z0-9_]+)/(\d+)/(\d+)', link)
    if public_topic:
        username = public_topic.group(1)
        if username.lower() != 'c':
            return username, int(public_topic.group(3)), int(public_topic.group(2))

    public_regular = re.search(_TG_DOMAIN + r'/([a-zA-Z0-9_]+)/(\d+)', link)
    if public_regular:
        username = public_regular.group(1)
        if username.lower() != 'c':
            return username, int(public_regular.group(2)), None

    return None, None, None


# ── TDLIB FORMATTED TEXT → HTML ───────────────────────────────────────────────
#
# TDLib TextEntity offset/length are counted in UTF-16 code UNITS, not Python
# code points. We first split the text into (utf16_index → char) maps and then
# emit HTML tags per entity, handling nesting by sorting opens/closes.
#
_ENTITY_OPEN = {
    'textEntityTypeBold':          '<b>',
    'textEntityTypeItalic':        '<i>',
    'textEntityTypeUnderline':     '<u>',
    'textEntityTypeStrikethrough': '<s>',
    'textEntityTypeSpoiler':       '<tg-spoiler>',
    'textEntityTypeCode':          '<code>',
    'textEntityTypeBlockQuote':    '<blockquote>',
}
_ENTITY_CLOSE = {
    'textEntityTypeBold':          '</b>',
    'textEntityTypeItalic':        '</i>',
    'textEntityTypeUnderline':     '</u>',
    'textEntityTypeStrikethrough': '</s>',
    'textEntityTypeSpoiler':       '</tg-spoiler>',
    'textEntityTypeCode':          '</code>',
    'textEntityTypeBlockQuote':    '</blockquote>',
}


def _html_escape(s: str) -> str:
    return (s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'))


def _utf16_boundaries(text: str):
    """Return list mapping utf16 offset → python char index boundary."""
    boundaries = [0]
    acc = 0
    for i, ch in enumerate(text):
        acc += 2 if ord(ch) > 0xFFFF else 1
        boundaries.append(i + 1)
        # boundaries[utf16_offset] is only exact at char edges; we index via dict
    # Build a dict utf16_offset → char_index for exact entity edges
    mapping = {}
    off = 0
    mapping[0] = 0
    for i, ch in enumerate(text):
        off += 2 if ord(ch) > 0xFFFF else 1
        mapping[off] = i + 1
    return mapping


def formatted_text_to_html(ft) -> str:
    """
    Convert a TDLib FormattedText (or plain string) to an HTML string suitable
    for re-sending with parse_mode="html". Mirrors Pyrogram's .html behaviour
    closely enough for caption/text manipulation + faithful re-upload.
    """
    if ft is None:
        return ""
    text     = getattr(ft, 'text', None)
    entities = getattr(ft, 'entities', None) or []
    if text is None:
        return _html_escape(str(ft))
    if not entities:
        return _html_escape(text)

    mapping = _utf16_boundaries(text)

    # Collect tag events: position → list of (order, tag)
    opens  = {}
    closes = {}
    for i, ent in enumerate(entities):
        etype = getattr(ent, 'type', None)
        tname = getattr(etype, 'ID', None) or getattr(etype, 'getType', lambda: '')()
        if callable(tname):
            tname = tname()
        start = mapping.get(getattr(ent, 'offset', 0))
        end   = mapping.get(getattr(ent, 'offset', 0) + getattr(ent, 'length', 0))
        if start is None or end is None or start >= end:
            continue

        if tname == 'textEntityTypeTextUrl':
            url = getattr(etype, 'url', '') or ''
            o, c = f'<a href="{_html_escape(url)}">', '</a>'
        elif tname == 'textEntityTypeMentionName':
            uid = getattr(etype, 'user_id', 0)
            o, c = f'<a href="tg://user?id={uid}">', '</a>'
        elif tname == 'textEntityTypePre':
            o, c = '<pre>', '</pre>'
        elif tname == 'textEntityTypePreCode':
            o, c = '<pre>', '</pre>'
        elif tname == 'textEntityTypeCustomEmoji':
            eid = getattr(etype, 'custom_emoji_id', 0)
            inner = _html_escape(text[start:end])
            # Emit immediately as a self-contained tag pair
            o, c = f'<tg-emoji emoji-id="{eid}">', '</tg-emoji>'
        elif tname == 'textEntityTypeExpandableBlockQuote':
            o, c = '<blockquote>', '</blockquote>'
        elif tname in _ENTITY_OPEN:
            o, c = _ENTITY_OPEN[tname], _ENTITY_CLOSE[tname]
        else:
            continue  # mentions/hashtags/urls/phone numbers need no tag

        opens.setdefault(start, []).append(o)
        closes.setdefault(end, []).insert(0, c)

    out = []
    for i in range(len(text) + 1):
        if i in closes:
            out.extend(closes[i])
        if i in opens:
            out.extend(opens[i])
        if i < len(text):
            out.append(_html_escape(text[i]))
    return ''.join(out)


def message_formatted_text(message):
    """Return the TDLib FormattedText of a message (text or caption)."""
    content = getattr(message, 'content', None)
    if content is None:
        return None
    if getattr(content, 'ID', getattr(content, 'type', '')) == 'messageText' or \
       content.__class__.__name__ == 'MessageText':
        return getattr(content, 'text', None)
    return getattr(content, 'caption', None)


def message_plain_text(message) -> str:
    """Plain text (or caption) of a pytdbot Message."""
    ft = message_formatted_text(message)
    if ft is None:
        return ""
    return getattr(ft, 'text', None) or str(ft)


def message_html_text(message) -> str:
    """HTML-formatted text/caption — the TDLib equivalent of Pyrogram's .html."""
    return formatted_text_to_html(message_formatted_text(message))


# ── TDLIB MEDIA HELPERS ───────────────────────────────────────────────────────

def _cname(obj) -> str:
    return obj.__class__.__name__ if obj is not None else ''


_SPECIAL_CONTENT = {
    'MessagePoll', 'MessageLocation', 'MessageVenue', 'MessageContact',
    'MessageDice', 'MessageGame', 'MessageInvoice', 'MessagePaidMedia',
    'MessageGiveaway', 'MessageGiveawayWinners', 'MessageStory',
    'MessageCall', 'MessageVideoChatStarted', 'MessageVideoChatEnded',
    'MessageVideoChatScheduled', 'MessageInviteVideoChatParticipants',
    'MessagePassportDataSent', 'MessagePassportDataReceived',
    'MessagePaymentSuccessful', 'MessageGiftedPremium',
}


def _content(message):
    return getattr(message, 'content', None)


def is_service_message(message) -> bool:
    """TDLib equivalent of Pyrogram's message.service — nothing to transfer."""
    c = _cname(_content(message))
    return c in {
        'MessagePinMessage', 'MessageChatJoinByLink', 'MessageChatJoinByRequest',
        'MessageChatAddMembers', 'MessageChatDeleteMember',
        'MessageChatChangeTitle', 'MessageChatChangePhoto',
        'MessageChatSetTheme', 'MessageChatSetBackground',
        'MessageChatDeletePhoto', 'MessageGroupCall', 'MessageHeaderDate',
        'MessageTopicCreated', 'MessageTopicEdited', 'MessageTopicClosed',
        'MessageTopicReopened', 'MessageForumTopicCreated',
        'MessageForumTopicEdited', 'MessageForumTopicIsClosedToggled',
        'MessageForumTopicIsHiddenToggled', 'MessageSuggestProfilePhoto',
        'MessageBotWriteAccessAllowed', 'MessageWebAppDataSent',
        'MessageWebAppDataReceived', 'MessageChatSetMessageAutoDeleteTime',
        'MessageChatBoost', 'MessageChatShared', 'MessageUserShared',
        'MessageChatUpgradeFrom', 'MessageChatUpgradeTo',
        'MessageProximityAlertTriggered', 'MessageCustomServiceAction',
        'MessageBasicGroupChatCreate', 'MessageSupergroupChatCreate',
        'MessageScreenshotTaken', 'MessageContactRegistered',
        'MessageExpiredPhoto', 'MessageExpiredVideo', 'MessageExpiredVideoNote',
        'MessageExpiredVoiceNote', 'MessageUnavailable', 'MessageAsyncStory',
        'MessageGiftedStars', 'MessageGiveawayCreated',
        'MessageGiveawayCompleted', 'MessagePaymentRefunded',
        'MessageUsersShared', 'MessageReport',
    }


def is_special_media(message) -> bool:
    """Media that cannot be downloaded as a file — must be re-created (copy)."""
    return _cname(_content(message)) in _SPECIAL_CONTENT


def _media_file(message):
    """Return the inner TDLib File object of the message's media, or None."""
    c = _content(message)
    name = _cname(c)
    if name == 'MessagePhoto':
        sizes = getattr(c.photo, 'sizes', None) or []
        if sizes:
            return getattr(sizes[-1], 'photo', None)
    elif name == 'MessageVideo':
        return getattr(c.video, 'video', None)
    elif name == 'MessageDocument':
        return getattr(c.document, 'document', None)
    elif name == 'MessageAudio':
        return getattr(c.audio, 'audio', None)
    elif name == 'MessageVoiceNote':
        return getattr(c.voice_note, 'voice', None)
    elif name == 'MessageVideoNote':
        return getattr(c.video_note, 'video', None)
    elif name == 'MessageAnimation':
        return getattr(c.animation, 'animation', None)
    elif name == 'MessageSticker':
        return getattr(c.sticker, 'sticker', None)
    return None


def _file_size(f) -> int:
    if f is None:
        return 0
    return getattr(f, 'size', 0) or getattr(f, 'expected_size', 0) or 0


def get_media_file_size(message) -> int:
    """File size in bytes for any downloadable TDLib media message."""
    return _file_size(_media_file(message))


def get_video_metadata(message):
    """
    Resolve (duration, width, height, thumb_remote_file_id) for a video-mode
    message — MessageVideo / MessageAnimation / MessageVideoNote, or a
    video-like MessageDocument (mkv/avi — has a thumbnail but no duration).
    """
    duration, width, height = 0, 0, 0
    thumb_remote_id = None
    c    = _content(message)
    name = _cname(c)

    media_obj = None
    if name == 'MessageVideo':
        media_obj = c.video
    elif name == 'MessageAnimation':
        media_obj = c.animation
    elif name == 'MessageVideoNote':
        media_obj = c.video_note

    if media_obj is not None:
        duration = getattr(media_obj, 'duration', 0) or 0
        width    = getattr(media_obj, 'width', 0) or getattr(media_obj, 'length', 0) or 0
        height   = getattr(media_obj, 'height', 0) or getattr(media_obj, 'length', 0) or 0
        thumb    = getattr(media_obj, 'thumbnail', None)
        if thumb is not None:
            tfile = getattr(thumb, 'file', None)
            remote = getattr(tfile, 'remote', None)
            thumb_remote_id = getattr(remote, 'id', None)
    elif name == 'MessageDocument':
        thumb = getattr(c.document, 'thumbnail', None)
        if thumb is not None:
            tfile = getattr(thumb, 'file', None)
            remote = getattr(tfile, 'remote', None)
            thumb_remote_id = getattr(remote, 'id', None)

    return duration, width, height, thumb_remote_id


def get_target_info(message):
    """
    Smart format detection for TDLib messages.
    Returns (filename, mime_type, is_video_mode).
    """
    if is_special_media(message):
        return None, None, False

    c    = _content(message)
    name = _cname(c)
    mid  = getattr(message, 'id', 0)

    if name == 'MessagePhoto':
        return f"Image_{mid}.jpg", "image/jpeg", False

    if name == 'MessageSticker':
        s    = c.sticker
        mime = getattr(getattr(s, 'format', None), '__class__', type('x', (), {})).__name__
        fmt  = _cname(getattr(s, 'format', None))
        ext  = '.webp'
        if 'Tgs' in fmt:   ext = '.tgs'
        elif 'Webm' in fmt: ext = '.webm'
        return f"Sticker_{mid}{ext}", 'image/webp', False

    if name == 'MessageVideoNote':
        return f"VideoNote_{mid}.mp4", "video/mp4", True

    if name == 'MessageAnimation':
        # GIFs/animations are mp4 — send as streamable video so they play
        # inline at the destination instead of arriving as bare documents.
        return f"Animation_{mid}.mp4", "video/mp4", True

    if name == 'MessageVoiceNote':
        return f"Voice_{mid}.ogg", "audio/ogg", False

    if name not in ('MessageVideo', 'MessageAudio', 'MessageDocument'):
        return None, None, False

    media_obj = c.video if name == 'MessageVideo' else (c.audio if name == 'MessageAudio' else c.document)
    mime          = getattr(media_obj, 'mime_type', '') or ''
    original_name = getattr(media_obj, 'file_name', '') or f"File_{mid}"
    base_name     = os.path.splitext(original_name)[0]

    # ── Video ─────────────────────────────────────────────────────────────
    if name == 'MessageVideo' or 'video' in mime or original_name.lower().endswith(
        ('.mkv', '.avi', '.webm', '.mov', '.flv', '.wmv', '.m4v', '.3gp', '.ts')
    ):
        return base_name + ".mp4", "video/mp4", True

    # ── Audio ─────────────────────────────────────────────────────────────
    if name == 'MessageAudio' or 'audio' in mime:
        ext = os.path.splitext(original_name)[1] or '.mp3'
        return base_name + ext, mime or 'audio/mpeg', False

    # ── Image document ────────────────────────────────────────────────────
    if 'image' in mime:
        ext = os.path.splitext(original_name)[1] or '.jpg'
        return base_name + ext, mime, False

    # ── PDF ───────────────────────────────────────────────────────────────
    if 'pdf' in mime or original_name.lower().endswith('.pdf'):
        return base_name + ".pdf", "application/pdf", False

    # ── Fallback: keep original name and mime ─────────────────────────────
    return original_name, mime or "application/octet-stream", False


# ── MANIPULATION HELPERS ──────────────────────────────────────────────────────

def apply_filename_manipulations(filename, settings):
    """Apply find/replace rules on a filename."""
    if not settings:
        return filename

    if 'find_name' in settings and 'replace_name' in settings:
        filename = smart_replace(filename, settings['find_name'], settings['replace_name'])

    for find_str, replace_str in settings.get('fname_rules', []):
        if find_str:
            filename = smart_replace(filename, find_str, replace_str)

    return filename


def apply_caption_manipulations(message, settings):
    """Apply caption find/replace/remove/append rules, preserving HTML entities/links."""
    original_caption = message_html_text(message)

    if not settings:
        return original_caption

    caption = original_caption

    if 'find_cap' in settings and 'replace_cap' in settings:
        caption = smart_replace(caption, settings['find_cap'], settings['replace_cap'])

    for find_str, replace_str in settings.get('cap_rules', []):
        if find_str:
            caption = smart_replace(caption, find_str, replace_str)

    if settings.get('extra_cap'):
        caption = f"{caption}\n\n{settings['extra_cap']}" if caption else settings['extra_cap']

    return caption


def sanitize_filename(filename):
    """Remove characters that are invalid in filenames."""
    for char in '<>:"/\\|?*':
        filename = filename.replace(char, '_')
    return filename
