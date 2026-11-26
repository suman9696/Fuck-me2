import asyncio
import json
import os
import re
import time
import secrets
from os import environ
from urllib.parse import urlparse
import threading

# Install uvloop before creating any WZGram clients (Linux/Heroku only).
try:
    import uvloop
    uvloop.install()
    print("uvloop enabled")
except ImportError:
    print("uvloop unavailable; using the default asyncio event loop")

from wzgram import Client, filters
from pyrogram.errors import (
    FloodWait,
    InviteHashExpired,
    UserAlreadyParticipant,
    UsernameNotOccupied,
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOT_TOKEN = environ["TOKEN"]
API_HASH = environ["HASH"]
API_ID = int(environ["ID"])
SESSION_STRING = environ.get("STRING", "").strip()

# WZGram has its own parallel media transfer engine. This setting caps
# simultaneous transfers; it does not directly set per-file chunk workers.
MAX_CONCURRENT_TRANSMISSIONS = max(
    1, min(16, int(environ.get("MAX_CONCURRENT_TRANSMISSIONS", "16")))
)

bot = Client(
    "mybot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    max_concurrent_transmissions=MAX_CONCURRENT_TRANSMISSIONS,
)

acc = (
    Client(
        "myacc",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=SESSION_STRING,
        max_concurrent_transmissions=MAX_CONCURRENT_TRANSMISSIONS,
    )
    if SESSION_STRING
    else None
)

OWNER_ID = int(environ.get("OWNER_ID", "0"))
TARGET_CHAT_ID = environ.get("TARGET_CHAT_ID", "").strip()
AUTO_UPLOAD = True
TARGET_CONFIG_FILE = "target_config.json"

# Keep jobs sequential to avoid exhausting the dyno or Telegram connections.
_job_lock = asyncio.Lock()
_progress_last_edit = {}

_PENDING_FILTERS = {}
_MEDIA_FILTERS = ("Video", "Document", "Photo", "Audio", "Voice",
                  "Animation", "Sticker", "Text")


def _filter_keyboard(key, selected):
    rows, row = [], []
    for media_type in _MEDIA_FILTERS:
        mark = "✅" if media_type in selected else "▫️"
        row.append(InlineKeyboardButton(
            f"{mark} {media_type}",
            callback_data=f"mf:{key}:{media_type.lower()}",
        ))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Done", callback_data=f"mf:{key}:done")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"mf:{key}:cancel")])
    return InlineKeyboardMarkup(rows)


def _chat_reference_from_input(value):
    value = value.strip().rstrip("/")
    if value.startswith("@"):
        return value
    if value.startswith(("https://t.me/", "http://t.me/")):
        parts = [part for part in urlparse(value).path.split("/") if part]
        if len(parts) >= 2 and parts[0] == "c" and parts[1].isdigit():
            return int("-100" + parts[1])
        if len(parts) >= 2 and parts[0] == "b":
            return parts[1]
        if parts and parts[0] not in ("+", "joinchat"):
            return parts[0]
        return None
    return value


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def safe_call(function, *args, **kwargs):
    """Await a WZGram API call and retry Telegram FloodWait responses."""
    retries = 0
    while True:
        try:
            return await function(*args, **kwargs)
        except FloodWait as error:
            retries += 1
            wait_seconds = int(getattr(error, "value", getattr(error, "x", 1)))
            print(f"FloodWait: sleeping {wait_seconds + 1}s ({retries}/5)")
            await asyncio.sleep(wait_seconds + 1)
            if retries >= 5:
                raise


def _load_target_config():
    global TARGET_CHAT_ID, AUTO_UPLOAD
    try:
        with open(TARGET_CONFIG_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
        TARGET_CHAT_ID = str(data.get("target_chat_id") or TARGET_CHAT_ID).strip()
        AUTO_UPLOAD = bool(data.get("auto_upload", AUTO_UPLOAD))
    except (OSError, ValueError, TypeError):
        pass


def _save_target_config():
    with open(TARGET_CONFIG_FILE, "w", encoding="utf-8") as file:
        json.dump(
            {"target_chat_id": TARGET_CHAT_ID, "auto_upload": AUTO_UPLOAD},
            file,
        )


_load_target_config()


def _destinations(private_chat_id):
    destinations = [private_chat_id]
    if AUTO_UPLOAD and TARGET_CHAT_ID:
        target = (
            int(TARGET_CHAT_ID)
            if TARGET_CHAT_ID.lstrip("-").isdigit()
            else TARGET_CHAT_ID
        )
        if target != private_chat_id:
            destinations.append(target)
    return destinations


async def _send_copy_to_destinations(
    source_chat_id, source_message_id, private_chat_id, reply_to_id=None
):
    for destination in _destinations(private_chat_id):
        kwargs = {}
        if destination == private_chat_id and reply_to_id:
            kwargs["reply_to_message_id"] = reply_to_id
        await safe_call(
            bot.copy_message,
            destination,
            source_chat_id,
            source_message_id,
            **kwargs,
        )


def _get_message_type(message):
    for attribute, label in (
        ("document", "Document"),
        ("video", "Video"),
        ("animation", "Animation"),
        ("sticker", "Sticker"),
        ("voice", "Voice"),
        ("audio", "Audio"),
        ("photo", "Photo"),
    ):
        if getattr(message, attribute, None):
            return label
    if getattr(message, "text", None):
        return "Text"
    return None


def _parse_post_link(text):
    """Return (chat reference, first message ID, last message ID, link type)."""
    cleaned = text.strip()
    # Accept both compact ranges (1831-1836) and spaced ranges (1831 - 1836).
    match = re.search(r"https?://t\.me/[^\s]+(?:\s*-\s*\d+)?", cleaned)
    if not match:
        return None

    url = match.group(0).rstrip(".,)")
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]

    if len(parts) >= 3 and parts[0] == "c":
        chat_ref = int("-100" + parts[1])
        message_part = parts[2]
    elif len(parts) >= 3 and parts[0] == "b":
        chat_ref = parts[1]
        message_part = parts[2]
    elif len(parts) >= 2:
        chat_ref = parts[0]
        message_part = parts[1]
    else:
        return None

    # Accept compact ranges (1874-1878) and spaced ranges (1874 - 1878).
    # The spaced form can be included in the parsed URL path, so trim and
    # validate both sides instead of relying on a separate suffix match.
    message_part = message_part.replace("?single", "").strip()
    range_match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", message_part)
    if range_match:
        first, last = range_match.groups()
    elif message_part.isdigit():
        first = last = message_part
    else:
        return None

    return chat_ref, int(first), int(last), parts[0]


def _is_private_link(text):
    return "https://t.me/+" in text or "https://t.me/joinchat/" in text


def _target_value():
    return (
        int(TARGET_CHAT_ID)
        if TARGET_CHAT_ID.lstrip("-").isdigit()
        else TARGET_CHAT_ID
    )


class TransferProgress:
    """Thread-safe progress callback for WZGram transfer worker threads."""

    def __init__(self, status_message, label):
        self.status_message = status_message
        self.label = label
        self.loop = asyncio.get_running_loop()
        self.last_time = 0.0
        self.last_bytes = 0
        self.speed = 0.0
        self.last_edit = 0.0
        self._callback_lock = threading.Lock()
        self._last_scheduled = 0.0

    def __call__(self, current, total):
        # WZGram may call this from transfer worker threads. Throttle BEFORE
        # scheduling to the event loop so thousands of chunks cannot create
        # a backlog of progress-update coroutines.
        if self.loop.is_closed():
            return

        now = time.monotonic()
        with self._callback_lock:
            is_final = bool(total and current >= total)
            if not is_final and now - self._last_scheduled < 1.0:
                return
            self._last_scheduled = now

        try:
            future = asyncio.run_coroutine_threadsafe(
                self._update(current, total), self.loop
            )
            # Do not wait on the future from the transfer worker thread.
            future.add_done_callback(
                lambda f: f.exception() if not f.cancelled() else None
            )
        except RuntimeError:
            pass

    async def _update(self, current, total):
        if not total:
            return

        now = time.monotonic()
        elapsed = now - self.last_time
        if elapsed >= 0.5:
            instant = max(0, current - self.last_bytes) / elapsed
            self.speed = instant if not self.speed else (
                0.7 * self.speed + 0.3 * instant
            )
            self.last_time = now
            self.last_bytes = current

        if now - self.last_edit < 1.2 and current < total:
            return
        self.last_edit = now

        percent = min(100.0, current * 100.0 / total)
        filled = int(percent // 10)
        bar = "█" * filled + "░" * (10 - filled)
        speed_text = (
            f"{self.speed / (1024 * 1024):.2f} MB/s"
            if self.speed > 0
            else "Calculating…"
        )
        remaining = max(0, total - current)
        eta = int(remaining / self.speed) if self.speed > 0 else 0
        eta_text = f"{eta // 60:02d}:{eta % 60:02d}" if self.speed > 0 else "Calculating…"

        details = (
            f"{self.label}\n"
            f"`{bar}` **{percent:.1f}%**\n"
            f"**Size:** {current / 1048576:.1f} / {total / 1048576:.1f} MB\n"
            f"**Speed:** {speed_text}\n"
            f"**ETA:** {eta_text}"
        )
        try:
            await safe_call(
                bot.edit_message_text,
                self.status_message.chat.id,
                self.status_message.id,
                details,
            )
        except Exception:
            # Ignore unchanged-message and transient edit errors.
            pass


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
@bot.on_message(filters.command(["start"]))
async def send_start(client, message):
    await safe_call(
        bot.send_message,
        message.chat.id,
        f"**♻️ 𝙷𝚒** **{message.from_user.mention}**, "
        "**𝐈 𝐚𝐦 𝐒𝐚𝐯𝐞 𝐑𝐞𝐬𝐭𝐫𝐢𝐜𝐭𝐞𝐝 𝐁𝐨𝐭, 𝐈 𝐜𝐚𝐧 𝐬𝐞𝐧𝐝 𝐲𝐨𝐮 "
        "𝐫𝐞𝐬𝐭𝐫𝐢𝐜𝐭𝐞𝐝 𝐜𝐨𝐧𝐭𝐞𝐧𝐭 𝐛𝐲 𝐢𝐭'𝐬 𝐩𝐨𝐬𝐭 𝐥𝐢𝐧𝐤**\n\n"
        "Send a Telegram post link to get started.\n\n"
        ,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton(
                "☢️ 𝐁𝐨𝐭 𝐃𝐞𝐯𝐞𝐥𝐨𝐩𝐞𝐫",
                url="tg://openmessage?user_id=2112247533",
            )]]
        ),
        reply_to_message_id=message.id,
    )


@bot.on_message(filters.command(["settarget"]) & filters.private)
async def set_target_command(client, message):
    global TARGET_CHAT_ID
    if len(message.command) < 2:
        return await safe_call(
            bot.send_message,
            message.chat.id,
            "Usage: `/settarget -1001234567890` or `/settarget @channelusername`",
        )

    value = message.command[1].strip()
    try:
        candidate = str(int(value)) if value.lstrip("-").isdigit() else value
        chat = await safe_call(
            bot.get_chat,
            int(candidate) if candidate.lstrip("-").isdigit() else candidate,
        )
        TARGET_CHAT_ID = candidate
        _save_target_config()
        await safe_call(
            bot.send_message,
            message.chat.id,
            f"✅ Target set to **{chat.title or chat.first_name or TARGET_CHAT_ID}** "
            f"(`{TARGET_CHAT_ID}`).",
        )
    except Exception as error:
        await safe_call(
            bot.send_message,
            message.chat.id,
            f"❌ Could not access that chat: `{str(error)[:400]}`\n"
            "Add the bot to the group/channel first and give it posting permission.",
        )


@bot.on_message(filters.command(["removetarget"]) & filters.private)
async def remove_target_command(client, message):
    global TARGET_CHAT_ID, AUTO_UPLOAD
    if not TARGET_CHAT_ID:
        return await safe_call(
            bot.send_message, message.chat.id, "ℹ️ No target is currently configured."
        )

    old_target = TARGET_CHAT_ID
    TARGET_CHAT_ID = ""
    AUTO_UPLOAD = False
    _save_target_config()
    await safe_call(
        bot.send_message,
        message.chat.id,
        f"✅ Target `{old_target}` removed. Auto upload is now **OFF**.",
    )


@bot.on_message(filters.command(["mytargets"]) & filters.private)
async def my_targets_command(client, message):
    status = "ON" if AUTO_UPLOAD else "OFF"
    await safe_call(
        bot.send_message,
        message.chat.id,
        f"**Target:** `{TARGET_CHAT_ID or 'Not set'}`\n**Auto upload:** `{status}`",
    )


@bot.on_message(filters.command(["autoupload"]) & filters.private)
async def auto_upload_command(client, message):
    global AUTO_UPLOAD
    if len(message.command) < 2 or message.command[1].lower() not in ("on", "off"):
        return await safe_call(
            bot.send_message,
            message.chat.id,
            "Usage: `/autoupload on` or `/autoupload off`",
        )
    if message.command[1].lower() == "on" and not TARGET_CHAT_ID:
        return await safe_call(
            bot.send_message,
            message.chat.id,
            "❌ Set a target first using `/settarget`.",
        )

    AUTO_UPLOAD = message.command[1].lower() == "on"
    _save_target_config()
    await safe_call(
        bot.send_message,
        message.chat.id,
        f"✅ Auto upload **{'ON' if AUTO_UPLOAD else 'OFF'}**.",
    )


# ---------------------------------------------------------------------------
# Help, recent-post lookup, and media-filter callbacks
# ---------------------------------------------------------------------------
@bot.on_message(filters.command(["h", "help"]))
async def help_command(client, message):
    help_text = USAGE + """**🛠️ Available Commands**

/start - Show the welcome message.
/h or /help - Show usage instructions and all commands.
/recent @groupusername - Get the latest post link from a public group/channel.
/settarget <chat_id or @username> - Set the destination chat for auto-upload.
/removetarget - Remove the configured destination and turn auto-upload off.
/mytargets - Show the configured destination and auto-upload status.
/autoupload on|off - Enable or disable automatic uploads to the destination.

**Examples**
`/settarget @mychannel`
`/autoupload on`
`/recent @publicgroup`
"""
    await safe_call(
        bot.send_message,
        message.chat.id,
        help_text,
        reply_to_message_id=message.id,
    )


@bot.on_message(filters.command(["recent"]))
async def recent_command(client, message):
    if len(message.command) < 2:
        return await safe_call(
            bot.send_message, message.chat.id,
            "Usage: `/recent @groupusername` or `/recent https://t.me/groupusername`",
            reply_to_message_id=message.id,
        )

    chat_ref = _chat_reference_from_input(message.command[1])
    if chat_ref is None:
        return await safe_call(bot.send_message, message.chat.id, "❌ Invalid group/channel link.")

    source_client = bot
    try:
        chat = await safe_call(bot.get_chat, chat_ref)
    except Exception:
        if acc is None:
            return await safe_call(
                bot.send_message, message.chat.id,
                "❌ I cannot access this group. For private groups, configure STRING "
                "and make sure the user session has joined the group.",
                reply_to_message_id=message.id,
            )
        source_client = acc
        try:
            chat = await safe_call(acc.get_chat, chat_ref)
        except Exception as error:
            return await safe_call(
                bot.send_message, message.chat.id,
                f"❌ Could not access the group: `{str(error)[:300]}`",
                reply_to_message_id=message.id,
            )

    try:
        latest = None
        async for item in source_client.get_chat_history(chat.id, limit=50):
            if (
                item
                and not getattr(item, "empty", False)
                and (item.text or item.caption or _get_message_type(item))
            ):
                latest = item
                break
        if latest is None:
            return await safe_call(bot.send_message, message.chat.id, "No recent post was found.")

        if getattr(chat, "username", None):
            post_link = f"https://t.me/{chat.username}/{latest.id}"
        else:
            raw_id = str(chat.id)
            internal_id = raw_id[4:] if raw_id.startswith("-100") else raw_id
            post_link = f"https://t.me/c/{internal_id}/{latest.id}"

        await safe_call(
            bot.send_message, message.chat.id,
            f"**Latest post in {chat.title or chat.first_name or 'group'}:**\n{post_link}",
            reply_to_message_id=message.id, disable_web_page_preview=True,
        )
    except Exception as error:
        await safe_call(
            bot.send_message, message.chat.id,
            f"❌ Could not find the latest post: `{str(error)[:300]}`",
            reply_to_message_id=message.id,
        )


@bot.on_callback_query(filters.regex(r"^mf:"))
async def media_filter_callback(client, query):
    try:
        _, key_text, choice = query.data.split(":", 2)
        state_key = (query.message.chat.id, key_text)
        state = _PENDING_FILTERS.get(state_key)
        prompt_id = query.message.id
        if not state:
            return await query.answer("This selection has expired.", show_alert=True)
        if query.from_user.id != state["owner_id"]:
            return await query.answer("This filter belongs to another user.", show_alert=True)

        if choice == "cancel":
            _PENDING_FILTERS.pop(state_key, None)
            # Delete the selection prompt when the user cancels.
            try:
                await safe_call(
                    bot.delete_messages,
                    query.message.chat.id,
                    prompt_id,
                )
            except Exception:
                await safe_call(
                    bot.edit_message_reply_markup,
                    query.message.chat.id,
                    prompt_id,
                    reply_markup=None,
                )
            await query.answer("Cancelled.")
            await safe_call(
                bot.send_message,
                query.message.chat.id,
                "❌ Media selection cancelled.",
                reply_to_message_id=state["original_message"].id,
            )
            return

        if choice == "done":
            if not state["selected"]:
                return await query.answer("Select at least one media type.", show_alert=True)
            _PENDING_FILTERS.pop(state_key, None)
            # Remove the whole selection prompt after the user confirms.
            try:
                await safe_call(
                    bot.delete_messages,
                    query.message.chat.id,
                    prompt_id,
                )
            except Exception:
                # If deletion is unavailable, at least remove the buttons.
                await safe_call(
                    bot.edit_message_reply_markup,
                    query.message.chat.id,
                    prompt_id,
                    reply_markup=None,
                )
            await query.answer("Filter applied.")
            async with _job_lock:
                await _process_message(bot, state["original_message"], set(state["selected"]))
            return

        media_type = choice.capitalize()
        if media_type in _MEDIA_FILTERS:
            if media_type in state["selected"]:
                state["selected"].remove(media_type)
            else:
                state["selected"].add(media_type)
            # Hide the instruction text as soon as the user makes a
            # selection, while keeping the media buttons available.
            await safe_call(
                bot.edit_message_text,
                query.message.chat.id,
                prompt_id,
                "\u2063",
                reply_markup=_filter_keyboard(key_text, state["selected"]),
            )
        await query.answer()
    except Exception as error:
        print(f"Media filter callback failed: {error!r}")
        try:
            await query.answer("Could not update the filter.", show_alert=True)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Download / upload
# ---------------------------------------------------------------------------
async def handle_private(message, chat_ref, message_id, selected_types=None):
    try:
        source_message = await safe_call(acc.get_messages, chat_ref, message_id)
    except Exception as error:
        print(f"Skipping private post {message_id}: {error!r}")
        return
    if not source_message or getattr(source_message, "empty", False):
        print(f"Skipping missing private post {message_id}")
        return

    message_type = _get_message_type(source_message)
    if selected_types is not None and message_type not in selected_types:
        return

    if message_type == "Text":
        sent = await safe_call(
            bot.send_message,
            message.chat.id,
            source_message.text,
            entities=source_message.entities,
            reply_to_message_id=message.id,
        )
        if AUTO_UPLOAD and TARGET_CHAT_ID:
            target = _target_value()
            if target != message.chat.id:
                await safe_call(bot.copy_message, target, sent.chat.id, sent.id)
        return

    if message_type not in {
        "Document", "Video", "Animation", "Sticker", "Voice", "Audio", "Photo"
    }:
        # Silently skip service messages and posts without downloadable media.
        return

    status_message = await safe_call(
        bot.send_message,
        message.chat.id,
        "⏳ **Preparing download…**",
        reply_to_message_id=message.id,
    )

    download_progress = TransferProgress(status_message, "📥 **DOWNLOADING**")
    upload_progress = TransferProgress(status_message, "📤 **UPLOADING**")
    downloaded_file = None
    thumbnail = None
    sent = None

    try:
        downloaded_file = await safe_call(
            acc.download_media,
            source_message,
            progress=download_progress,
        )
        if not downloaded_file:
            return

        common = {"reply_to_message_id": message.id}
        caption = source_message.caption
        caption_entities = source_message.caption_entities

        if message_type == "Document":
            if source_message.document.thumbs:
                try:
                    thumbnail = await safe_call(
                        acc.download_media,
                        source_message.document.thumbs[0].file_id,
                    )
                except Exception:
                    thumbnail = None
            sent = await safe_call(
                bot.send_document,
                message.chat.id,
                downloaded_file,
                thumb=thumbnail,
                caption=caption,
                caption_entities=caption_entities,
                progress=upload_progress,
                **common,
            )

        elif message_type == "Video":
            if source_message.video.thumbs:
                try:
                    thumbnail = await safe_call(
                        acc.download_media,
                        source_message.video.thumbs[0].file_id,
                    )
                except Exception:
                    thumbnail = None
            sent = await safe_call(
                bot.send_video,
                message.chat.id,
                downloaded_file,
                duration=source_message.video.duration,
                width=source_message.video.width,
                height=source_message.video.height,
                thumb=thumbnail,
                caption=caption,
                caption_entities=caption_entities,
                progress=upload_progress,
                **common,
            )

        elif message_type == "Animation":
            sent = await safe_call(
                bot.send_animation,
                message.chat.id,
                downloaded_file,
                caption=caption,
                caption_entities=caption_entities,
                progress=upload_progress,
                **common,
            )

        elif message_type == "Sticker":
            sent = await safe_call(
                bot.send_sticker, message.chat.id, downloaded_file, **common
            )

        elif message_type == "Voice":
            sent = await safe_call(
                bot.send_voice,
                message.chat.id,
                downloaded_file,
                caption=caption,
                caption_entities=caption_entities,
                progress=upload_progress,
                **common,
            )

        elif message_type == "Audio":
            sent = await safe_call(
                bot.send_audio,
                message.chat.id,
                downloaded_file,
                caption=caption,
                caption_entities=caption_entities,
                progress=upload_progress,
                **common,
            )

        elif message_type == "Photo":
            sent = await safe_call(
                bot.send_photo,
                message.chat.id,
                downloaded_file,
                caption=caption,
                caption_entities=caption_entities,
                progress=upload_progress,
                **common,
            )

        if sent and AUTO_UPLOAD and TARGET_CHAT_ID:
            target = _target_value()
            if target != message.chat.id:
                await safe_call(bot.copy_message, target, sent.chat.id, sent.id)

    finally:
        for path in (thumbnail, downloaded_file):
            if path and isinstance(path, str) and os.path.isfile(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
        try:
            await safe_call(bot.delete_messages, message.chat.id, status_message.id)
        except Exception:
            pass


async def _process_message(client, message, selected_types=None):
    text = message.text or ""

    if _is_private_link(text):
        if acc is None:
            await safe_call(
                bot.send_message,
                message.chat.id,
                "**String Session is not Set**",
                reply_to_message_id=message.id,
            )
            return
        try:
            await safe_call(acc.join_chat, text.strip())
            await safe_call(
                bot.send_message,
                message.chat.id,
                "**Chat Joined**",
                reply_to_message_id=message.id,
            )
        except UserAlreadyParticipant:
            await safe_call(
                bot.send_message,
                message.chat.id,
                "**Chat already Joined**",
                reply_to_message_id=message.id,
            )
        except InviteHashExpired:
            await safe_call(
                bot.send_message,
                message.chat.id,
                "**Invalid Link**",
                reply_to_message_id=message.id,
            )
        except Exception as error:
            await safe_call(
                bot.send_message,
                message.chat.id,
                f"**Error:** __{error}__",
                reply_to_message_id=message.id,
            )
        return

    parsed = _parse_post_link(text)
    if not parsed:
        return

    chat_ref, first_id, last_id, link_kind = parsed

    if link_kind in ("c", "b") and acc is None:
        await safe_call(
            bot.send_message,
            message.chat.id,
            "**String Session is not Set**",
            reply_to_message_id=message.id,
        )
        return

    # Multi-post links first ask which media types should be included.
    if last_id > first_id and selected_types is None:
        # Use a unique callback key because Telegram assigns the prompt message
        # ID only after send_message completes. This avoids sending buttons with
        # a temporary/invalid ID and then relying on a second edit request.
        filter_key = secrets.token_hex(4)
        state_key = (message.chat.id, filter_key)
        _PENDING_FILTERS[state_key] = {
            "owner_id": message.from_user.id,
            "original_message": message,
            "selected": set(),
        }
        try:
            await safe_call(
                bot.send_message,
                message.chat.id,
                "Select the media types to process. You can choose multiple options, "
                "then press **Done**.",
                reply_to_message_id=message.id,
                reply_markup=_filter_keyboard(filter_key, set()),
            )
        except Exception:
            _PENDING_FILTERS.pop(state_key, None)
            raise
        return

    for message_id in range(first_id, last_id + 1):
        try:
            if link_kind in ("c", "b"):
                await handle_private(message, chat_ref, message_id, selected_types)
                continue

            try:
                source_message = await safe_call(bot.get_messages, chat_ref, message_id)
            except UsernameNotOccupied:
                await safe_call(
                    bot.send_message, message.chat.id,
                    "**The username is not occupied by anyone**",
                    reply_to_message_id=message.id,
                )
                return
            except Exception as error:
                print(f"Skipping post {message_id}: {error!r}")
                continue

            if not source_message or getattr(source_message, "empty", False):
                print(f"Skipping missing/deleted post {message_id}")
                continue

            message_type = _get_message_type(source_message)
            if selected_types is not None and message_type not in selected_types:
                continue

            try:
                await _send_copy_to_destinations(
                    source_message.chat.id, source_message.id,
                    message.chat.id, message.id,
                )
            except Exception as copy_error:
                if acc is None:
                    print(f"Public copy failed and no user session is configured: {copy_error!r}")
                    continue
                await handle_private(message, chat_ref, message_id, selected_types)
        except Exception as error:
            print(f"Skipping post {message_id} after error: {error!r}")
            continue


@bot.on_message(filters.text)
async def save(client, message):
    if not message.text or message.text.startswith("/"):
        return
    async with _job_lock:
        try:
            await _process_message(client, message)
        except FloodWait as error:
            wait_seconds = int(getattr(error, "value", getattr(error, "x", 1)))
            await asyncio.sleep(wait_seconds + 1)
            try:
                await _process_message(client, message)
            except Exception as retry_error:
                print(f"Task failed after FloodWait retry: {retry_error!r}")
        except Exception as error:
            print(f"Task failed: {error!r}")
            try:
                await safe_call(
                    bot.send_message,
                    message.chat.id,
                    f"❌ **Task failed:** `{str(error)[:700]}`",
                    reply_to_message_id=message.id,
                )
            except Exception as send_error:
                print(f"Could not report task error: {send_error!r}")


USAGE = """**🈂️ 𝐅𝐨𝐫 𝐏𝐮𝐛𝐥𝐢𝐜 𝐂𝐡𝐚𝐭𝐬 🈂️**
Send a public post link to save its content.

**🈂️ 𝐅𝐨𝐫 𝐏𝐫𝐢𝐯𝐚𝐭𝐞 𝐂𝐡𝐚𝐭𝐬 🈂️**
First send the invite link (if needed), then send the private post link.
The bot's user session must have access to the private group/channel.

**🈂️ 𝐌𝐮𝐥𝐭𝐢 𝐏𝐨𝐬𝐭𝐬 🈂️**
Send a range link, for example:
https://t.me/channelusername/101-110
Choose one or more media types, then press **Done** to start.
Press **❌ Cancel** to cancel media selection. Deleted or unavailable posts are skipped.

"""


async def main():
    await bot.start()
    print("WZGram bot started.")

    if acc is not None:
        await acc.start()
        print("WZGram user session started.")

    try:
        # Keep the process alive; Heroku terminates the worker on dyno shutdown.
        await asyncio.Event().wait()
    finally:
        if acc is not None:
            await acc.stop()
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
