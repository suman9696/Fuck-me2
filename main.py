import asyncio
import json
import os
import re
import time
from os import environ
from urllib.parse import urlparse

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

# WZGram has its own parallel media transfer engine. Start conservatively;
# increase only after testing on your Heroku dyno and Telegram account.
MAX_CONCURRENT_TRANSMISSIONS = max(
    1, min(16, int(environ.get("MAX_CONCURRENT_TRANSMISSIONS", "8")))
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
    match = re.search(r"https?://t\.me/[^\s]+", cleaned)
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

    message_part = message_part.replace("?single", "")
    if "-" in message_part:
        first, last = message_part.split("-", 1)
    else:
        first = last = message_part

    if not first.isdigit() or not last.isdigit():
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

    def __call__(self, current, total):
        # WZGram may invoke progress callbacks from a concurrent.futures
        # worker thread. A synchronous callback avoids an un-awaited
        # coroutine; schedule the Telegram edit safely on the main loop.
        if self.loop.is_closed():
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self._update(current, total), self.loop
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
        f"{USAGE}",
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
# Download / upload
# ---------------------------------------------------------------------------
async def handle_private(message, chat_ref, message_id):
    source_message = await safe_call(acc.get_messages, chat_ref, message_id)
    if not source_message or getattr(source_message, "empty", False):
        return

    message_type = _get_message_type(source_message)

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


async def _process_message(client, message):
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

    for message_id in range(first_id, last_id + 1):
        if link_kind in ("c", "b"):
            await handle_private(message, chat_ref, message_id)
            continue

        try:
            source_message = await safe_call(
                bot.get_messages, chat_ref, message_id
            )
        except UsernameNotOccupied:
            await safe_call(
                bot.send_message,
                message.chat.id,
                "**The username is not occupied by anyone**",
                reply_to_message_id=message.id,
            )
            return

        try:
            await _send_copy_to_destinations(
                source_message.chat.id,
                source_message.id,
                message.chat.id,
                message.id,
            )
        except Exception as copy_error:
            if acc is None:
                print(f"Public copy failed and no user session is configured: {copy_error!r}")
                continue
            await handle_private(message, chat_ref, message_id)


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
𝐉𝐮𝐬𝐭 𝐒𝐞𝐧𝐝 𝐏𝐨𝐬𝐭/𝐬 𝐋𝐢𝐧𝐤

**🈂️ 𝐅𝐨𝐫 𝐏𝐫𝐢𝐯𝐚𝐭𝐞 𝐂𝐡𝐚𝐭𝐬 🈂️**
𝐅𝐢𝐫𝐬𝐭 𝐬𝐞𝐧𝐝 𝐭𝐡𝐞 𝐢𝐧𝐯𝐢𝐭𝐞 𝐥𝐢𝐧𝐤 (𝐢𝐟 𝐧𝐞𝐞𝐝𝐞𝐝), 𝐭𝐡𝐞𝐧 𝐬𝐞𝐧𝐝 𝐭𝐡𝐞 𝐩𝐨𝐬𝐭 𝐥𝐢𝐧𝐤.

**🈂️ 𝐌𝐮𝐥𝐭𝐢 𝐏𝐨𝐬𝐭𝐬 🈂️**
https://t.me/channelusername/101-110

**Commands**
/settarget -1001234567890
/mytargets
/removetarget
/autoupload on
/autoupload off
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
