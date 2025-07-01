# =========================
# Imports
# =========================
import asyncio
import base64
from bson import ObjectId
import os
import re
import sys
from datetime import datetime, timezone
from collections import defaultdict

from pyrogram import Client, enums, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from pyrogram.errors import ListenerTimeout
import uvicorn

from config import *
from utility import (
    add_user, is_token_valid, authorize_user, is_user_authorized,
    generate_token, shorten_url, get_token_link, extract_channel_and_msg_id,
    safe_api_call, get_allowed_channels, invalidate_search_cache,
    delete_after_delay, human_readable_size,
    queue_file_for_processing, file_queue_worker,
    file_queue, extract_tmdb_link, periodic_expiry_cleanup,
    restore_tmdb_photos, get_cached_search,
    set_cached_search
)
from db import (db, users_col, 
                tokens_col, 
                files_col, 
                allowed_channels_col, 
                auth_users_col,
                tmdb_col,
                )

from fast_api import api
from tmdb import get_by_id
import logging
from pyrogram.types import CallbackQuery
from urllib.parse import quote_plus, unquote_plus
import base64

# =========================
# Constants & Globals
# ========================= 

TOKEN_VALIDITY_SECONDS = 24 * 60 * 60  # 24 hours token validity
MAX_FILES_PER_SESSION = 10             # Max files a user can access per session
PAGE_SIZE = 10  # Number of files per page
SEARCH_PAGE_SIZE = 10  # You can adjust this

# Initialize Pyrogram bot client
bot = Client(
    "bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=1000,
    parse_mode=enums.ParseMode.HTML
)

# Track how many files each user has accessed in the current session
user_file_count = defaultdict(int)
copy_lock = asyncio.Lock()

if "file_name_text" not in [idx["name"] for idx in files_col.list_indexes()]:
    files_col.create_index([("file_name", "text")])

def encode_file_link(channel_id, message_id):
    # Returns a base64 string for deep linking
    raw = f"{channel_id}_{message_id}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")

# =========================
# Bot Command Handlers
# =========================

@bot.on_message(filters.command("start"))
async def start_handler(client, message):
    """
    Handles the /start command.
    - Registers the user.
    - Handles token-based authorization.
    - Handles file access via deep link.
    - Sends a greeting if no special argument is provided.
    """
    # Only allow in private chat
    if not message.chat.type == enums.ChatType.PRIVATE:
        try:
            reply = await safe_api_call(
                message.reply_text(
                    f"🔒 Please <b>DM</b> to use <code>/start</code>.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Open Bot", url=f"https://t.me/{BOT_USERNAME}")]]
                    ),
                    parse_mode=enums.ParseMode.HTML,
                )
            )
            if reply:
                bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
            bot.loop.create_task(delete_after_delay(client, message.chat.id, message.id))
        except Exception:
            pass
        return
    
    try:
        user_id = message.from_user.id
        user = message.from_user
        user_name = user.first_name or user.last_name or (user.username and f"@{user.username}") or "USER"

        add_user(user_id)
        bot_username = BOT_USERNAME

        # --- Token-based authorization ---
        if len(message.command) == 2 and message.command[1].startswith("token_"):
            if is_token_valid(message.command[1][6:], user_id):
                authorize_user(user_id)
                await safe_api_call(message.reply_text("✅ You are now authorized to access files for 24 hours."))
                await safe_api_call(bot.send_message(LOG_CHANNEL_ID, f"✅ User <b>{user_name}</b> (<code>{user.id}</code>) authorized via token."))
            else:
                await safe_api_call(message.reply_text("❌ Invalid or expired token. Please get a new link."))
            return

        # --- File access via deep link ---
        if len(message.command) == 2 and message.command[1].startswith("file_"):
            # Check if user is authorized
            if not is_user_authorized(user_id):
                now = datetime.now(timezone.utc)
                token_doc = tokens_col.find_one({
                    "user_id": user_id,
                    "expiry": {"$gt": now}
                })
                token_id = token_doc["token_id"] if token_doc else generate_token(user_id)
                short_link = shorten_url(get_token_link(token_id, bot_username))
                reply = await safe_api_call(message.reply_text(
                    "❌ You are not authorized\n"
                    "Please use this link to get access for 24 hours:",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Get Access Link", url=short_link)]]
                    )
                ))
                bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))

                return

            # Limit file access per session
            if user_file_count[user_id] >= MAX_FILES_PER_SESSION:
                await safe_api_call(message.reply_text("❌ You have reached the maximum of 10 files per session."))
                return

            # Decode file link and send file
            try:
                b64 = message.command[1][5:]
                padding = '=' * (-len(b64) % 4)
                decoded = base64.urlsafe_b64decode(b64 + padding).decode()
                channel_id_str, msg_id_str = decoded.split("_")
                channel_id = int(channel_id_str)
                msg_id = int(msg_id_str)
            except Exception:
                await safe_api_call(message.reply_text("Invalid file link."))
                return

            file_doc = files_col.find_one({"channel_id": channel_id, "message_id": msg_id})
            if not file_doc:
                await safe_api_call(message.reply_text("File not found."))
                return

            try:
                sent = await safe_api_call(client.copy_message(
                    chat_id=message.chat.id,
                    from_chat_id=file_doc["channel_id"],
                    message_id=file_doc["message_id"]
                ))
                user_file_count[user_id] += 1
                bot.loop.create_task(delete_after_delay(client, sent.chat.id, sent.id))
            except Exception as e:
                await safe_api_call(message.reply_text(f"Failed to send file: {e}"))
            return

        # --- Default greeting ---
        reply = await safe_api_call(
            message.reply_text(
            f"<b>Welcome, {user_name}!</b>\n\n"
            f"<b>Use /search to find files.</b>\n\n"
            f"<b>Example:</b> <code>/search Batman</code>\n\n"
            f"<b>Need help?</b> Contact: {SUPPORT}",
            reply_markup=InlineKeyboardMarkup(
                [
                [InlineKeyboardButton("Updates Channel", url=f"{UPDATE_CHANNEL_LINK}")]
                ]
            ),
            parse_mode=enums.ParseMode.HTML
            )
        )
        if reply:
            bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
    except Exception as e:
        await safe_api_call(message.reply_text(f"⚠️ An unexpected error occurred: {e}"))

@bot.on_message(filters.channel & (filters.document | filters.video | filters.audio | filters.photo))
async def channel_file_handler(client, message):
    allowed_channels = await get_allowed_channels()
    if message.chat.id not in allowed_channels:
        return
    await queue_file_for_processing(message, reply_func=message.reply_text)
    await file_queue.join()
    invalidate_search_cache()

@bot.on_message(filters.command("index") & filters.private & filters.user(OWNER_ID) )
async def index_channel_files(client, message):
    """
    Handles the /index command for the owner.
    - Asks for start and end file links.
    - Indexes files in the specified range from allowed channels.
    - Only supports /c/ links.
    """
    prompt = await safe_api_call(message.reply_text("Please send the **start file link** (Telegram message link, only /c/ links supported):"))
    try:
        start_msg = await client.listen(message.chat.id, timeout=120)
    except ListenerTimeout:
        await safe_api_call(prompt.edit_text("⏰ Timeout! You took too long to reply. Please try again."))
        return
    start_link = start_msg.text.strip()

    prompt2 = await safe_api_call(message.reply_text("Now send the **end file link** (Telegram message link, only /c/ links supported):"))
    try:
        end_msg = await client.listen(message.chat.id, timeout=120)
    except ListenerTimeout:
        await safe_api_call(prompt2.edit_text("⏰ Timeout! You took too long to reply. Please try again."))
        return
    end_link = end_msg.text.strip()

    try:
        start_id, start_msg_id = extract_channel_and_msg_id(start_link)
        end_id, end_msg_id = extract_channel_and_msg_id(end_link)

        if start_id != end_id:
            await message.reply_text("Start and end links must be from the same channel.")
            return

        channel_id = start_id
        allowed_channels = await get_allowed_channels()
        if channel_id not in allowed_channels:
            await message.reply_text("❌ This channel is not allowed for indexing.")
            return

        if start_msg_id > end_msg_id:
            start_msg_id, end_msg_id = end_msg_id, start_msg_id

    except Exception as e:
        await message.reply_text(f"Invalid link: {e}")
        return

    reply = await message.reply_text(f"Indexing files from {start_msg_id} to {end_msg_id} in channel {channel_id}...")

    batch_size = 50
    total_queued = 0
    for batch_start in range(start_msg_id, end_msg_id + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, end_msg_id)
        ids = list(range(batch_start, batch_end + 1))
        try:
            messages = []
            for msg_id in ids:
                msg = await safe_api_call(client.get_messages(channel_id, msg_id))
                messages.append(msg)
        except Exception as e:
            await message.reply_text(f"Failed to get messages {batch_start}-{batch_end}: {e}")
            continue
        for msg in messages:
            if not msg:
                continue
            if msg.document or msg.video or msg.audio or msg.photo:
                await queue_file_for_processing(
                    msg,
                    channel_id=channel_id,
                    reply_func=reply.edit_text
                )
                total_queued += 1
        invalidate_search_cache()

    logger.info(f"✅ Queued {total_queued} files from channel {channel_id} for processing.")


@bot.on_message(filters.private & filters.command("del") & filters.user(OWNER_ID))
async def delete_command(client, message):
    try:
        args = message.text.split(maxsplit=2)
        if len(args) < 3:
            await message.reply_text("Usage: /del <file|tmdb> <link>")
            return
        delete_type = args[1].strip().lower()
        user_input = args[2].strip()
        if delete_type == "file":
            try:
                channel_id, msg_id = extract_channel_and_msg_id(user_input)
            except Exception as e:
                await message.reply_text(f"Error: {e}")
                return
            # Try to find by channel_id and message_id (not msg_id)
            file_doc = files_col.find_one({"channel_id": channel_id, "message_id": msg_id})
            if not file_doc:
                await message.reply_text("No file found with that link in the database.")
                return
            # Use the same keys for deletion as for finding
            result = files_col.delete_one({"channel_id": channel_id, "message_id": msg_id})
            if result.deleted_count > 0:
                await message.reply_text(f"Database record deleted. File name: {file_doc.get('file_name')}")
            else:
                await message.reply_text(f"No file found with File name: {file_doc.get('file_name')}")
        elif delete_type == "tmdb":
            try:
                tmdb_type, tmdb_id = await extract_tmdb_link(user_input)
            except Exception as e:
                await message.reply_text(f"Error: {e}")
                return
            result = tmdb_col.delete_one({"tmdb_type": tmdb_type, "tmdb_id": tmdb_id})
            if result.deleted_count > 0:
                await message.reply_text(f"Database record deleted {tmdb_type}/{tmdb_id}.")
            else:
                await message.reply_text(f"No TMDB record found with ID {tmdb_type}/{tmdb_id} in the database.")
        else:
            await message.reply_text("Invalid delete type. Use 'file' or 'tmdb' or 'imgbb'.")

    except Exception as e:
        await message.reply_text(f"Error: {e}")
                                 
@bot.on_message(filters.command('restart') & filters.private & filters.user(OWNER_ID))
async def restart(client, message):
    """
    Handles the /restart command for the owner.
    - Deletes the log file, runs update.py, and restarts the bot.
    """    
    log_file = "bot_log.txt"
    if os.path.exists(log_file):
        try:
            os.remove(log_file)
        except Exception as e:
            await safe_api_call(message.reply_text(f"Failed to delete log file: {e}"))
    os.system("python3 update.py")
    os.execl(sys.executable, sys.executable, "bot.py")

@bot.on_message(filters.private & filters.command("restore") & filters.user(OWNER_ID))
async def update_info(client, message):
    try:
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("Usage: /restore tmdb [start_objectid]")
            return
        restore_type = args[1].strip()
        start_id = args[2] if len(args) > 2 else None
        if start_id:
            try:
                start_id = ObjectId(start_id)
            except Exception:
                await message.reply_text("Invalid ObjectId format for start_id.")
                return
        if restore_type == "tmdb":
            await restore_tmdb_photos(bot, start_id)
        else:
            await message.reply_text("Invalid restore type. Use 'tmdb'.")
    except Exception as e:
        await message.reply_text(f"Error in Update Command: {e}")
        

@bot.on_message(filters.command("add") & filters.private & filters.user(OWNER_ID))
async def add_channel_handler(client, message: Message):
    """
    Handles the /add command for the owner.
    - Adds a channel to the allowed channels list in the database.
    """
    if len(message.command) < 3:
        await message.reply_text("Usage: /add channel_id channel_name")
        return
    try:
        channel_id = int(message.command[1])
        channel_name = " ".join(message.command[2:])
        allowed_channels_col.update_one(
            {"channel_id": channel_id},
            {"$set": {"channel_id": channel_id, "channel_name": channel_name}},
            upsert=True
        )
        await message.reply_text(f"✅ Channel {channel_id} ({channel_name}) added to allowed channels.")
    except Exception as e:
        await message.reply_text(f"Error: {e}")

@bot.on_message(filters.command("rm") & filters.private & filters.user(OWNER_ID))
async def remove_channel_handler(client, message: Message):
    """
    Handles the /rm command for the owner.
    - Removes a channel from the allowed channels list in the database.
    """
    if len(message.command) != 2:
        await message.reply_text("Usage: /rm channel_id")
        return
    try:
        channel_id = int(message.command[1])
        result = allowed_channels_col.delete_one({"channel_id": channel_id})
        if result.deleted_count:
            await message.reply_text(f"✅ Channel {channel_id} removed from allowed channels.")
        else:
            await message.reply_text("❌ Channel not found in allowed channels.")
    except Exception as e:
        await message.reply_text(f"Error: {e}")

@bot.on_message(filters.command("broadcast") & filters.private & filters.user(OWNER_ID))
async def broadcast_handler(client, message: Message):
    """
    Handles the /broadcast command for the owner.
    - If replying to a message, copies that message to all users.
    - Otherwise, broadcasts a text message.
    - Removes users from DB if blocked or deactivated.
    """
    if message.reply_to_message:
        users = users_col.find({}, {"_id": 0, "user_id": 1})
        total = 0
        failed = 0
        removed = 0
        for user in users:
            try:
                await asyncio.sleep(1)  # Rate limit to avoid flooding
                await safe_api_call(message.reply_to_message.copy(user["user_id"]))
                total += 1
            except Exception as e:
                failed += 1
                err_str = str(e)
                if "UserIsBlocked" in err_str or "InputUserDeactivated" in err_str:
                    users_col.delete_one({"user_id": user["user_id"]})
                    removed += 1
                continue
            await asyncio.sleep(3)
        await message.reply_text(f"✅ Broadcasted replied message to {total} users. Failed: {failed}. Removed: {removed}")

@bot.on_message(filters.command("log") & filters.private & filters.user(OWNER_ID))
async def send_log_file(client, message: Message):
    """
    Handles the /log command for the owner.
    - Sends the bot.log file to the owner.
    """
    log_file = "bot_log.txt"
    if not os.path.exists(log_file):
        await safe_api_call(message.reply_text("Log file not found."))
        return
    try:
        await safe_api_call(client.send_document(message.chat.id, log_file, caption="Here is the log file."))
    except Exception as e:
        await safe_api_call(message.reply_text(f"Failed to send log file: {e}"))

@bot.on_message(filters.command("stats") & filters.private & filters.user(OWNER_ID))
async def stats_command(client, message: Message):
    """Show statistics (only for OWNER_ID)."""
    try:
        total_auth_users = auth_users_col.count_documents({})
        total_users = users_col.count_documents({})
        total_files = files_col.count_documents({})
        pipeline = [
            {"$group": {"_id": None, "total": {"$sum": "$file_size"}}}
        ]
        result = list(files_col.aggregate(pipeline))
        total_storage = result[0]["total"] if result else 0

        stats = db.command("dbstats")
        db_storage = stats.get("storageSize", 0)

        await safe_api_call(
            message.reply_text(
            f"👤 Total auth users: <b>{total_auth_users}/{total_users}</b>\n"
            f"📁 Total files: <b>{total_files}</b>\n"
            f"💾 Files size: <b>{human_readable_size(total_storage)}</b>\n"
            f"📊 Database storage used: <b>{db_storage / (1024 * 1024):.2f} MB</b>",
            )
        )
    except Exception as e:
        await message.reply_text(f"⚠️ An error occurred while fetching stats:\n<code>{e}</code>")

@bot.on_message(filters.private & filters.command("tmdb") & filters.user(OWNER_ID))
async def tmdb_command(client, message):
    try:
        if len(message.command) < 2:
            await safe_api_call(message.reply_text("Usage: /tmdb tmdb_link"))
            return

        tmdb_link = message.command[1]
        tmdb_type, tmdb_id = await extract_tmdb_link(tmdb_link)
        result = await get_by_id(tmdb_type, tmdb_id)
        poster_url = result.get('poster_url')
        trailer = result.get('trailer_url')
        info = result.get('message')

        update = {
            "$setOnInsert": {"tmdb_id": tmdb_id, "tmdb_type": tmdb_type}
        }
        tmdb_col.update_one(
            {"tmdb_id": tmdb_id, "tmdb_type": tmdb_type},
            update,
            upsert=True
        )
        
        if poster_url:
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🎥 Trailer", url=trailer)]]) if trailer else None
            await safe_api_call(
                client.send_photo(
                    UPDATE_CHANNEL_ID,
                    photo=poster_url,
                    caption=info,
                    parse_mode=enums.ParseMode.HTML,
                    reply_markup=keyboard
                )
            )
    except Exception as e:
        logging.exception("Error in tmdb_command")
        await safe_api_call(message.reply_text(f"Error in tmdb command: {e}"))

@bot.on_message(filters.command("search"))
async def search_files_handler(client, message):
    """
    Search files by name across all allowed channels, with pagination and buttons.
    Usage: /search <query>
    If no query is given, lets user browse by channel or select a channel to search in.
    If used outside private chat, instructs user to DM the bot.
    Handles errors gracefully (e.g., 400 button data invalid).
    """
    # Only allow in private chat
    if not message.chat.type == enums.ChatType.PRIVATE:
        try:
            reply = await safe_api_call(
                message.reply_text(
                    f"🔒 Please <b>DM</b> to use <code>/search</code>.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Open Bot", url=f"https://t.me/{BOT_USERNAME}")]]
                    ),
                    parse_mode=enums.ParseMode.HTML,
                )
            )
            if reply:
                bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
            bot.loop.create_task(delete_after_delay(client, message.chat.id, message.id))
        except Exception:
            pass
        return

    args = message.text.split(maxsplit=1)
    channels = list(allowed_channels_col.find({}, {"_id": 0, "channel_id": 1, "channel_name": 1}))
    if len(args) < 2:
        # No query: show channel browse menu and search-in-channel menu
        if not channels:
            try:
                reply = await safe_api_call(message.reply_text("No channels available for browsing."))
                if reply:
                    bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
                bot.loop.create_task(delete_after_delay(client, message.chat.id, message.id))
            except Exception as e:
                await safe_api_call(message.reply_text(f"Error: {e}"))
            return
        try:
            buttons = [
                [InlineKeyboardButton(f"{c['channel_name']}", callback_data=f"browse_{c['channel_id']}_0")]
                for c in channels
            ]
            reply = await safe_api_call(
                message.reply_text(
                    "<b>Browse Files</b>\n"
                    "<b>Tip:</b> Use <code>/search Batman</code> or any keyword.\n"
                    "<b>Select a category to browse:</b>",
                    reply_markup=InlineKeyboardMarkup(buttons),
                    parse_mode=enums.ParseMode.HTML
                )
            )
            if reply:
                bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
            bot.loop.create_task(delete_after_delay(client, message.chat.id, message.id))
        except Exception as e:
            await safe_api_call(message.reply_text(f"Error displaying browse menu: {e}"))
        return

    query = args[1].strip()
    page = 0
    # Add channel selection buttons for search
    if not channels:
        try:
            reply = await safe_api_call(message.reply_text("No channels available for searching."))
            if reply:
                bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
            bot.loop.create_task(delete_after_delay(client, message.chat.id, message.id))
        except Exception as e:
            await safe_api_call(message.reply_text(f"Error: {e}"))
        return
    try:
        buttons = [
            [InlineKeyboardButton(c["channel_name"], callback_data=f"searchchan_{c['channel_id']}_0_{quote_plus(query)}")]
            for c in channels
        ]
        reply = await safe_api_call(
            message.reply_text(
                f"<b>Select a category to search:</b>\n<code>{query}</code>",
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode=enums.ParseMode.HTML
            )
        )
        if reply:
            bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
        bot.loop.create_task(delete_after_delay(client, message.chat.id, message.id))
    except Exception as e:
        logger.error(f"Error displaying search menu: {e}")
        await safe_api_call(message.reply_text("Invalid search keywords."))

async def send_search_results(client, message_or_callback, query, page, as_callback=False, channel_id=None):
    skip = page * SEARCH_PAGE_SIZE
    files, total_files = get_cached_search(query, page, channel_id)
    if files is None:
        search_stage = {
            "$search": {
                "index": "default",  # Change if your index is named differently
                "text": {
                    "query": query,
                    "path": "file_name",
                    "fuzzy": {
                        "maxEdits": 2,      # Allow up to 2 typos
                        "prefixLength": 2,  # Require first 2 chars to match
                        "maxExpansions": 50
                    }
                }
            }
        }
        match_stage = {}
        if channel_id is not None:
            match_stage = {"$match": {"channel_id": channel_id}}
        else:
            channels = list(allowed_channels_col.find({}, {"_id": 0, "channel_id": 1}))
            allowed_ids = [c["channel_id"] for c in channels]
            match_stage = {"$match": {"channel_id": {"$in": allowed_ids}}}

        pipeline = [
            search_stage,
            match_stage,
            {"$project": {
                "_id": 0,
                "file_name": 1,
                "file_size": 1,
                "file_format": 1,
                "message_id": 1,
                "date": 1,
                "channel_id": 1,
                "score": {"$meta": "searchScore"}
            }},
            {"$sort": {"score": -1}},
            {"$skip": skip},
            {"$limit": SEARCH_PAGE_SIZE}
        ]
        files = list(files_col.aggregate(pipeline))
        # For total count, run the same pipeline but with $count
        count_pipeline = [
            search_stage,
            match_stage,
            {"$count": "total"}
        ]
        count_result = list(files_col.aggregate(count_pipeline))
        total_files = count_result[0]["total"] if count_result else 0
        set_cached_search(query, page, channel_id, files, total_files)

    if not files:
        text = "No files found for your search."
        if as_callback:
            reply = await safe_api_call(message_or_callback.edit_message_text(text))
            if reply:
                bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
        else:
            reply = await safe_api_call(message_or_callback.reply_text(text))
            if reply:
                bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
        return

    text = f"Search results for <b>{query}</b> (Page {page+1}):"
    buttons = []
    for f in files:
        file_link = encode_file_link(f["channel_id"], f["message_id"])
        size_str = human_readable_size(f.get('file_size', 0))
        btn_text = f"{size_str} | {f.get('file_name')}"
        buttons.append([
            InlineKeyboardButton(btn_text, url=f"https://t.me/{BOT_USERNAME}?start=file_{file_link}")
        ])

    nav = []
    if skip > 0:
        if channel_id is not None:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"searchchan_{channel_id}_{page-1}_{quote_plus(query)}"))
        else:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"search_{quote_plus(query)}_{page-1}"))
    if skip + SEARCH_PAGE_SIZE < total_files:
        if channel_id is not None:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"searchchan_{channel_id}_{page+1}_{quote_plus(query)}"))
        else:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"search_{quote_plus(query)}_{page+1}"))
    if nav:
        buttons.append(nav)

    if as_callback:
        reply = await safe_api_call(
            message_or_callback.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode=enums.ParseMode.HTML
            )
        )
        if reply:
            bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
    else:
        reply = await safe_api_call(
            message_or_callback.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode=enums.ParseMode.HTML
            )
        )
        if reply:
            bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))

@bot.on_callback_query(filters.regex(r"^search_(.+)_(\d+)$"))
async def search_pagination_callback(client, callback_query: CallbackQuery):
    m = re.match(r"^search_(.+)_(\d+)$", callback_query.data)
    if not m:
        reply = await safe_api_call(callback_query.answer("Invalid search callback.", show_alert=True))
        return
    query, page = m.groups()
    query = unquote_plus(query)
    page = int(page)
    await send_search_results(client, callback_query, query, page, as_callback=True)

@bot.on_callback_query(filters.regex(r"^searchchan_(\-?\d+)_(\d+)_(.+)$"))
async def search_channel_callback(client, callback_query: CallbackQuery):
    m = re.match(r"^searchchan_(\-?\d+)_(\d+)_(.+)$", callback_query.data)
    if not m:
        reply = await safe_api_call(callback_query.answer("Invalid search-in-channel callback.", show_alert=True))
        return
    channel_id, page, query = m.groups()
    channel_id = int(channel_id)
    page = int(page)
    query = unquote_plus(query)
    await send_search_results(client, callback_query, query, page, as_callback=True, channel_id=channel_id)

@bot.on_callback_query(filters.regex(r"^browse_(\-?\d+)_(\d+)$"))
async def browse_channel_callback(client, callback_query: CallbackQuery):
    m = re.match(r"^browse_(\-?\d+)_(\d+)$", callback_query.data)
    if not m:
        reply = await safe_api_call(callback_query.answer("Invalid browse callback.", show_alert=True))
        return
    channel_id, page = m.groups()
    channel_id = int(channel_id)
    page = int(page)
    skip = page * SEARCH_PAGE_SIZE
    files = list(files_col.find(
        {"channel_id": channel_id},
        {"_id": 0, "file_name": 1, "file_size": 1, "file_format": 1, "message_id": 1, "date": 1, "channel_id": 1}
    ).sort("message_id", -1).skip(skip).limit(SEARCH_PAGE_SIZE))
    total_files = files_col.count_documents({"channel_id": channel_id})

    channel_doc = allowed_channels_col.find_one({"channel_id": channel_id})
    channel_name = channel_doc["channel_name"] if channel_doc else str(channel_id)

    if not files:
        reply = await safe_api_call(callback_query.edit_message_text(f"No files found in <b>{channel_name}</b>.", parse_mode=enums.ParseMode.HTML))
        if reply:
            bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
        return

    text = f"Browsing <b>{channel_name}</b> (Page {page+1}):"
    buttons = []
    for f in files:
        file_link = encode_file_link(f["channel_id"], f["message_id"])
        size_str = human_readable_size(f.get('file_size', 0))
        btn_text = f"{size_str} | {f.get('file_name')}"
        buttons.append([
            InlineKeyboardButton(btn_text, url=f"https://t.me/{BOT_USERNAME}?start=file_{file_link}")
        ])

    nav = []
    if skip > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"browse_{channel_id}_{page-1}"))
    if skip + SEARCH_PAGE_SIZE < total_files:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"browse_{channel_id}_{page+1}"))
    if nav:
        buttons.append(nav)

    reply = await safe_api_call(
        callback_query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=enums.ParseMode.HTML
        )
    )
    if reply:
        bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))

@bot.on_message(filters.chat(GROUP_ID) & filters.service)
async def delete_service_messages(client, message):
    try:
        # Greet new members and guide them to use /search
        if message.new_chat_members:
            for user in message.new_chat_members:
                try:
                    reply = await message.reply_text(
                        f"<b>Welcome, {user.first_name}!</b>\n"
                        f"<b>Find any file instantly.</b>\n"
                        f"DM <b>@{BOT_USERNAME}</b> and use <code>/search</code> to get started.",
                        parse_mode=enums.ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(
                            [
                                [InlineKeyboardButton("Updates Channel", url=f"{UPDATE_CHANNEL_LINK}")]
                            ]
                        )
                    )
                    if reply:
                        bot.loop.create_task(delete_after_delay(client, reply.chat.id, reply.id))
                except Exception as e:
                    logger.error(f"Failed to greet new member: {e}")
        await message.delete()
    except Exception as e:
        logger.error(f"Failed to delete service message: {e}")

@bot.on_message(filters.command("chatop") & filters.private & filters.user(OWNER_ID))
async def chatop_handler(client, message: Message):
    """
    Usage:
      /chatop send <chat_id> <message>
      /chatop del <chat_id> <message_id>
    """
    args = message.text.split(maxsplit=3)
    if len(args) < 4 and not (len(args) == 4 and args[1] == "send"):
        await message.reply_text("Usage:\n/chatop send <chat_id> <message>\n/chatop del <chat_id> <message_id>")
        return
    op = args[1].lower()
    chat_id = args[2]
    if op == "send":
        if len(args) < 4:
            await message.reply_text("Usage: /chatop send <chat_id> <message>")
            return
        try:
            sent = await client.send_message(int(chat_id), args[3])
            await message.reply_text(f"✅ Sent to {chat_id} (message_id: {sent.id})")
        except Exception as e:
            await message.reply_text(f"❌ Failed: {e}")
    elif op == "del":
        if len(args) != 4:
            await message.reply_text("Usage: /chatop del <chat_id> <message_id>")
            return
        try:
            await client.delete_messages(int(chat_id), int(args[3]))
            await message.reply_text(f"✅ Deleted message {args[3]} in chat {chat_id}")
        except Exception as e:
            await message.reply_text(f"❌ Failed: {e}")
    else:
        await message.reply_text("Invalid operation. Use 'send' or 'del'.")

# =========================
# Main Entrypoint
# =========================

async def main():
    """
    Starts the bot and FastAPI server.
    """
    # Set bot commands


    await bot.start()

    await bot.set_bot_commands([
        BotCommand("start", "check bot status"),
        BotCommand("search", "search files"),
        BotCommand("stats", "this command is for admin only)")
    ])
    
    bot.loop.create_task(start_fastapi())
    bot.loop.create_task(file_queue_worker(bot))  # Start the queue worker
    bot.loop.create_task(periodic_expiry_cleanup())

    # Send startup message to log channel
    try:
        me = await bot.get_me()
        user_name = me.username or "Bot"
        await bot.send_message(LOG_CHANNEL_ID, f"✅ @{user_name} started and FastAPI server running.")
        logger.info("Bot started and FastAPI server running.")
    except Exception as e:
        print(f"Failed to send startup message to log channel: {e}")

async def start_fastapi():
    """
    Starts the FastAPI server using Uvicorn.
    """
    try:
        config = uvicorn.Config(api, host="0.0.0.0", port=8000, loop="asyncio", log_level="warning")
        server = uvicorn.Server(config)
        await server.serve()
    except KeyboardInterrupt:
        pass
        logger.info("FastAPI server stopped.")

if __name__ == "__main__":
    """
    Main process entrypoint.
    - Runs the bot and FastAPI server.
    - Handles graceful shutdown on KeyboardInterrupt.
    """
    try:
        bot.loop.run_until_complete(main())
        bot.loop.run_forever()
    except KeyboardInterrupt:
        bot.stop()
        tasks = asyncio.all_tasks(loop=bot.loop)
        for task in tasks:
            task.cancel()
        bot.loop.stop()
        logger.info("Bot stopped.")
