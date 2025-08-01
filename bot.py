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
    auto_delete_message, human_readable_size,
    queue_file_for_processing, file_queue_worker,
    file_queue, extract_tmdb_link, periodic_expiry_cleanup,
    restore_tmdb_photos
)
from db import (db, users_col, 
                tokens_col, 
                files_col, 
                allowed_channels_col, 
                auth_users_col,
                tmdb_col
                )

from fast_api import api
from tmdb import get_by_id
import logging
from pyrogram.types import CallbackQuery
import base64
from urllib.parse import quote_plus, unquote_plus

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

def sanitize_query(query):
    """Sanitizes and normalizes a search query for consistent matching of 'and' and '&'."""
    query = query.strip().lower()
    # Replace all '&' with 'and'
    query = re.sub(r"\s*&\s*", " and ", query)
    # Replace multiple spaces and limit length
    query = re.sub(r"\s+", " ", query)
    return query[:100]

def contains_url(text):
    url_pattern = r'https?://\S+|www\.\S+'
    return re.search(url_pattern, text) is not None

def build_search_pipeline(query, allowed_ids, skip, limit):
    # If the query contains any space, treat it as a phrase search
    if " " in query.strip():
        search_stage = {
            "$search": {
                "index": "default",
                "phrase": {
                    "query": query,
                    "path": "file_name"
                }
            }
        }
    else:
        search_stage = {
            "$search": {
                "index": "default",
                "text": {
                    "query": query,
                    "path": "file_name"
                }
            }
        }

    match_stage = {"$match": {"channel_id": {"$in": allowed_ids}}}
    project_stage = {
        "$project": {
            "_id": 0,
            "file_name": 1,
            "file_size": 1,
            "file_format": 1,
            "message_id": 1,
            "date": 1,
            "channel_id": 1,
            "score": {"$meta": "searchScore"}
        }
    }
    sort_stage = {"$sort": {"score": -1, "file_name": 1}}

    facet_stage = {
        "$facet": {
            "results": [
                project_stage,
                sort_stage,
                {"$skip": skip},
                {"$limit": limit}
            ],
            "totalCount": [
                {"$count": "total"}
            ]
        }
    }
    return [search_stage, match_stage, facet_stage]

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
    - Deletes every message sent and received, but only once after all tasks are done.
    """
    reply_msg = None  
    # Only allow in private chat
    if not message.chat.type == enums.ChatType.PRIVATE:
        try:
            reply_msg = await safe_api_call(
                message.reply_text(
                    f"🔒 Please <b>DM</b> to use <code>/start</code>.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Open Bot", url=f"https://t.me/{BOT_USERNAME}")]]
                    ),
                    parse_mode=enums.ParseMode.HTML,
                )
            )
        except Exception:
            pass
    else:
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
                    reply_msg = await safe_api_call(message.reply_text("✅ You are now authorized to access files for 24 hours."))
                    await safe_api_call(bot.send_message(LOG_CHANNEL_ID, f"✅ User <b>{user_name}</b> (<code>{user.id}</code>) authorized via token."))
                else:
                    reply_msg = await safe_api_call(message.reply_text("❌ Invalid or expired token. Please get a new link."))

            # --- File access via deep link ---
            elif len(message.command) == 2 and message.command[1].startswith("file_"):
                # Check if user is authorized, but skip for OWNER_ID
                if user_id != OWNER_ID and not is_user_authorized(user_id):
                    now = datetime.now(timezone.utc)
                    token_doc = tokens_col.find_one({
                        "user_id": user_id,
                        "expiry": {"$gt": now}
                    })
                    token_id = token_doc["token_id"] if token_doc else generate_token(user_id)
                    short_link = shorten_url(get_token_link(token_id, bot_username))
                    reply_msg = await safe_api_call(message.reply_text(
                        "❌ You are not authorized\n"
                        "Please use this link to get access for 24 hours:",
                        reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Get Access Link", url=short_link)]]
                        )
                    ))
                elif user_id != OWNER_ID and user_file_count[user_id] >= MAX_FILES_PER_SESSION:
                    reply_msg = await safe_api_call(message.reply_text("❌ You have reached the maximum of 10 files per session."))
                else:
                    # Decode file link and send file
                    try: 
                        b64 = message.command[1][5:]
                        padding = '=' * (-len(b64) % 4)
                        decoded = base64.urlsafe_b64decode(b64 + padding).decode()
                        channel_id_str, msg_id_str = decoded.split("_")
                        channel_id = int(channel_id_str)
                        msg_id = int(msg_id_str)
                        file_doc = files_col.find_one({"channel_id": channel_id, "message_id": msg_id})
                        if not file_doc:
                            reply_msg = await safe_api_call(message.reply_text("File not found."))
                        else:
                            reply_msg = await safe_api_call(client.copy_message(
                                chat_id=message.chat.id,
                                from_chat_id=file_doc["channel_id"],
                                message_id=file_doc["message_id"]
                            ))
                            user_file_count[user_id] += 1
                    except Exception as e:
                        reply_msg = await safe_api_call(message.reply_text(f"Failed to send file: {e}"))
            # --- Default greeting ---
            else:
                reply_msg = await safe_api_call(
                    message.reply_text(
                    f"<b>Welcome, {user_name}!</b>\n\n"
                    f"<b>Just send your search query and get instant answers</b>\n\n"
                    f"<b>Need help?</b> Contact: {SUPPORT}",
                    reply_markup=InlineKeyboardMarkup(
                        [
                        [InlineKeyboardButton("Updates Channel", url=f"{UPDATE_CHANNEL_LINK}")]
                        ]
                    ),
                    parse_mode=enums.ParseMode.HTML
                    )
                )
        except Exception as e:
            reply_msg = await safe_api_call(message.reply_text(f"⚠️ An unexpected error occurred: {e}"))

    if reply_msg:
        bot.loop.create_task(auto_delete_message(message, reply_msg))

@bot.on_message(filters.channel & (filters.document | filters.video | filters.audio | filters.photo))
async def channel_file_handler(client, message):
    allowed_channels = await get_allowed_channels()
    if message.chat.id not in allowed_channels:
        return
    await queue_file_for_processing(message, reply_func=message.reply_text)
    await file_queue.join()
    invalidate_search_cache()

@bot.on_message(filters.command("index") & filters.private & filters.user(OWNER_ID))
async def index_channel_files(client, message):
    """
    Handles the /index command for the owner.
    - Supports optional 'dup' flag.
    """
    # Support both `/index` and `/index dup`
    dup = False
    if len(message.command) > 1 and message.command[1].lower() == "dup":
        dup = True

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

    reply = await message.reply_text(f"Indexing files from {start_msg_id} to {end_msg_id} in channel {channel_id}... Duplicates allowed: {dup}")

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
                    reply_func=reply.edit_text,
                    duplicate=dup      # Pass the flag here!
                )
                total_queued += 1
        invalidate_search_cache()

    await message.reply_text(f"✅ Queued {total_queued} files from channel {channel_id} for processing. Duplicates allowed: {dup}")


@bot.on_message(filters.private & filters.command("del") & filters.user(OWNER_ID))
async def delete_command(client, message):
    try:
        args = message.text.split(maxsplit=3)
        reply = None
        if len(args) < 3:
            reply = await message.reply_text("Usage: /del <file|tmdb> <link> [end_link]")
            return
        delete_type = args[1].strip().lower()
        user_input = args[2].strip()
        end_input = args[3].strip() if len(args) > 3 else None

        if delete_type == "file":
            try:
                channel_id, msg_id = extract_channel_and_msg_id(user_input)
                if end_input:
                    end_channel_id, end_msg_id = extract_channel_and_msg_id(end_input)
                    if channel_id != end_channel_id:
                        await message.reply_text("Start and end links must be from the same channel.")
                        return
                    if msg_id > end_msg_id:
                        msg_id, end_msg_id = end_msg_id, msg_id
                    # Delete in range
                    result = files_col.delete_many({
                        "channel_id": channel_id,
                        "message_id": {"$gte": msg_id, "$lte": end_msg_id}
                    })
                    reply = await message.reply_text(f"Deleted {result.deleted_count} files from {msg_id} to {end_msg_id} in channel {channel_id}.")
                    return
            except Exception as e:
                await message.reply_text(f"Error: {e}")
                return
            # Single file delete
            file_doc = files_col.find_one({"channel_id": channel_id, "message_id": msg_id})
            if not file_doc:
                reply = await message.reply_text("No file found with that link in the database.")
                return
            result = files_col.delete_one({"channel_id": channel_id, "message_id": msg_id})
            if result.deleted_count > 0:
                reply = await message.reply_text(f"Database record deleted. File name: {file_doc.get('file_name')}\n({user_input})")
            else:
                reply = await message.reply_text(f"No file found with File name: {file_doc.get('file_name')}")
        elif delete_type == "tmdb":
            try:
                tmdb_type, tmdb_id = await extract_tmdb_link(user_input)
            except Exception as e:
                reply = await message.reply_text(f"Error: {e}")
                return
            result = tmdb_col.delete_one({"tmdb_type": tmdb_type, "tmdb_id": tmdb_id})
            if result.deleted_count > 0:
                reply = await message.reply_text(f"Database record deleted {tmdb_type}/{tmdb_id}.")
            else:
                reply = await message.reply_text(f"No TMDB record found with ID {tmdb_type}/{tmdb_id} in the database.")
        else:
            reply = await message.reply_text("Invalid delete type. Use 'file' or 'tmdb'.")
        if reply:
            bot.loop.create_task(auto_delete_message(message, reply))
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
    """Show statistics including per-channel file counts (OWNER only)."""
    try:
        total_auth_users = auth_users_col.count_documents({})
        total_users = users_col.count_documents({})

        # Total file storage size
        pipeline = [
            {"$group": {"_id": None, "total": {"$sum": "$file_size"}}}
        ]
        result = list(files_col.aggregate(pipeline))
        total_storage = result[0]["total"] if result else 0

        # Database storage size
        stats = db.command("dbstats")
        db_storage = stats.get("storageSize", 0)

        # Per-channel counts
        channel_pipeline = [
            {"$group": {"_id": "$channel_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        channel_counts = list(files_col.aggregate(channel_pipeline))
        channel_docs = allowed_channels_col.find({}, {"_id": 0, "channel_id": 1, "channel_name": 1})
        channel_names = {c["channel_id"]: c.get("channel_name", "") for c in channel_docs}

        # Compose stats message
        text = (
            f"👤 <b>Total auth users: {total_auth_users} / {total_users}</b>\n"
            f"💾 <b>Files size:</b> <b>{human_readable_size(total_storage)}</b>\n"
            f"📊 <b>Database storage used: {db_storage / (1024 * 1024):.2f} MB</b>\n"
        )

        if not channel_counts:
            text += " <b>No files indexed yet.</b>"
        else:
            for c in channel_counts:
                chan_id = c['_id']
                chan_name = channel_names.get(chan_id, 'Unknown')
                text += f"<b>{chan_name}</b>: <b>{c['count']} files</b>\n"

        reply = await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
        if reply:
            bot.loop.create_task(auto_delete_message(message, reply))
    except Exception as e:
        await message.reply_text(f"⚠️ An error occurred while fetching stats:\n<code>{e}</code>")

@bot.on_message(filters.private & filters.command("tmdb") & filters.user(OWNER_ID))
async def tmdb_command(client, message):
    try:
        if len(message.command) < 2:
            reply = await safe_api_call(message.reply_text("Usage: /tmdb tmdb_link"))
            await auto_delete_message(message, reply)
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
    await message.delete()

@bot.on_message(filters.private & filters.text & ~filters.command([
    "start", "stats", "add", "rm", "broadcast", "log", "tmdb", "restore", "index", "del", "restart", "chatop"
]))
async def instant_search_handler(client, message):
    reply = None
    try:
        if contains_url(message.text):
            return
        query = sanitize_query(message.text)
        if not query:
            return

        channels = list(allowed_channels_col.find({}, {"_id": 0, "channel_id": 1, "channel_name": 1}))
        if not channels:
            reply = await safe_api_call(message.reply_text("No allowed channels available for search."))
            return

        # Show channel selection buttons
        text = f"<b>What are you looking for 🔎</b> :"
        buttons = []
        for c in channels:
            chan_id = c["channel_id"]
            chan_name = c.get("channel_name", str(chan_id))
            data = f"search_channel:{quote_plus(query)}:{chan_id}:1"
            # Callback data includes query and channel_id, page=1
            buttons.append([
                InlineKeyboardButton(
                    chan_name,
                    callback_data=data
                )
            ])
        reply_markup = InlineKeyboardMarkup(buttons)
        reply = await safe_api_call(
            message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=enums.ParseMode.HTML
            )
        )
    except Exception as e:
        logger.error(f"Error in instant_search_handler: {e}")
        reply = await message.reply_text("Invalid search query. Please try again with a different query.")
    if reply:
        bot.loop.create_task(auto_delete_message(message, reply))


@bot.on_callback_query(filters.regex(r"^search_channel:(.+):(-?\d+):(\d+)$"))
async def channel_search_callback_handler(client, callback_query: CallbackQuery):
    """
    Handles user's channel selection for search with pagination.
    Performs the search only in the selected channel and displays paged results.
    """
    query = callback_query.matches[0].group(1)
    channel_id = int(callback_query.matches[0].group(2))
    page = int(callback_query.matches[0].group(3))
    query = sanitize_query(unquote_plus(query))
    skip = (page - 1) * SEARCH_PAGE_SIZE

    pipeline = build_search_pipeline(query, [channel_id], skip, SEARCH_PAGE_SIZE)
    result = list(files_col.aggregate(pipeline))
    files = result[0]["results"] if result and result[0]["results"] else []
    total_files = result[0]["totalCount"][0]["total"] if result and result[0]["totalCount"] else 0

    channel_info = allowed_channels_col.find_one({'channel_id': channel_id})
    channel_name = channel_info.get('channel_name', str(channel_id)) if channel_info else str(channel_id)

    if not files:
        await callback_query.edit_message_text(
            f"🔍 <b>Search:</b> <code>{query}</code>\n"
            f"❌ <b>No files found</b> in <b>{channel_name}</b>.\n\n"
            f"📝 <i>Tip: Double-check your spelling or try searching the title on <a href='https://www.google.com/search?q={quote_plus(query)}'>Google</a>.</i>",
            parse_mode=enums.ParseMode.HTML,
            disable_web_page_preview=True
        )
        # Send to log channel with the query and user id
        user_id = callback_query.from_user.id
        await bot.send_message(
            LOG_CHANNEL_ID,
            f"🔎 No result for query:\n<code>{query}</code> in <b>{channel_name}</b>\nUser ID: <code>{user_id}</code>"
        )
        await callback_query.answer()
        return
    
    total_pages = (total_files + SEARCH_PAGE_SIZE - 1) // SEARCH_PAGE_SIZE
    text = (
        f"<b>📝 Searched: <code>{query}</b></code>"
        f"\n<b>{channel_name}: Page {page} of {total_pages}</b>"
    )
    buttons = []
    for f in files:
        file_link = encode_file_link(f["channel_id"], f["message_id"])
        size_str = human_readable_size(f.get('file_size', 0))
        btn_text = f"{size_str} ✪ {f.get('file_name')}"
        buttons.append([
            InlineKeyboardButton(btn_text, url=f"https://t.me/{BOT_USERNAME}?start=file_{file_link}")
        ])

    # Pagination controls
    page_buttons = []
    if page > 1:
        page_buttons.append(
            InlineKeyboardButton(
                "⬅️ Prev",
                callback_data=f"search_channel:{quote_plus(query)}:{channel_id}:{page-1}"
            )
        )
    if page < total_pages:
        page_buttons.append(
            InlineKeyboardButton(
                "➡️ Next",
                callback_data=f"search_channel:{quote_plus(query)}:{channel_id}:{page+1}"
            )
        )

    reply_markup = InlineKeyboardMarkup(buttons + ([page_buttons] if page_buttons else []))

    try:
        await callback_query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    except Exception:
        pass
    await callback_query.answer()

@bot.on_message(filters.chat(GROUP_ID) & filters.service)
async def delete_service_messages(client, message):
    try:
        # Greet new members and guide them to use /search
        if message.new_chat_members:
            for user in message.new_chat_members:
                try:
                    reply = await message.reply_text(
                        f"<b>Welcome, {user.first_name}!</b>\n",
                        parse_mode=enums.ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(
                            [
                                [InlineKeyboardButton("Updates Channel", url=f"{UPDATE_CHANNEL_LINK}")]
                            ]
                        )
                    )
                    if reply:
                        bot.loop.create_task(auto_delete_message(message, reply))
                except Exception as e:
                    logger.error(f"Failed to greet new member: {e}")
    except Exception as e:
        logger.error(f"Failed to delete service message: {e}")

@bot.on_message(filters.chat(GROUP_ID) & ~filters.service & ~filters.bot)
async def group_auto_reply_and_delete(client, message):
    try:
        # Compose welcome message (same as service message greeting)
        user = message.from_user
        user_name = user.first_name if user else "User"
        reply = await message.reply_text(
            f"<b>Welcome, {user_name}!</b>\n",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Updates Channel", url=f"{UPDATE_CHANNEL_LINK}")]
                ]
            )
        )
        # Optionally delete the bot's reply after some time (same as your bot.loop.create_task)
        if reply:
            bot.loop.create_task(auto_delete_message(message, reply))
        # Delete the user's original message
    except Exception as e:
        logger.error(f"Failed to reply and delete message: {e}")

@bot.on_message(filters.command("chatop") & filters.private & filters.user(OWNER_ID))
async def chatop_handler(client, message: Message):
    """
    Usage:
      /chatop send <chat_id> (reply to a message to send)
      /chatop del <chat_id> <message_id>
    """
    args = message.text.split(maxsplit=3)
    if len(args) < 3:
        await message.reply_text("Usage:\n/chatop send <chat_id> (reply to a message)\n/chatop del <chat_id> <message_id>")
        return
    op = args[1].lower()
    chat_id = args[2]
    if op == "send":
        if not message.reply_to_message:
            await message.reply_text("Reply to a message to send it.\nUsage: /chatop send <chat_id> (reply to a message)")
            return
        try:
            sent = await message.reply_to_message.copy(int(chat_id))
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
        BotCommand("stats", "(admin only) Show bot stats")
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