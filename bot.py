# =========================
# Imports
# =========================
import asyncio
import imgbbpy
import base64
from bson import ObjectId
import os
import re
import sys
from datetime import datetime, timezone
from collections import defaultdict

from pyrogram import Client, enums, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
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
    restore_tmdb_photos, build_search_pipeline,
    get_user_link, delete_after_delay,
    restore_imgbb_photos
    )
from db import (db, users_col, 
                tokens_col, 
                files_col, 
                allowed_channels_col, 
                auth_users_col,
                tmdb_col, imgbb_col
                )

from fast_api import api
from tmdb import get_info
import logging
from pyrogram.types import CallbackQuery
import base64
from urllib.parse import unquote_plus
from query_helper import store_query, get_query_by_id, start_query_id_cleanup_thread
# =========================
# Constants & Globals
# ========================= 

TOKEN_VALIDITY_SECONDS = 24 * 60 * 60  # 24 hours token validity
MAX_FILES_PER_SESSION = 100             # Max files a user can access per session
PAGE_SIZE = 10  # Number of files per page
SEARCH_PAGE_SIZE = 10  # You can adjust this

# Initialize Pyrogram bot client
bot = Client(
    "bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    parse_mode=enums.ParseMode.HTML
)

# Track how many files each user has accessed in the current session
user_file_count = defaultdict(int)
copy_lock = asyncio.Lock()
pending_captions = {}

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
    query = re.sub(r"[:',]", "", query)
    query = re.sub(r"[.\s_\-\(\)\[\]]+", " ", query).strip()
    return query

async def imgbb_auto_handler(client, message):
    try:
        text = message.text.strip()
        user_id = message.from_user.id

        if user_id != OWNER_ID:
            return False  # not handled → continue with quer

        # If message contains a URL → ask for caption
        if re.search(r'https?://\S+|www\.\S+', text):
            pending_captions[user_id] = text
            reply = await message.reply_text("📝 Please reply with a caption for this image.")
            bot.loop.create_task(auto_delete_message(message, reply))
            return True   # handled by imgbb

        # If user already sent a URL before → treat current msg as caption
        if user_id in pending_captions:
            image_url = pending_captions.pop(user_id)
            caption = re.sub(r'\.', ' ', text)

            imgbb_client = imgbbpy.AsyncClient(IMGBB_API_KEY)
            try:
                pic = await imgbb_client.upload(url=image_url, name=caption)
                pic_doc = {
                    "pic_url": pic.url,
                    "caption": caption,
                }
                imgbb_col.insert_one(pic_doc)

                # Send to channel
                await bot.send_photo(
                    UPDATE_CHANNEL_ID3,
                    pic.url,
                    caption=f"<b>{caption}</b>"
                )
                await safe_api_call(message.delete())
            except Exception as e:
                await message.reply_text(f"❌ Failed to upload image to imgbb: {e}")
            finally:
                await imgbb_client.close()
            return True   # handled by imgbb

        return False  # not handled, continue with query logic

    except Exception as e:
        await message.reply_text(f"⚠️ An unexpected error occurred: {e}")
        return True  # stop query flow if error
    
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
    try: 
        user_id = message.from_user.id
        user_link = await get_user_link(message.from_user) 
        first_name = message.from_user.first_name or None
        username = message.from_user.username or None
        # Add user (or fetch existing)
        user_doc = add_user(user_id)

        # Log if newly added
        if user_doc["_new"]:
            log_msg = f"👤 New user added:\nID: <code>{user_id}</code>\n"
            if first_name:
                log_msg += f"First Name: <b>{first_name}</b>\n"
            if username:
                log_msg += f"Username: @{username}\n"
            await safe_api_call(
                bot.send_message(LOG_CHANNEL_ID, log_msg, parse_mode=enums.ParseMode.HTML)
            )
        
        # Check if user is blocked
        if user_doc.get("blocked", True):
            return

        # --- Token-based authorization ---
        if len(message.command) == 2 and message.command[1].startswith("token_"):
            if is_token_valid(message.command[1][6:], user_id):
                authorize_user(user_id)
                reply_msg = await safe_api_call(message.reply_text("✅ You are now authorized to access files for 24 hours."))
                await safe_api_call(bot.send_message(LOG_CHANNEL_ID, f"✅ User <b>{user_link}</b> authorized via token."))
            else:
                reply_msg = await safe_api_call(message.reply_text("❌ Invalid or expired token. Please get a new link."))
                await safe_api_call(bot.send_message(LOG_CHANNEL_ID, f"❌ User <b>{user_link}</b> used invalid or expired token."))

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
                short_link = shorten_url(get_token_link(token_id, BOT_USERNAME))
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
            # Get user join date from users_col
            user_doc = users_col.find_one({"user_id": user_id})
            if user_doc and "joined" in user_doc:
                joined_date = user_doc["joined"]
                if isinstance(joined_date, datetime):
                    joined_str = joined_date.strftime("%Y-%m-%d %H:%M")
                else:
                    joined_str = str(joined_date)
            else:
                joined_str = "Unknown"

            buttons = [
                    InlineKeyboardButton(name, callback_data=f"gen_invite:{chan_id}")
                        for name, chan_id in UPDATE_CHANNELS.items()
                        ]
            
            keyboard = [buttons[i:i+2] for i in range(0, len(buttons), 2)]

            welcome_text = (
                f"👋 Hi, {user_link}! 🔰\n\n"
                f"I'm Auto Filter 🤖\n" 
                f"Here you can search files in PM\n" 
                f"Use the below buttons to get updates or send me the name of file to search.\n\n"
                f"🗓️ You joined: <code>{joined_str}</code>\n\n"
                f"❤️ Enjoy your experience here! ❤️"
            )

            reply_msg = await safe_api_call(message.reply_text(
                welcome_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=enums.ParseMode.HTML
            ))
    except Exception as e:
        logger.error(f"⚠️ An unexpected error occurred: {e}")

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

@bot.on_message(filters.private & (filters.document | filters.video | filters.audio) & filters.user(OWNER_ID))
async def del_file_handler(client, message):
    try:
        reply = None
        channel_id = message.forward_from_chat.id if message.forward_from_chat else None
        msg_id = message.forward_from_message_id if message.forward_from_message_id else None
        if channel_id and msg_id:
            file_doc = files_col.find_one({"channel_id": channel_id, "message_id": msg_id})
            if not file_doc:
                reply = await message.reply_text("No file found with that name in the database.")
                return
            result = files_col.delete_one({"channel_id": channel_id, "message_id": msg_id})
            if result.deleted_count > 0:
                reply = await message.reply_text(f"Database record deleted. File name: {file_doc['file_name']}")
        else:
            reply = await message.reply_text("Please forward a file from a channel to delete its record.")
        if reply:
            bot.loop.create_task(auto_delete_message(message, reply))
    except Exception as e:
        await logger.error(f"Error: {e}")

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
            reply = await message.reply_text("Usage: /del <file|tmdb|imgbb> <link> [end_link]")
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
        elif delete_type == "tmdb":
            try:
                # Case: /del tmdb movie 12345
                if end_input:
                    tmdb_type = user_input.lower()
                    tmdb_id = int(end_input.strip())
                else:
                    # Case: /del tmdb <tmdb_link>
                    tmdb_type, tmdb_id = await extract_tmdb_link(user_input)

                result = tmdb_col.delete_one({
                    "tmdb_type": tmdb_type,
                    "tmdb_id": tmdb_id
                })

                if result.deleted_count > 0:
                    reply = await message.reply_text(f"Database record deleted: {tmdb_type}/{tmdb_id}.")
                else:
                    reply = await message.reply_text(f"No TMDB record found with ID {tmdb_type}/{tmdb_id} in the database.")
            except Exception as e:
                reply = await message.reply_text(f"Error: {e}")
        elif delete_type == "imgbb":
            result = imgbb_col.delete_one({"pic_url": user_input})
            if result.deleted_count > 0:
                reply = await message.reply_text(f"Database record deleted : {user_input}")
            else:
                reply = await message.reply_text(f"No record found with: {user_input}")
        else:
            reply = await message.reply_text("Invalid delete type. Use 'file' or 'tmdb' or 'imgbb'.")
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
        elif restore_type == "imgbb":
            await restore_imgbb_photos(bot, start_id)
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
    - Removes users from DB if any exception occurs during message send.
    """
    if message.reply_to_message:
        users = users_col.find({}, {"_id": 0, "user_id": 1})
        total = 0
        failed = 0
        removed = 0

        for user in users:
             try:
                await asyncio.sleep(1)  # Rate limit
                await safe_api_call(message.reply_to_message.copy(user["user_id"]))
                total += 1
             except Exception:
                failed += 1
                users_col.delete_one({"user_id": user["user_id"]})
                removed += 1
                continue
             await asyncio.sleep(3)

        await message.reply_text(f"✅ Broadcasted to {total} users.\n❌ Failed: {failed}\n🗑️ Removed: {removed}")
                                                                                                

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
            f"👤 <b>Total auth users:</b> {total_auth_users} / {total_users}\n"
            f"💾 <b>Files size:</b> {human_readable_size(total_storage)}\n"
            f"📊 <b>Database storage used:</b> {db_storage / (1024 * 1024):.2f} MB\n"
        )

        if not channel_counts:
            text += " <b>No files indexed yet.</b>"
        else:
            for c in channel_counts:
                chan_id = c['_id']
                chan_name = channel_names.get(chan_id, 'Unknown')
                text += f"<b>{chan_name}</b>: {c['count']} files\n"

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
        result = await get_info(tmdb_type, tmdb_id)
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
            keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton("🎥 Trailer", url=trailer)]]) if trailer else None
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

# Handles incoming text messages in private chat that aren't commands
@bot.on_message(filters.private & filters.text & ~filters.command([
    "start", "stats", "add", "rm", "broadcast", "log", "tmdb", 
    "restore", "index", "del", "restart", "chatop", "block"]))
async def instant_search_handler(client, message):
    reply = None
    user_id = message.from_user.id
    try:   
        handled = await imgbb_auto_handler(client, message)
        if handled:
            return

        query = sanitize_query(message.text)
        query_id = store_query(query)

        if not query:
            return
        
        user_doc = add_user(user_id) 
        # Check if user is blocked
        if user_doc.get("blocked", True):
            return
                
        reply = await message.reply_text("Searching please wait ...")

        channels = list(allowed_channels_col.find({}, {"_id": 0, "channel_id": 1, "channel_name": 1}))
        if not channels:
            reply = await safe_api_call(message.reply_text(f"No allowed channels available for search."))
            return

        # Show channel selection buttons
        text = (f"<b>✅ Select a Category</b>")
        buttons = []
        for c in channels:
            chan_id = c["channel_id"]
            chan_name = c.get("channel_name", str(chan_id))
            data = f"search_channel:{query_id}:{chan_id}:1:0"
            buttons.append([
                InlineKeyboardButton(
                    chan_name,
                    callback_data=data
                )
            ])
        reply_markup = InlineKeyboardMarkup(buttons)
        reply = await safe_api_call(
            reply.edit_text(
                text,
                reply_markup=reply_markup,
                parse_mode=enums.ParseMode.HTML
            )
        )
    except Exception as e:
        logger.error(f"Error in instant_search_handler: {e}")
        reply = await reply.edit_text(f"Invalid search query. Please try again with a different query.")
    if reply:
        bot.loop.create_task(auto_delete_message(message, reply))


# Callback handler when user selects a channel to search in
@bot.on_callback_query(filters.regex(r"^search_channel:(.+):(-?\d+):(\d+):(\d+)$"))
async def channel_search_callback_handler(client, callback_query: CallbackQuery):    
    query_id = callback_query.matches[0].group(1)
    query = get_query_by_id(query_id)
    channel_id = int(callback_query.matches[0].group(2))
    page = int(callback_query.matches[0].group(3))
    mode = int(callback_query.matches[0].group(4))
    query = sanitize_query(unquote_plus(query))
    skip = (page - 1) * SEARCH_PAGE_SIZE
    user_link = await get_user_link(callback_query.from_user)

    pipeline = build_search_pipeline(query, [channel_id], skip, SEARCH_PAGE_SIZE)
    result = list(files_col.aggregate(pipeline))
    files = result[0]["results"] if result and result[0]["results"] else []
    total_files = result[0]["totalCount"][0]["total"] if result and result[0]["totalCount"] else 0

    channel_info = allowed_channels_col.find_one({'channel_id': channel_id})
    channel_name = channel_info.get('channel_name', str(channel_id)) if channel_info else str(channel_id)

    if not files:
        await safe_api_call(callback_query.edit_message_text(
            f"<b>❌ No files found for {query}</b>\n"
            "Try like Inception | Loki | Loki S01 | Loki S01E01",
            parse_mode=enums.ParseMode.HTML,
            disable_web_page_preview=True)
        )
        await safe_api_call(bot.send_message(
            LOG_CHANNEL_ID, 
            f"🔎 No result for query:\n<code>{query}</code> in <b>{channel_name}</b>\nUser: {user_link}"
        ))
        await callback_query.answer()
        return

    total_pages = (total_files + SEARCH_PAGE_SIZE - 1) // SEARCH_PAGE_SIZE
    text = (f"<b>📂 Here's what i found for {query}</b>")
    buttons = []
    for f in files:
        file_link = encode_file_link(f["channel_id"], f["message_id"])
        size_str = human_readable_size(f.get('file_size', 0))
        btn_text = f"{size_str} 🔰 {f.get('file_name')}"
        if mode == 0:
            # Normal Get button
            btn = InlineKeyboardButton(
                btn_text,
                callback_data=f"getfile:{file_link}"
            )
        else:
            btn = InlineKeyboardButton(
                btn_text,
                callback_data=f"viewfile:{f['channel_id']}:{f['message_id']}"
            )
        buttons.append([btn])

    # Pagination
    page_buttons = []
    if page > 1:
        prev_data = f"search_channel:{query_id}:{channel_id}:{page - 1}:{mode}"
        page_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=prev_data))
    # Page info button (not clickable)
    page_buttons.append(InlineKeyboardButton(f"📃 {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        next_data = f"search_channel:{query_id}:{channel_id}:{page + 1}:{mode}"
        page_buttons.append(InlineKeyboardButton("➡️ Next", callback_data=next_data))

    toggle_mode = 1 if mode == 0 else 0
    toggle_icon = "👁️ View" if mode == 0 else "📲 Send"
    toggle_data = f"search_channel:{query_id}:{channel_id}:{page}:{toggle_mode}"
    page_buttons.append(InlineKeyboardButton(toggle_icon, callback_data=toggle_data))

    reply_markup = InlineKeyboardMarkup(buttons + ([page_buttons] if page_buttons else []))

    try:
        await safe_api_call(callback_query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        ))
    except Exception:
        pass
    await callback_query.answer()


# Callback handler to send file to user
@bot.on_callback_query(filters.regex(r"^getfile:(.+)$"))
async def send_file_callback(client, callback_query: CallbackQuery):
    file_link = callback_query.matches[0].group(1)
    user_id = callback_query.from_user.id
    try:
        if not is_user_authorized(user_id):
            now = datetime.now(timezone.utc)
            token_doc = tokens_col.find_one({
                "user_id": user_id,
                "expiry": {"$gt": now}
            })
            token_id = token_doc["token_id"] if token_doc else generate_token(user_id)
            short_link = shorten_url(get_token_link(token_id, BOT_USERNAME))
            reply = await safe_api_call(callback_query.edit_message_text(
                text=(
                    "🎉 Just one step away!\n\n"
                    "To access files, please contribute a little by clicking the link below. "
                    "It’s completely free for you — and it helps keep the bot running by supporting the server costs. ❤️\n\n"
                    "Click below to get 24-hour access:"
                ),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔓 Get Access Link", url=short_link)]]
                )
            ))
            bot.loop.create_task(delete_after_delay(reply))
            return

        if user_file_count[user_id] >= MAX_FILES_PER_SESSION:
            await safe_api_call(callback_query.answer("Limit reached. Please take a break.", show_alert=True))
            return

        padding = '=' * (-len(file_link) % 4)
        decoded = base64.urlsafe_b64decode(file_link + padding).decode()
        channel_id_str, msg_id_str = decoded.split("_")
        channel_id = int(channel_id_str)
        msg_id = int(msg_id_str)

        file_doc = files_col.find_one({"channel_id": channel_id, "message_id": msg_id})
        if not file_doc:
            await callback_query.answer("File not found.", show_alert=True)
            return

        send_file = await safe_api_call(client.copy_message(
            chat_id=user_id,
            caption=f'<b>{file_doc["file_name"]}</b>',
            from_chat_id=file_doc["channel_id"],
            message_id=file_doc["message_id"]
        ))
        user_file_count[user_id] += 1
        await safe_api_call(callback_query.answer(
            f"File will be auto deleted in 5 minutes — forward it.", show_alert=True))
        bot.loop.create_task(delete_after_delay(send_file))
    except Exception as e:
        await callback_query.answer(f"Failed: {e}", show_alert=True)

@bot.on_callback_query(filters.regex(r"^viewfile:(-?\d+):(\d+)$"))
async def view_file_callback_handler(client, callback_query: CallbackQuery):
    channel_id = int(callback_query.matches[0].group(1))
    message_id = int(callback_query.matches[0].group(2))

    # Fetch file from DB
    file_doc = files_col.find_one({"channel_id": channel_id, "message_id": message_id})
    if not file_doc:
        await callback_query.answer("❌ File not found!", show_alert=True)
        return

    file_name = file_doc.get("file_name", "Unknown file")

    # Show as a toast (non-blocking notification)
    await callback_query.answer(file_name, show_alert=True)

    
@bot.on_callback_query(filters.regex(r"^noop$"))
async def noop_callback_handler(client, callback_query: CallbackQuery):
    await callback_query.answer()  # Instantly respond, does nothing

@bot.on_callback_query(filters.regex(r"^gen_invite:(-?\d+)$"))
async def generate_and_send_invite(client, callback_query: CallbackQuery):
    """
    Generates a custom invite link for the requested update channel,
    sends it to the user, and revokes it after auto_delete_message completes.
    """
    try:

        chan_id = int(callback_query.matches[0].group(1))

        invite = await bot.create_chat_invite_link(
            chan_id, creates_join_request=True
        )

        reply = await callback_query.edit_message_text(
            f"🔗 Here is your invite link:\n{invite.invite_link}\n\nThis link will be revoked soon.",
            disable_web_page_preview=True
        )
        await callback_query.answer()

        async def cleanup():
            await delete_after_delay(reply)
            try:
                await bot.revoke_chat_invite_link(chan_id, invite.invite_link)
            except Exception:
                pass

        bot.loop.create_task(cleanup())

    except Exception as e:
        logger.error(f"Failed generate_and_send_invite: {e}")

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

@bot.on_message(filters.command("block") & filters.private & filters.user(OWNER_ID))
async def block_user_handler(client, message: Message):
    """
    Handles the /block command for the owner.
    Usage: /block <user_id>
    Blocks a user by adding their user_id to the auth_users_col with a 'blocked' flag.
    """
    args = message.text.split()
    if len(args) != 2:
        await message.reply_text("Usage: /block <user_id>")
        return
    try:
        user_id = int(args[1])
        users_col.update_one(
            {"user_id": user_id},
            {"$set": {"blocked": True}},
            upsert=True
        )
        await message.reply_text(f"✅ User {user_id} has been blocked.")
    except Exception as e:
        await message.reply_text(f"❌ Failed to block user: {e}")


'''
@bot.on_chat_join_request()
async def approve_join_request_handler(client, join_request):
    """
    Automatically approves join requests for channels where the bot is admin.
    """
    try:
        await client.approve_chat_join_request(join_request.chat.id, join_request.from_user.id)
        # Optionally, you can log or notify somewhere:
        # await bot.send_message(LOG_CHANNEL_ID, f"Approved join request for {join_request.from_user.mention} in {join_request.chat.title}")
    except Exception as e:
        logger.error(f"Failed to approve join request: {e}")  
'''
# =========================
# Main Entrypoint
# =========================

async def main():
    """
    Starts the bot and FastAPI server.
    """
    # Set bot commands
    await bot.start()

    bot.loop.create_task(start_fastapi())
    bot.loop.create_task(file_queue_worker(bot))  # Start the queue worker
    bot.loop.create_task(periodic_expiry_cleanup())
    start_query_id_cleanup_thread()

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