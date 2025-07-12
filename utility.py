import re
import asyncio
import base64
import uuid
import time
import PTN
import requests
from datetime import datetime, timezone, timedelta
from pyrogram.errors import FloodWait
from pyrogram import enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from db import (
    allowed_channels_col,
    users_col,
    tokens_col,
    auth_users_col,
    files_col,
    tmdb_col,
    imgbb_col
)
from config import *
from tmdb import get_movie_by_name, get_tv_by_name, get_by_id
from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from mutagen.mp4 import MP4
from mutagen.id3 import ID3, APIC
from mutagen import File as MutagenFile

# =========================
# Constants & Globals
# =========================

TOKEN_VALIDITY_SECONDS = 24 * 60 * 60  # 24 hours
AUTO_DELETE_SECONDS = 5 * 60


# CACHE FOR SEARCH RESULTS
search_cache = {}
SEARCH_CACHE_TTL = 300  # seconds (5 minutes)

def make_search_cache_key(query, page, channel_id=None):
    return (query.lower(), page, channel_id)

def get_cached_search(query, page, channel_id=None):
    key = make_search_cache_key(query, page, channel_id)
    entry = search_cache.get(key)
    if entry and (time.time() - entry['time'] < SEARCH_CACHE_TTL):
        return entry['files'], entry['total_files']
    if entry:
        del search_cache[key]
    return None, None

def set_cached_search(query, page, channel_id, files, total_files):
    key = make_search_cache_key(query, page, channel_id)
    search_cache[key] = {
        'files': files,
        'total_files': total_files,
        'time': time.time()
    }

def invalidate_search_cache():
    search_cache.clear()


# =========================
# Channel & User Utilities
# =========================

async def get_allowed_channels():
    return [
        doc["channel_id"]
        for doc in allowed_channels_col.find({}, {"_id": 0, "channel_id": 1})
    ]

def add_user(user_id):
    users_col.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id}},
        upsert=True
    )

def authorize_user(user_id):
    """Authorize a user for 24 hours."""
    expiry = datetime.now(timezone.utc) + timedelta(seconds=TOKEN_VALIDITY_SECONDS)
    auth_users_col.update_one(
        {"user_id": user_id},
        {"$set": {"expiry": expiry}},
        upsert=True
    )

def is_user_authorized(user_id):
    """Check if a user is authorized."""
    doc = auth_users_col.find_one({"user_id": user_id})
    if not doc:
        return False
    expiry = doc["expiry"]
    if isinstance(expiry, str):
        try:
            expiry = datetime.fromisoformat(expiry)
        except Exception:
            return False
    if isinstance(expiry, datetime) and expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    if expiry < datetime.now(timezone.utc):
        return False
    return True

# =========================
# Token Utilities
# =========================

def generate_token(user_id):
    """Generate a new access token for a user."""
    token_id = str(uuid.uuid4())
    expiry = datetime.now(timezone.utc) + timedelta(seconds=TOKEN_VALIDITY_SECONDS)
    tokens_col.insert_one({
        "token_id": token_id,
        "user_id": user_id,
        "expiry": expiry,
        "created_at": datetime.now(timezone.utc)
    })
    return token_id

def is_token_valid(token_id, user_id):
    """Check if a token is valid for a user."""
    token = tokens_col.find_one({"token_id": token_id, "user_id": user_id})
    if not token:
        return False
    expiry = token["expiry"]
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    if expiry < datetime.now(timezone.utc):
        tokens_col.delete_one({"_id": token["_id"]})
        return False
    return True

def get_token_link(token_id, bot_username):
    """Generate a Telegram deep link for a token."""
    return f"https://telegram.dog/{bot_username}?start=token_{token_id}"

# =========================
# Link & URL Utilities
# =========================

def generate_telegram_link(bot_username, channel_id, message_id):
    """Generate a base64-encoded Telegram deep link for a file."""
    raw = f"{channel_id}_{message_id}".encode()
    b64 = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    return f"https://telegram.dog/{bot_username}?start=file_{b64}"

def generate_c_link(channel_id, message_id):
    # channel_id must be like -1001234567890
    return f"https://t.me/c/{str(channel_id)[4:]}/{message_id}"

def extract_channel_and_msg_id(link):
    # Only support t.me/c/(-?\d+)/(\d+)
    match = re.search(r"t\.me/c/(-?\d+)/(\d+)", link)
    if match:
        channel_id = int("-100" + match.group(1)) if not match.group(1).startswith("-100") else int(match.group(1))
        msg_id = int(match.group(2))
        return channel_id, msg_id
    raise ValueError("Invalid Telegram message link format. Only /c/ links are supported.")

def shorten_url(long_url):
    """
    Shorten a URL using the configured shortener service.
    Returns the original URL if shortening fails.
    """
    try:
        resp = requests.get(
            f"https://{SHORTERNER_URL}/api?api={URLSHORTX_API_TOKEN}&url={long_url}",
            timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "success" and data.get("shortenedUrl"):
                return data["shortenedUrl"]
        logger.warning(f"Failed to shorten URL, status code: {resp.status_code}")
        return long_url
    except Exception as e:
        logger.error(f"Exception while shortening URL: {e}")
        return long_url
    
# =========================
# File Utilities
# =========================

def upsert_file_info(file_info):
    """Insert or update file info, avoiding duplicates."""
    files_col.update_one(
        {"channel_id": file_info["channel_id"], "message_id": file_info["message_id"]},
        {"$set": file_info},
        upsert=True
    )

def upsert_tmdb_info(tmdb_id, tmdb_type):
    """
    Insert or update TMDB info in tmdb_col.
    Only stores tmdb_id and tmdb_type.
    """
    tmdb_col.update_one(
        {"tmdb_id": tmdb_id, "tmdb_type": tmdb_type},
        {"$setOnInsert": {"tmdb_id": tmdb_id, "tmdb_type": tmdb_type}},
        upsert=True
    )

async def restore_tmdb_photos(bot, start_id=None):
    """
    Restore all TMDB poster photos from the database.
    For each tmdb entry, fetch details and send the poster to UPDATE_CHANNEL_ID.
    """
    query = {}
    if start_id:
        query['_id'] = {'$gt': start_id}
    cursor = tmdb_col.find(query).sort('_id', 1)
    docs = list(cursor)
    for doc in docs:
        tmdb_id = doc.get("tmdb_id")
        tmdb_type = doc.get("tmdb_type")
        try:
            results = await get_by_id(tmdb_type, tmdb_id)
            poster_url = results.get('poster_url')
            trailer = results.get('trailer_url')
            info = results.get('message')
            if poster_url:
                keyboard = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🎥 Trailer", url=trailer)]]) if trailer else None
                await asyncio.sleep(3)  # Avoid hitting API limits
                await safe_api_call(
                    bot.send_photo(
                        UPDATE_CHANNEL_ID,
                        photo=poster_url,
                        caption=info,
                        parse_mode=enums.ParseMode.HTML,
                        reply_markup=keyboard
                    )
                )
        except Exception as e:
            logger.error(f"Error in restore_tmdb_photos for tmdb_id={tmdb_id}: {e}")
            continue  # Continue to the next doc

async def restore_imgbb_photos(bot, start_id=None):
    """
    Restore all TMDB poster photos from the database.
    For each tmdb entry, fetch details and send the poster to UPDATE_CHANNEL_ID.
    """
    query = {}
    if start_id:
        query['_id'] = {'$gt': start_id}
    cursor = imgbb_col.find(query).sort('_id', 1)
    docs = list(cursor)
    for doc in docs:
        pic_url = doc.get("pic_url")
        caption = doc.get("caption")
        try:
            await asyncio.sleep(3) 
            # Avoid hitting API limits
            if pic_url:
                await safe_api_call(
                    bot.send_photo( 
                        UPDATE_CHANNEL2_ID,
                        photo=pic_url,
                        caption=caption,
                        parse_mode=enums.ParseMode.HTML,
                    )
                )
        except Exception as e:
            logger.error(f"Error in restore_imgbb_photos for pic_url={pic_url}: {e}")
            continue

def extract_file_info(message, channel_id=None):
    """Extract file info from a Pyrogram message."""
    caption_name = message.caption.strip() if message.caption else None
    file_info = {
        "channel_id": channel_id if channel_id is not None else message.chat.id,
        "message_id": message.id,
        "file_name": None,
        "file_size": None,
        "file_format": None,
    }
    if message.document:
        file_info["file_name"] = caption_name or message.document.file_name
        file_info["file_size"] = message.document.file_size
        file_info["file_format"] = message.document.mime_type
    elif message.video:
        file_info["file_name"] = caption_name or (message.video.file_name or "video.mp4")
        file_info["file_size"] = message.video.file_size
        file_info["file_format"] = message.video.mime_type
    elif message.audio:
        file_info["file_name"] = caption_name or (message.audio.file_name or "audio.mp3")
        file_info["file_size"] = message.audio.file_size
        file_info["file_format"] = message.audio.mime_type
    elif message.photo:
        file_info["file_name"] = caption_name or "photo.jpg"
        file_info["file_size"] = getattr(message.photo, "file_size", None)
        file_info["file_format"] = "image/jpeg"
    if file_info["file_name"]:
        file_info["file_name"] = remove_extension(file_info["file_name"])
    return file_info

def human_readable_size(size):
    for unit in ['B','KB','MB','GB','TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def remove_extension(caption):
    try:
        # Remove .mkv and .mp4 extensions if present
        cleaned_caption = re.sub(r'\.mkv|\.mp4|\.webm', '', caption)
        return cleaned_caption
    except Exception as e:
        logger.error(e)
        return None

# =========================
# Async/Bot Utilities
# =========================

async def safe_api_call(coro):
    """Utility wrapper to add delay before every bot API call."""
    while True:
        try:
            return await coro
        except FloodWait as e:
            print(f"FloodWait: Sleeping for {e.value} seconds")
            await asyncio.sleep(e.value)
        except Exception:
            raise

async def delete_after_delay(client, chat_id, msg_id):
    await asyncio.sleep(AUTO_DELETE_SECONDS)
    try:
        await safe_api_call(client.delete_messages(chat_id, msg_id))
    except Exception:
        pass

async def extract_tmdb_link(tmdb_url):
    movie_pattern = r'themoviedb\.org\/movie\/(\d+)'
    tv_pattern = r'themoviedb\.org\/tv\/(\d+)'
    collection_pattern = r'themoviedb\.org\/collection\/(\d+)'
    
    if re.search(movie_pattern, tmdb_url):
        tmdb_type = 'movie'
        tmdb_id = int(re.search(movie_pattern, tmdb_url).group(1))
    elif re.search(tv_pattern, tmdb_url):
        tmdb_type = 'tv'
        tmdb_id = int(re.search(tv_pattern, tmdb_url).group(1)) 
    elif re.search(collection_pattern, tmdb_url):
        tmdb_type = 'collection'
        tmdb_id = int(re.search(collection_pattern, tmdb_url).group(1)) 
    return tmdb_type, tmdb_id
        
# =========================
# Queue System for File Processing
# =========================

file_queue = asyncio.Queue()

async def file_queue_worker(bot):
    processing_count = 0
    last_reply_func = None
    while True:
        item = await file_queue.get()
        file_info, reply_func, message, duplicate = item
        processing_count += 1
        if reply_func:
            last_reply_func = reply_func
        try:
            # Check for duplicate by file name in this channel
            existing = files_col.find_one({
                "channel_id": file_info["channel_id"],
                "file_name": file_info["file_name"]
            })
            if existing:
                telegram_link = generate_c_link(file_info["channel_id"], file_info["message_id"])
                if reply_func:
                    if duplicate:
                        await safe_api_call(
                            bot.send_message(
                                LOG_CHANNEL_ID,
                                f"⚠️ Duplicate File.\nLink: {telegram_link}",
                                parse_mode=enums.ParseMode.HTML
                            )
                        )
            else:
                upsert_file_info(file_info)
                if message.audio:
                    audio_path = await bot.download_media(message)
                    thumb_path = await get_audio_thumbnail(audio_path)
                    file_info = f"🎧 <b>{message.audio.title}</b>\n🧑‍🎤 <b>{message.audio.performer}</b>"
                    await bot.send_photo(UPDATE_CHANNEL3_ID, photo=thumb_path, caption=file_info)
                    os.remove(audio_path)
                    os.remove(thumb_path)
                try:
                    if str(file_info["channel_id"]) in TMDB_CHANNEL_ID:
                        title = remove_redandent(file_info["file_name"])
                        parsed_data = PTN.parse(title)
                        title = parsed_data.get("title", "").replace("_", " ").replace("-", " ").replace(":", " ")
                        title = ' '.join(title.split())
                        year = parsed_data.get("year")
                        season = parsed_data.get("season")
                        if season:
                            result = await get_tv_by_name(title, year)
                        else:
                            result = await get_movie_by_name(title, year)
                        tmdb_id, tmdb_type = result['id'], result['media_type']
                        exists = tmdb_col.find_one({"tmdb_id": tmdb_id, "tmdb_type": tmdb_type})
                        if not exists:
                            results = await get_by_id(tmdb_type, tmdb_id)
                            poster_url = results.get('poster_url')
                            trailer = results.get('trailer_url')
                            info = results.get('message')
                            if poster_url:
                                keyboard = InlineKeyboardMarkup(
                                    [[InlineKeyboardButton("🎥 Trailer", url=trailer)]]) if trailer else None
                                await asyncio.sleep(3)
                                await safe_api_call(
                                    bot.send_photo(
                                        UPDATE_CHANNEL_ID,
                                        photo=poster_url,
                                        caption=info,
                                        parse_mode=enums.ParseMode.HTML,
                                        reply_markup=keyboard
                                    )
                                )
                                upsert_tmdb_info(tmdb_id, tmdb_type)

                except Exception as e:
                    logger.error(f'Error processing TMDB info:{e} {file_info["file_name"]}')
        except Exception as e:
            logger.error(f"❌ Error saving file: {e}")
        finally:
            file_queue.task_done()
            if file_queue.empty():
                if processing_count > 1 and last_reply_func:
                    try:
                        logger.info("✅ Done processing file(s) in the queue.")
                    except Exception:
                        pass
                processing_count = 0  # Reset for next batch
                last_reply_func = None  # Reset for next batch

            

# =========================
# Unified File Queueing
# =========================

async def queue_file_for_processing(message, channel_id=None, reply_func=None, duplicate=True):
    try:            
        file_info = extract_file_info(message, channel_id=channel_id)
        if file_info["file_name"]:
            await file_queue.put((file_info, reply_func, message, duplicate))
    except Exception as e:
        if reply_func:
            await safe_api_call(reply_func(f"❌ Error queuing file: {e}"))

def delete_expired_auth_users():
    """
    Delete expired auth users from auth_users_col using 'expiry' field.
    """
    now = datetime.now(timezone.utc)
    result = auth_users_col.delete_many({"expiry": {"$lt": now}})
    logger.info(f"Deleted {result.deleted_count} expired auth users.")

def delete_expired_tokens():
    """
    Delete expired tokens from tokens_col using 'expiry' field.
    """
    now = datetime.now(timezone.utc)
    result = tokens_col.delete_many({"expiry": {"$lt": now}})
    logger.info(f"Deleted {result.deleted_count} expired tokens.")

async def periodic_expiry_cleanup(interval_seconds=3600 * 4):
    """
    Periodically delete expired auth users and tokens.
    """
    while True:
        delete_expired_auth_users()
        delete_expired_tokens()
        await asyncio.sleep(interval_seconds)


def remove_redandent(filename):
    """
    Remove common username patterns from a filename while preserving the content title.

    Args:
        filename (str): The input filename

    Returns:
        str: Filename with usernames removed
    """
    filename = filename.replace("\n", "\\n")

    patterns = [
        r"^@[\w\.-]+?(?=_)",
        r"_@[A-Za-z]+_|@[A-Za-z]+_|[\[\]\s@]*@[^.\s\[\]]+[\]\[\s@]*",  
        r"^[\w\.-]+?(?=_Uploads_)",  
        r"^(?:by|from)[\s_-]+[\w\.-]+?(?=_)",  
        r"^\[[\w\.-]+?\][\s_-]*",  
        r"^\([\w\.-]+?\)[\s_-]*",  
    ]

    result = filename
    for pattern in patterns:
        match = re.search(pattern, result)
        if match:
            result = re.sub(pattern, " ", result)
            break  

    
    result = re.sub(r"^[_\s-]+|[_\s-]+$", " ", result)

    return result

async def get_audio_thumbnail(audio_path, output_dir="downloads"):
    audio = MutagenFile(audio_path)
    thumbnail_path = os.path.join(output_dir, "audio_thumbnail.jpg")

    if isinstance(audio, MP3):
        if audio.tags and isinstance(audio.tags, ID3):
            for tag in audio.tags.values():
                if isinstance(tag, APIC):
                    with open(thumbnail_path, "wb") as img_file:
                        img_file.write(tag.data)
                    return thumbnail_path
    elif isinstance(audio, FLAC):
        if audio.pictures:
            with open(thumbnail_path, "wb") as img_file:
                img_file.write(audio.pictures[0].data)
            return thumbnail_path
    elif isinstance(audio, MP4):
        if audio.tags and 'covr' in audio.tags:
            cover = audio.tags['covr'][0]
            with open(thumbnail_path, "wb") as img_file:
                img_file.write(cover)
            return thumbnail_path
    
    return None