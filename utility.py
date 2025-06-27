import re
import os
import asyncio
import base64
import uuid
import time
import requests
from functools import wraps
from datetime import datetime, timezone, timedelta
from pyrogram.errors import FloodWait
from pyrogram import enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import logger

from db import (
    allowed_channels_col,
    users_col,
    tokens_col,
    auth_users_col,
    files_col,
    tmdb_col,
)
from config import *
from tmdb import get_movie_by_name, get_tv_by_name, get_by_id

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

def upsert_tmdb_info(tmdb_id, tmdb_type, season=None, episode=None):
    """
    Insert or update TMDB info in tmdb_col.
    If the same tmdb_id and tmdb_type exists, update season_info array.
    """
    season_info = {}
    if season is not None:
        season_info["season"] = int(season)
    if episode is not None:
        season_info["episode"] = int(episode)
    update = {
        "$setOnInsert": {"tmdb_id": tmdb_id, "tmdb_type": tmdb_type}
    }
    if season_info:
        update["$addToSet"] = {"season_info": season_info}
    tmdb_col.update_one(
        {"tmdb_id": tmdb_id, "tmdb_type": tmdb_type},
        update,
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
        season_infos = doc.get("season_info", [])

        # If no season_info, just call once with None values
        if not season_infos:
            season_episode_list = [(None, None)]
        else:
            season_episode_list = [(s.get("season"), s.get("episode")) for s in season_infos]

        for season, episode in season_episode_list:
            try:
                results = await get_by_id(tmdb_type, tmdb_id, season, episode)
                poster_url = results.get('poster_url')
                trailer = results.get('trailer_url')
                info = results.get('message')
                if poster_url:
                    keyboard = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🎥 Trailer", url=trailer)]]) if trailer else None
                    await asyncio.sleep(3) 
                    # Avoid hitting API limits
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
                logger.error(f"Error in restore_tmdb_photos for tmdb_id={tmdb_id}, season={season}, episode={episode}: {e}")
                continue  # Continue to the next (season, episode) or doc

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

async def extract_movie_info(caption):
    try:
        # Extract season and episode (e.g., S01, S02, E01, E02)
        season_match = re.search(r'\bS(\d{1,2})\b', caption, re.IGNORECASE)
        episode_match = re.search(r'\bE(\d{1,2})\b', caption, re.IGNORECASE)

        season = f"{int(season_match.group(1)):02d}" if season_match else None
        episode = f"{int(episode_match.group(1)):02d}" if episode_match else None

        current_year = datetime.now().year + 2  # Allow a couple of years ahead for upcoming movies
        # Exclude 4-digit numbers followed by 'p' (like 1080p, 2160p, 720p)
        years = [
            y for y in re.findall(r'(\d{4})', caption)
            if 1900 <= int(y) <= current_year and not re.search(rf'{y}p', caption, re.IGNORECASE)
        ]
        release_year = years[-1] if years else None

        # Get everything before the last year
        if release_year:
            movie_name = caption.rsplit(release_year, 1)[0]
            movie_name = movie_name.replace('.', ' ').replace('(', '').replace(')', '').strip()
            movie_name = re.split(r'\s*A\s*K\s*A\s*', movie_name, flags=re.IGNORECASE)[0].strip()
        else:
            movie_name = caption

        # Return all info separately
        return movie_name, release_year, season, episode
    except Exception as e:
        logger.error(f"Extract Movie info Error : {e}")
    return None, None, None, None

        
# =========================
# Queue System for File Processing
# =========================

file_queue = asyncio.Queue()

async def file_queue_worker(bot):
    processing_count = 0
    last_reply_func = None
    while True:
        item = await file_queue.get()
        file_info, reply_func, message = item
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
                    await safe_api_call(
                        bot.send_message(
                            LOG_CHANNEL_ID,
                            f"⚠️ Duplicate File.\nLink: {telegram_link}",
                            parse_mode=enums.ParseMode.HTML
                        )
                    )
            else:
                upsert_file_info(file_info)
                try:
                    if str(file_info["channel_id"]) in TMDB_CHANNEL_ID:
                        title, release_year, season, episode = await extract_movie_info(file_info["file_name"])
                        if season:
                            result = await get_tv_by_name(title, release_year)
                        else:
                            result = await get_movie_by_name(title, release_year)

                        tmdb_id, tmdb_type = result['id'], result['media_type'] 
                        results = await get_by_id(tmdb_type, tmdb_id, season, episode)
                        poster_url = results.get('poster_url')
                        trailer = results.get('trailer_url')
                        info = results.get('message')

                        
                        if poster_url:
                            # Check if this tmdb_id, tmdb_type, season, episode already exists in tmdb_col
                            query = {"tmdb_id": tmdb_id, "tmdb_type": tmdb_type}
                            if season is not None:
                                query["season_info"] = {"$elemMatch": {"season": int(season)}}
                                if episode is not None:
                                    query["season_info"]["$elemMatch"]["episode"] = int(episode)
                            elif episode is not None:
                                query["season_info"] = {"$elemMatch": {"episode": int(episode)}}

                            exists = tmdb_col.find_one(query)
                            if not exists:
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
                            upsert_tmdb_info(tmdb_id, tmdb_type, season, episode)

                except Exception as e:
                    logger.error(f"Error processing TMDB info:{e}")
                    if reply_func:
                        await safe_api_call(
                            bot.send_message(
                                LOG_CHANNEL_ID,
                                f'❌ Error processing TMDB info: {file_info["file_name"]}/n/n{e}',
                                parse_mode=enums.ParseMode.HTML
                            )
                        )
        except Exception as e:
            if reply_func:
                await safe_api_call(reply_func(f"❌ Error saving file: {e}"))
        finally:
            file_queue.task_done()
            if file_queue.empty():
                if processing_count > 1 and last_reply_func:
                    try:
                        await safe_api_call(
                            last_reply_func(
                                f"✅ Done processing {processing_count} file(s) in the queue."
                            )
                        )
                    except Exception:
                        pass
                processing_count = 0  # Reset for next batch
                last_reply_func = None  # Reset for next batch

            

# =========================
# Unified File Queueing
# =========================

async def queue_file_for_processing(message, channel_id=None, reply_func=None):
    try:            
        file_info = extract_file_info(message, channel_id=channel_id)
        if file_info["file_name"]:
            await file_queue.put((file_info, reply_func, message))
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


