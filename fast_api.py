import time
import PTN
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from config import MY_DOMAIN, BOT_USERNAME, TMDB_CHANNEL_ID
from db import allowed_channels_col, files_col
from tmdb import get_movie_id, get_tv_id, get_info
from utility import (get_cache_key, search_api_cache, CACHE_TTL, 
                     build_search_pipeline, remove_redandent,
                     generate_telegram_link, human_readable_size)


api = FastAPI()
api.add_middleware(
    CORSMiddleware,
    allow_origins=[f"{MY_DOMAIN}"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@api.get("/")
async def root():
    """Greet users on root route."""
    return JSONResponse({"message": "👋 Hello! Welcome to the Sharing Bot"})

@api.get("/api/channels", response_class=JSONResponse)
async def get_channels():
    channels = list(allowed_channels_col.find({}, {"_id": 0, "channel_id": 1, "channel_name": 1}))
    return {"channels": channels}

@api.get("/api/search", response_class=JSONResponse)
async def search_files(
    q: str = Query("", description="Search..."),
    channel_id: int = Query(None, description="Filter by channel_id"),
    limit: int = 10,
    skip: int = 0
):
    if not channel_id:
        return {"results": [], "total": 0}
    cache_key = get_cache_key(q, channel_id)
    now = time.time()
    # Check cache
    if cache_key in search_api_cache:
        ts, cached = search_api_cache[cache_key]
        if now - ts < CACHE_TTL:
            files = cached[skip:skip+limit]
            total = len(cached)
            return {"results": files, "total": total}
        else:
            del search_api_cache[cache_key]
    # Not cached or expired
    if q.strip():
        pipeline = build_search_pipeline(q, [channel_id], 0, 1000)
        results = list(files_col.aggregate(pipeline))
        files = results[0]["results"] if results and results[0].get("results") else []
        total = len(files)
    else:
        files = list(files_col.find(
            {"channel_id": channel_id},
            {"_id": 0, "file_name": 1, "file_size": 1, "ss_url": 1,"file_format": 1, "message_id": 1, "date": 1, "channel_id": 1}
        ).sort("_id", -1))
        total = len(files)

    for f in files:
        f["telegram_link"] = generate_telegram_link(BOT_USERNAME, f["channel_id"], f["message_id"])
        f["file_size"] = human_readable_size(f.get("file_size", 0))
        f["ss_url"] = f.get("ss_url") if "ss_url" in f else None
        if str(f["channel_id"]) in [str(cid) for cid in TMDB_CHANNEL_ID]:
            f["info_button"] = True
        else:
            f["info_button"] = False
    search_api_cache[cache_key] = (now, files)
    return {"results": files[skip:skip+limit], "total": total}


@api.get("/api/tmdb", response_class=JSONResponse)
async def tmdb_info(file_name: str = Query(..., description="File name to search TMDB info for")):
    # Clean filename
    title = remove_redandent(file_name)
    parsed_data = PTN.parse(title)
    title = parsed_data.get("title", "").replace("_", " ").replace("-", " ").replace(":", " ")
    title = ' '.join(title.split())
    year = parsed_data.get("year")
    season = parsed_data.get("season")
    result = None
    if season:
        result = await get_tv_id(title, year)
    else:
        result = await get_movie_id(title, year)
    if not result:
        return {"error": "No TMDB info found for this file name."}
    tmdb_id, tmdb_type = result['id'], result['media_type']
    info = await get_info(tmdb_type, tmdb_id)
    return {"message": info.get("message"), "poster_url": info.get("poster_url"), "trailer_url": info.get("trailer_url")}


