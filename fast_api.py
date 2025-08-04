import time
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from config import MY_DOMAIN
from db import allowed_channels_col, files_col
from utility import (get_cache_key, search_api_cache, CACHE_TTL, 
                     build_search_pipeline, 
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
            {"_id": 0, "file_name": 1, "file_size": 1, "file_format": 1, "message_id": 1, "date": 1, "channel_id": 1}
        ).sort("_id", -1))
        total = len(files)
    for f in files:
        f["telegram_link"] = generate_telegram_link("YourBotUsername", f["channel_id"], f["message_id"])
        f["file_size"] = human_readable_size(f.get("file_size", 0))
    search_api_cache[cache_key] = (now, files)
    return {"results": files[skip:skip+limit], "total": total}
