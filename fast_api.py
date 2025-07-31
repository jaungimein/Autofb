import httpx
import time
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from config import MY_DOMAIN, TMDB_API_KEY, BOT_USERNAME
from db import tmdb_col, files_col
from fastapi import HTTPException
from utility import generate_telegram_link

# In-memory cache: {(tmdb_type, tmdb_id, page): (timestamp, data)}
tmdb_cache = {}

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

@api.get("/tmdb/all/")
async def get_all_tmdb_details(
    page: int = Query(1, ge=1)
):
    """
    Fetch all tmdb_id/type from MongoDB, get details from TMDB API, paginate (10/page), cache for 5 min.
    """
    cache_key = ("all_tmdb", page)
    now = time.time()
    if cache_key in tmdb_cache:
        ts, data = tmdb_cache[cache_key]
        if now - ts < 300:
            return data

    # Get all tmdb_id/type from MongoDB
    docs = list(tmdb_col.find({}, {"_id": 0, "tmdb_id": 1, "tmdb_type": 1}).sort([("_id", -1)]))
    total = len(docs)
    start = (page - 1) * 10
    end = start + 10
    page_docs = docs[start:end]

    results = []
    async with httpx.AsyncClient() as client:
        for doc in page_docs:
            tmdb_id = doc["tmdb_id"]
            tmdb_type = doc["tmdb_type"]
            if tmdb_type == "movie":
                url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
            elif tmdb_type == "tv":
                url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
            else:
                continue
            resp = await client.get(url)
            if resp.status_code != 200:
                continue
            item = resp.json()
            poster_url = f"https://image.tmdb.org/t/p/w780{item.get('poster_path')}" if item.get("poster_path") else None
            results.append({
                "tmdb_id": tmdb_id,
                "tmdb_type": tmdb_type,
                "title": item.get("title") or item.get("name"),
                "vote_average": item.get("vote_average"),
                "poster_url": poster_url
            })

    paginated = {
        "page": page,
        "total": total,
        "results": results
    }
    tmdb_cache[cache_key] = (now, paginated)
    return paginated

@api.get("/files/by_tmdb/")
async def get_files_by_tmdb(
    tmdb_id: int,
    tmdb_type: str
):
    """
    Get all files from files_col matching tmdb_id and tmdb_type.
    Returns TMDB info (once) and a list of files (file_name, file_size, link).
    """
    files = list(files_col.find(
        {"tmdb_id": tmdb_id, "tmdb_type": tmdb_type},
        {"_id": 0, "file_name": 1, "file_size": 1, "channel_id": 1, "message_id": 1}
    ))
    if not files:
        raise HTTPException(status_code=404, detail="No files found for this TMDB ID and type.")

    # Fetch TMDB details once
    if tmdb_type == "movie":
        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US&append_to_response=videos"
    elif tmdb_type == "tv":
        url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US&append_to_response=videos"
    else:
        raise HTTPException(status_code=400, detail="Invalid tmdb_type.")

    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            raise HTTPException(status_code=404, detail="TMDB details not found.")
        item = resp.json()

    # Extract trailer (YouTube)
    trailer_url = None
    videos = item.get("videos", {}).get("results", [])
    for v in videos:
        if v.get("site") == "YouTube" and v.get("type") == "Trailer":
            trailer_url = f"https://www.youtube.com/watch?v={v.get('key')}"
            break

    tmdb_info = {
        "title": item.get("title") or item.get("name"),
        "release_date": item.get("release_date") or item.get("first_air_date"),
        "vote_average": f"{item.get('vote_average', 0):.1f}" if item.get("vote_average") else None,
        "overview": item.get("overview"),
        "poster_url": f"https://image.tmdb.org/t/p/w780{item.get('poster_path')}" if item.get("poster_path") else None,
        "trailer_url": trailer_url
    }

    results = []
    for f in files:
        link = generate_telegram_link(BOT_USERNAME, f["channel_id"], f["message_id"])
        results.append({
            "file_name": f["file_name"],
            "file_size": f.get("file_size"),
            "link": link
        })

    return {
        "tmdb_info": tmdb_info,
        "results": results
    }


@api.get("/files/search/")
async def search_files(
    query: str,
    page: int = Query(1, ge=1)
):
    """
    Search files_col for file_name using MongoDB Atlas Search (punctuation-insensitive, typo-tolerant).
    For each result, fetch TMDB details and return in the same format as /tmdb/all/.
    Paginate results (10 per page). Results are deduplicated by (tmdb_id, tmdb_type) and sorted by search score.
    """
    cache_key = ("files_search_atlas", query.lower(), page)
    now = time.time()
    if cache_key in tmdb_cache:
        ts, data = tmdb_cache[cache_key]
        if now - ts < 300:
            return data

    # Main pipeline for paginated, deduplicated, and score-sorted results
    pipeline = [
        {
            "$search": {
                "index": "default",
                "text": {
                    "query": query,
                    "path": "file_name"
                }
            }
        },
        {
            "$project": {
                "tmdb_id": 1,
                "tmdb_type": 1,
                "score": {"$meta": "searchScore"}
            }
        },
        {
            "$group": {
                "_id": {"tmdb_id": "$tmdb_id", "tmdb_type": "$tmdb_type"},
                "tmdb_id": {"$first": "$tmdb_id"},
                "tmdb_type": {"$first": "$tmdb_type"},
                "score": {"$max": "$score"}
            }
        },
        {
            "$sort": {"score": -1}
        },
        {"$skip": (page - 1) * 10},
        {"$limit": 10},
        {"$project": {"_id": 0, "tmdb_id": 1, "tmdb_type": 1, "score": 1}}
    ]

    # Pipeline to count total unique (tmdb_id, tmdb_type) matches
    count_pipeline = [
        {
            "$search": {
                "index": "default",
                "text": {
                    "query": query,
                    "path": "file_name"
                }
            }
        },
        {
            "$group": {
                "_id": {"tmdb_id": "$tmdb_id", "tmdb_type": "$tmdb_type"}
            }
        },
        {"$count": "total"}
    ]

    docs = list(files_col.aggregate(pipeline))
    count_result = list(files_col.aggregate(count_pipeline))
    total = count_result[0]["total"] if count_result else 0

    results = []
    async with httpx.AsyncClient() as client:
        for doc in docs:
            tmdb_id = doc.get("tmdb_id")
            tmdb_type = doc.get("tmdb_type")
            if not tmdb_id or not tmdb_type:
                continue
            if tmdb_type == "movie":
                url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
            elif tmdb_type == "tv":
                url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
            else:
                continue
            resp = await client.get(url)
            if resp.status_code != 200:
                continue
            item = resp.json()
            poster_url = f"https://image.tmdb.org/t/p/w780{item.get('poster_path')}" if item.get("poster_path") else None
            results.append({
                "tmdb_id": tmdb_id,
                "tmdb_type": tmdb_type,
                "title": item.get("title") or item.get("name"),
                "vote_average": item.get("vote_average"),
                "poster_url": poster_url
            })

    paginated = {
        "page": page,
        "total": total,  # Accurate unique TMDB entries count
        "results": results
    }
    tmdb_cache[cache_key] = (now, paginated)
    return paginated