import re
import aiohttp
import imdb
from config import TMDB_API_KEY, logger

POSTER_BASE_URL = 'https://image.tmdb.org/t/p/original'

def get_cast_and_crew(tmdb_type, movie_id):
    """
    Fetches the cast and crew details (starring actors and director) for a movie or TV show.
    """
    import requests
    cast_crew_url = f'https://api.themoviedb.org/3/{tmdb_type}/{movie_id}/credits?api_key={TMDB_API_KEY}&language=en-US'
    response = requests.get(cast_crew_url)
    cast_crew_data = response.json()

    starring = [member['name'] for member in cast_crew_data.get('cast', [])[:5]]
    director = next((member['name'] for member in cast_crew_data.get('crew', []) if member['job'] == 'Director'), 'N/A')
    return {"starring": starring, "director": director}

def get_imdb_details(imdb_id):
    ia = imdb.IMDb()
    try:
        movie = ia.get_movie(imdb_id.replace('tt', ''))
        if not movie:
            return {}
        return {
            "rating": movie.get('rating'),
            "plot": movie.get('plot', [None])[0]
        }
    except Exception as e:
        logger.error(f"IMDbPY error: {e}")
        return {}

def format_tmdb_info(tmdb_type, movie_id, data, season, episode):
    cast_crew = get_cast_and_crew(tmdb_type, movie_id)

    if tmdb_type == 'movie':
        imdb_id = data.get('imdb_id')
        imdb_info = get_imdb_details(imdb_id) if imdb_id else {}

        plot = imdb_info.get('plot')
        title = data.get('title')
        duration = format_duration(data.get('runtime'))
        language = ", ".join(lang.get('name', '') for lang in data.get('spoken_languages', [])) if data.get('spoken_languages') else ""
        genre = extract_genres(data)
        genre_tags = " ".join([genre_tag_with_emoji(g) for g in genre])
        release_date = data.get('release_date', '')[:10] if data.get('release_date') else ""
        director = cast_crew.get('director')
        starring = ", ".join(cast_crew.get('starring', [])) if cast_crew.get('starring') else None
        vote_average = data.get('vote_average', None)
        vote_average_str = f"{vote_average:.1f}" if vote_average is not None else None

        if release_date and len(release_date) == 10:
            from datetime import datetime
            try:
                release_date_fmt = datetime.strptime(release_date, "%Y-%m-%d").strftime("%b %d, %Y")
            except Exception:
                release_date_fmt = release_date
        else:
            release_date_fmt = release_date

        message = (
            f"<b>🎬Name:</b> {title}\n"
        )
        message += f"<b>⭐Rating:</b> {vote_average_str}/10\n" if vote_average_str is not None else ""
        message += f"<b>⏳Length:</b> {duration}\n" if duration else ""
        message += f"<b>🅰️Language:</b> {language}\n" if language else ""
        message += f"<b>⚙️Genres:</b> {genre_tags}\n" if genre_tags else ""
        message += f"<b>📅Released:</b> {release_date_fmt}\n" if release_date_fmt else ""
        message += "\n"
        message += f"<b>📝Plot:</b> {plot}\n" if plot else ""
        message += f"<b>🎥Director:</b> {director}\n" if director else ""
        message += f"<b>⭐Cast:</b> {starring}\n" if starring else ""

        return message.strip()

    elif tmdb_type == 'tv':
        imdb_id = get_tv_imdb_id_sync(movie_id)
        imdb_info = get_imdb_details(imdb_id) if imdb_id else {}

        plot = imdb_info.get('plot') if imdb_info.get('plot') else data.get('overview')
        title = data.get('name')
        language = ", ".join(lang.get('name', '') for lang in data.get('spoken_languages', [])) if data.get('spoken_languages') else ""
        genre = extract_genres(data)
        genre_tags = " ".join([genre_tag_with_emoji(g) for g in genre])
        release_date = data.get('first_air_date', '')[:10] if data.get('first_air_date') else ""
        director = ", ".join([creator['name'] for creator in data.get('created_by', [])]) if data.get('created_by') else None
        starring = ", ".join(cast_crew.get('starring', [])) if cast_crew.get('starring') else None
        vote_average = data.get('vote_average', None)
        vote_average_str = f"{vote_average:.1f}" if vote_average is not None else None

        if release_date and len(release_date) == 10:
            from datetime import datetime
            try:
                release_date_fmt = datetime.strptime(release_date, "%Y-%m-%d").strftime("%b %d, %Y")
            except Exception:
                release_date_fmt = release_date
        else:
            release_date_fmt = release_date

        message = (
            f"<b>🎬Name:</b> {title}\n"
            f"<b>📺Season:</b> S{season}\n" if season else ""
            f"<b>📺Episode:</b> E{episode}\n" if episode else ""
        )
        message += f"<b>⭐Rating:</b> {vote_average_str}/10\n" if vote_average_str is not None else ""
        message += f"<b>🅰️Language:</b> {language}\n" if language else ""
        message += f"<b>⚙️Genres:</b> {genre_tags}\n" if genre_tags else ""
        message += f"<b>📅Released:</b> {release_date_fmt}\n" if release_date_fmt else ""
        message += "\n"
        message += f"<b>📝Plot:</b> {plot}\n" if plot else ""
        message += f"<b>🎥Director:</b> {director}\n" if director else ""
        message += f"<b>⭐Cast:</b> {starring}\n" if starring else ""

        return message.strip()
    else:
        return "Unknown type. Unable to format information."

def get_tv_imdb_id_sync(tv_id):
    import requests
    url = f"https://api.themoviedb.org/3/tv/{tv_id}/external_ids?api_key={TMDB_API_KEY}"
    resp = requests.get(url)
    data = resp.json()
    return data.get("imdb_id")

async def get_by_id(tmdb_type, tmdb_id, season=None, episode=None):
    api_url = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
    image_url = f'https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id}/images?api_key={TMDB_API_KEY}&language=en-US&include_image_language=en'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as detail_response:
                data = await detail_response.json()
                async with session.get(image_url) as movie_response:
                    images = await movie_response.json()
                message = format_tmdb_info(tmdb_type, tmdb_id, data, season, episode)
                poster_path = data.get('poster_path', None)
                if 'backdrops' in images and images['backdrops']:
                    poster_path = images['backdrops'][0]['file_path']
                elif 'posters' in images and images['posters']:
                    poster_path = images['posters'][0]['file_path']
                poster_url = f"https://image.tmdb.org/t/p/original{poster_path}" if poster_path else None

                video_url = f'https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id}/videos?api_key={TMDB_API_KEY}'
                async with session.get(video_url) as video_response:
                    video_data = await video_response.json()
                    trailer_url = None
                    for video in video_data.get('results', []):
                        if video['site'] == 'YouTube' and video['type'] == 'Trailer':
                            trailer_url = f"https://www.youtube.com/watch?v={video['key']}"
                            break

                return {"message": message, "poster_url": poster_url, "trailer_url": trailer_url}
    except aiohttp.ClientError as e:
        print(f"Error fetching TMDB data: {e}")
    return {"message": f"Error: {str(e)}", "poster_url": None}

def truncate_overview(overview):
    """
    Truncate the overview if it exceeds the specified limit.

    Args:
    - overview (str): The overview text from the API.

    Returns:
    - str: Truncated overview with an ellipsis if it exceeds the limit.
    """
    MAX_OVERVIEW_LENGTH = 600  # Define your maximum character length for the summary
    if len(overview) > MAX_OVERVIEW_LENGTH:
        return overview[:MAX_OVERVIEW_LENGTH] + "..."
    return overview

async def get_movie_by_name(movie_name, release_year=None):
    tmdb_search_url = f'https://api.themoviedb.org/3/search/movie?api_key={TMDB_API_KEY}&query={movie_name}'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(tmdb_search_url) as search_response:
                search_data = await search_response.json()
                if search_data.get('results'):
                    results = search_data['results']
                    if release_year:
                        # Filter by release year if provided
                        results = [
                            result for result in results
                            if 'release_date' in result and result['release_date'] and result['release_date'][:4] == str(release_year)
                        ]
                    if results:
                        result = results[0]
                        return {
                            "id": result['id'],
                            "media_type": "movie"
                        }
        return None
    except Exception as e:
        logger.error(f"Error fetching TMDb movie by name: {e}")
        return

async def get_tv_by_name(tv_name, first_air_year=None):
    tmdb_search_url = f'https://api.themoviedb.org/3/search/tv?api_key={TMDB_API_KEY}&query={tv_name}'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(tmdb_search_url) as search_response:
                search_data = await search_response.json()
                if search_data.get('results'):
                    results = search_data['results']
                    if first_air_year:
                        # Filter by first air year if provided
                        results = [
                            result for result in results
                            if 'first_air_date' in result and result['first_air_date'] and result['first_air_date'][:4] == str(first_air_year)
                        ]
                    if results:
                        result = results[0]
                        return {
                            "id": result['id'],
                            "media_type": "tv"
                        }
        return None
    except Exception as e:
        logger.error(f"Error fetching TMDb TV by name: {e}")
        return
    
GENRE_EMOJI_MAP = {
    "Action": "🥊", "Adventure": "🌋", "Animation": "🎬", "Comedy": "😂",
    "Crime": "🕵️", "Documentary": "🎥", "Drama": "🎭", "Family": "👨‍👩‍👧‍👦",
    "Fantasy": "🧙", "History": "📜", "Horror": "👻", "Music": "🎵",
    "Mystery": "🕵️‍♂️", "Romance": "❤️", "ScienceFiction": "🤖",
    "Sci-Fi": "🤖", "SciFi": "🤖", "TV Movie": "📺", "Thriller": "🔪",
    "War": "⚔️", "Western": "🤠", "Sport": "🏆", "Biography": "📖"
}

def clean_genre_name(genre):
    return re.sub(r'[^A-Za-z0-9]', '', genre)

def genre_tag_with_emoji(genre):
    clean_name = clean_genre_name(genre)
    emoji = GENRE_EMOJI_MAP.get(clean_name, "")
    return f"#{clean_name}{' ' + emoji if emoji else ''}"

def extract_genres(data):
    genres = []
    for genre in data.get('genres', []):
        # Split genre names containing '&' into separate genres
        if '&' in genre['name']:
            parts = [g.strip() for g in genre['name'].split('&')]
            genres.extend(parts)
        else:
            genres.append(genre['name'])
    return genres

def format_duration(duration):
    """
    Format duration in minutes to 'Xh YYmin' format.
    """
    try:
        mins = int(duration)
        hours = mins // 60
        mins = mins % 60
        return f"{hours}h {mins:02d}min" if hours else f"{mins}min"
    except Exception:
        return duration or ""
    
async def get_tv_imdb_id(tv_id):
    url = f"https://api.themoviedb.org/3/tv/{tv_id}/external_ids?api_key={TMDB_API_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            return data.get("imdb_id")
