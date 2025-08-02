import random
import string
import threading
import time

# In-memory mapping: query_id (str) -> (query, timestamp)
query_id_map = {}
QUERY_ID_TTL = 5 * 60

def generate_query_id(length=8):
    """Generate a short random string for query IDs."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def store_query(query):
    """
    Store the query and return its short ID.
    Cleans up expired IDs.
    """
    clean_query_id_map()
    query_id = generate_query_id()
    while query_id in query_id_map:
        query_id = generate_query_id()
    query_id_map[query_id] = (query, time.time())
    return query_id

def get_query_by_id(query_id):
    """
    Retrieve the query string by its ID.
    Returns "" if not found or expired.
    """
    entry = query_id_map.get(query_id)
    if entry:
        query, ts = entry
        if time.time() - ts < QUERY_ID_TTL:
            return query
        else:
            del query_id_map[query_id]
    return ""

def clean_query_id_map():
    """
    Remove expired query IDs from the map.
    """
    now = time.time()
    to_remove = [qid for qid, (q, ts) in query_id_map.items() if now - ts > QUERY_ID_TTL]
    for qid in to_remove:
        del query_id_map[qid]

# Optionally, run periodic cleanup in background (for long-running bots)
def start_query_id_cleanup_thread():
    def loop():
        while True:
            time.sleep(600)
            clean_query_id_map()
    t = threading.Thread(target=loop, daemon=True)
    t.start()