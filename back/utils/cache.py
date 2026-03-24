import time
from config import CACHE_TTL_SECONDS

api_cache = {}

def get_cache(cache_key):
    cached = api_cache.get(cache_key)
    if not cached:
        return None, False

    if cached["expires_at"] < time.time():
        api_cache.pop(cache_key, None)
        return None, False

    return cached["value"], True

def set_cache(cache_key, value):
    api_cache[cache_key] = {
        "value": value,
        "expires_at": time.time() + CACHE_TTL_SECONDS,
    }