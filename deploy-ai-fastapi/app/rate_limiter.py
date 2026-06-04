import os
from slowapi import Limiter
from slowapi.util import get_remote_address
from dotenv import load_dotenv

load_dotenv()


def _get_key(request) -> str:
    """Rate limit by API key if present, otherwise by IP."""
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return api_key
    return get_remote_address(request)


RATE_LIMIT = os.getenv("RATE_LIMIT", "10/minute")

limiter = Limiter(key_func=_get_key)
