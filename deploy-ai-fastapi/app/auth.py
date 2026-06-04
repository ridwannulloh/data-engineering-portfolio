import os
import logging
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

# Load valid API keys from environment
_raw = os.getenv("API_KEYS", "")
VALID_API_KEYS: set[str] = {k.strip() for k in _raw.split(",") if k.strip()}


async def verify_api_key(api_key: str = Security(API_KEY_HEADER)) -> str:
    """Validate the API key from the request header."""
    if api_key is None:
        logger.warning("Request missing API key header")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key. Provide it via X-API-Key header.",
        )
    if api_key not in VALID_API_KEYS:
        logger.warning("Invalid API key attempt: %s...", api_key[:8])
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key.",
        )
    return api_key
