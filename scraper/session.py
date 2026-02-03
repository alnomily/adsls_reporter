import threading
import os
import logging
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from supabase import create_client

from config import SUPABASE_URL, SUPABASE_KEY

logger = logging.getLogger("yemen_scraper.session")

# Thread-local storage for session and supabase client
_thread_local = threading.local()

# Predictor globals are created in processor to avoid import cycles

def get_supabase():
    """Create or return a thread-local Supabase client."""
    if getattr(_thread_local, "supabase", None) is None:
        try:
            _thread_local.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            logger.debug("Supabase client created for thread %s", threading.get_ident())
        except Exception:
            logger.exception("Failed to create supabase client")
            _thread_local.supabase = None
    return _thread_local.supabase


def get_session(pool_size: int = 20, retries: int = 2, backoff: float = 0.5) -> requests.Session:
    """Return a thread-local requests.Session configured with pooling and retries."""
    if getattr(_thread_local, "session", None) is None:
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; YemenNetScraper/1.0)"})
        s.trust_env = False
        retry = Retry(
            total=retries,
            backoff_factor=backoff,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
        )
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=retry)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _thread_local.session = s
    return _thread_local.session
