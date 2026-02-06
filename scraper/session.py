import threading
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("yemen_scraper.session")

# Thread-local storage for requests session
_thread_local = threading.local()

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
