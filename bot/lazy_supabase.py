from typing import Any
from config import SUPABASE_URL, SUPABASE_KEY

class LazySupabase:
    def __init__(self):
        self._client = None

    def _init_client(self):
        if self._client is None:
            from supabase import create_client
            self._client = create_client(SUPABASE_URL, SUPABASE_KEY)
        return self._client

    def __getattr__(self, name: str) -> Any:
        return getattr(self._init_client(), name)

    def reset(self):
        """Force re-initialization of the Supabase client on next use."""
        try:
            self._client = None
        except Exception:
            self._client = None

supabase = LazySupabase()
