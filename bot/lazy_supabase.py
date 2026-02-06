"""Legacy Supabase client (deprecated).

This project has been migrated to local PostgreSQL. This module remains only so
any stale imports fail fast with a clear message, without requiring the
`supabase` package.
"""


class LazySupabase:
    def __getattr__(self, name: str):
        raise RuntimeError(
            "Supabase has been removed from this project. Use local PostgreSQL helpers instead."
        )

    def reset(self):
        return None


supabase = LazySupabase()
