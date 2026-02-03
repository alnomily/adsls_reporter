"""Backward-compatible facade for the refactored scraper package.

This file keeps the original module path but delegates to the new
`runner` module which exposes the same simple functions expected by other
parts of the project (`fetch_users`, `fetch_single_user`, `save_account_data`).
"""
from .runner import fetch_users, fetch_single_user, save_account_data

__all__ = ["fetch_users", "fetch_single_user", "save_account_data"]
