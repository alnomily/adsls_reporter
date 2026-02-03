"""Shared mutable state used by handlers to avoid circular imports."""
from typing import Dict, Any

# Interactive add-users flow state: chat_id -> state dict
PENDING_ADD_USERS: Dict[int, Dict[str, Any]] = {}
