import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")

def _parse_admin_ids(raw: str | None) -> list[int]:
	if not raw:
		return []
	parts = raw.replace(";", ",").split(",")
	ids: list[int] = []
	for part in parts:
		p = part.strip()
		if not p:
			continue
		try:
			ids.append(int(p))
		except Exception:
			continue
	return ids

ADMIN_IDS = _parse_admin_ids(os.getenv("ADMIN_ID"))
ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else 0
