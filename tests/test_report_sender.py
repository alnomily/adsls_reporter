import asyncio
import os
import tempfile
from datetime import timezone
import asyncio
import os
from datetime import timezone

import pytest

from bot.report_sender import send_images, collect_saved_user_reports


class DummyBot:
    def __init__(self):
        self.sent = []

    async def send_photo(self, chat_id, photo, caption=None):
        # Simulate sending by recording
        # FSInputFile has attribute file_path in aiogram; we accept either
        fp = getattr(photo, 'file_path', None) or getattr(photo, 'file_path', None)
        if fp and os.path.exists(fp):
            size = os.path.getsize(fp)
            if size == 0:
                from aiogram.exceptions import TelegramBadRequest
                raise TelegramBadRequest('Bad Request: file must be non-empty')
        self.sent.append((chat_id, fp, caption))


class DummyUserManager:
    def __init__(self, data_map):
        # data_map: user_id -> latest_account_data
        self.data_map = data_map

    async def get_latest_account_data(self, user_id):
        return self.data_map.get(user_id)


@pytest.mark.asyncio
async def test_send_images_skips_empty_files_and_reports(tmp_path):
    bot = DummyBot()
    tz = timezone.utc

    # Create a non-empty image and an empty image
    non_empty = tmp_path / "img1.png"
    empty = tmp_path / "img2.png"
    non_empty.write_bytes(b"\x89PNG\r\n\x1a\ndata")
    empty.write_bytes(b"")

    images = [str(non_empty), str(empty)]
    user_reports = [("u1", {}), ("u2", {})]

    result = await send_images(bot, "12345", images, user_reports, tz)

    assert result["sent"] == 1
    assert result["skipped"] >= 1


@pytest.mark.asyncio
async def test_collect_saved_user_reports_filters_none():
    users = [
        {"id": "u1", "username": "a"},
        {"id": "u2", "username": "b"},
    ]
    manager = DummyUserManager({"u1": {"balance": 10}, "u2": None})

    sem = asyncio.Semaphore(2)
    reports = await collect_saved_user_reports(users, sem, manager)

    assert isinstance(reports, list)
    assert len(reports) == 1
    assert reports[0][0] == "a"
