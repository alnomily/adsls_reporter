import logging
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor

from aiogram import Bot, Dispatcher

from config import BOT_TOKEN

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("YemenNetBot")

# Bot and dispatcher singletons used across handler modules
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Executor and concurrency limits
_MAX_WORKERS = min(32, (os.cpu_count() or 1) * 5)
EXEC = ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix="yemen_scraper")
SCRAPE_SEMAPHORE = asyncio.Semaphore(8)   # limit external scraping concurrency

def shutdown_executor(wait: bool = True) -> None:
    try:
        EXEC.shutdown(wait=wait)
    except Exception:
        pass
