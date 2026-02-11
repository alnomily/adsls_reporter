"""Compatibility facade exposing the original API expected by the bot.

This module wraps the new processor/repository modules and keeps backward
compatibility: fetch_users(), fetch_single_user(username), save_account_data(...)
"""
import os
import logging
from typing import Dict, Any

from .processor import (
    fetch_users as _fetch_users,
    fetch_single_user as _fetch_single_user,
    process_all_adsls as _process_all_adsls,
    process_all_adsls_with_usernames as _process_all_adsls_with_usernames,
    process_adsl_range_to_accounts2 as _process_adsl_range_to_accounts2,
    start_process_adsl_range_to_accounts2_background as _start_process_adsl_range_to_accounts2_background,
)
from .repository import save_account_data_rpc

logger = logging.getLogger("yemen_scraper.runner")

# Model path default (same as original layout)
OCR_MODEL_PATH = os.path.join(os.path.dirname(__file__), "ocr_crnn_model.keras")


def fetch_users() -> Dict[str, bool]:
    return _fetch_users(model_path=OCR_MODEL_PATH)


def fetch_single_user(username: str, is_admin: bool = False) -> Dict[str, bool]:
    return _fetch_single_user(username=username, is_admin=is_admin, model_path=OCR_MODEL_PATH)


def save_account_data(user_id: int, account_data: Dict[str, Any]) -> bool:
    return save_account_data_rpc(user_id, account_data)

def process_all_adsls(adsl_numbers: list[str],
    network_id: int,
    max_workers: int = 6,
) -> dict:
    return _process_all_adsls(
        adsl_numbers=adsl_numbers,
        network_id=network_id,
        model_path=OCR_MODEL_PATH,
        max_workers=max_workers,
    )

def process_all_adsls_with_usernames(adsl_user_map: dict[str, str],
    network_id: int,
    max_workers: int = 6,
) -> dict:
    return _process_all_adsls_with_usernames(
        adsl_user_map=adsl_user_map,
        network_id=network_id,
        model_path=OCR_MODEL_PATH,
        max_workers=max_workers,
    )

def process_adsl_range_to_accounts2(
    start_adsl: int,
    end_adsl: int,
    network_id: int,
    max_workers: int = 6,
    save_account_data: bool = False,
) -> dict:
    return _process_adsl_range_to_accounts2(
        start_adsl=start_adsl,
        end_adsl=end_adsl,
        network_id=network_id,
        model_path=OCR_MODEL_PATH,
        max_workers=max_workers,
        save_account_data=save_account_data,
    )

def start_process_adsl_range_to_accounts2_background(
    start_adsl: int,
    end_adsl: int,
    network_id: int,
    max_workers: int = 6,
    save_account_data: bool = False,
):
    return _start_process_adsl_range_to_accounts2_background(
        start_adsl=start_adsl,
        end_adsl=end_adsl,
        network_id=network_id,
        model_path=OCR_MODEL_PATH,
        max_workers=max_workers,
        save_account_data=save_account_data,
    )