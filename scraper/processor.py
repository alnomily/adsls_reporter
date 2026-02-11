import os
import time
import threading
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .session import get_session
from .utils import (
    extract_form_inputs,
    find_username_password_fields,
    download_captcha_to_temp,
    extract_account_data,
    add_log,
)
from .repository import fetch_active_users, fetch_user_by_username, save_account_data_rpc, insert_log
from .predict_image_api import PredictImageAPI

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("yemen_scraper.processor")

# Config defaults (can be tuned via env in future)
REQUEST_DELAY = 1.0
CAPTCHA_TIMEOUT = 25
MAX_ATTEMPTS = 3
THREADS = max(2, min(64, (os.cpu_count() or 4) * 2))
HTTP_POOL_SIZE = 20
SESSION_TTL_SECONDS = int(os.getenv("SESSION_TTL_SECONDS", "86400")) 

# Predictor globals
_global_predictor = None
_predictor_init_lock = threading.Lock()
_predict_lock = threading.Lock()

_user_sessions: Dict[str, requests.Session] = {}
_user_session_last_used: Dict[str, float] = {}
_user_sessions_lock = threading.RLock()


def _create_user_session(pool_size: int = HTTP_POOL_SIZE, retries: int = 2, backoff: float = 0.5) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; YemenNetScraper/1.0)"})
    session.trust_env = False
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _get_user_session(username: str) -> requests.Session:
    with _user_sessions_lock:
        now = time.time()
        _cleanup_user_sessions(now)
        session = _user_sessions.get(username)
        # logger.info("Session for user %s: %s", username, "exists" if session else "not found")
        # logger.info("Current active sessions: %s", list(_user_sessions.keys()))
        if session is None:
            session = _create_user_session()
            _user_sessions[username] = session
            # logger.info("Created new session for user %s", username)
        _user_session_last_used[username] = now
        return session


def _cleanup_user_sessions(now: Optional[float] = None) -> None:
    if SESSION_TTL_SECONDS <= 0:
        return
    with _user_sessions_lock:
        ts = now if now is not None else time.time()
        # logger.info("Cleaning up user sessions at %s", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)))
        expired = [
            username
            for username, last_used in _user_session_last_used.items()
            if (ts - last_used) > SESSION_TTL_SECONDS
        ]
        # logger.info("Found %d expired sessions: %s", len(expired), expired)
        for username in expired:
            session = _user_sessions.pop(username, None)
            _user_session_last_used.pop(username, None)
            if session is not None:
                try:
                    session.close()
                    # logger.info("Closed expired session for user %s", username)
                except Exception:
                    logger.debug("Failed to close expired session for %s", username, exc_info=True)


def _try_account_from_session(session: requests.Session) -> Optional[Dict[str, Any]]:
    try:
        r = session.get("https://adsl.yemen.net.ye/ar/login.aspx", timeout=CAPTCHA_TIMEOUT)
        r.raise_for_status()
        acc = extract_account_data(r.text)
        # logger.info("Account data from session: %s", acc)
        return acc if acc else None
    except Exception:
        return None


def get_predictor(model_path: str) -> PredictImageAPI:
    global _global_predictor
    if _global_predictor is None:
        with _predictor_init_lock:
            if _global_predictor is None:
                logger.info("Loading OCR model...")
                _global_predictor = PredictImageAPI(model_path)
                try:
                    if hasattr(_global_predictor, "warmup"):
                        _global_predictor.warmup()
                except Exception:
                    logger.debug("Predictor warmup failed", exc_info=True)
    return _global_predictor


def process_user(user_data: Dict[str, Any], model_path: str) -> bool:
    user_id = user_data["id"]
    username = user_data["username"]
    password = user_data["password"]

    session = _get_user_session(username)

    predictor = get_predictor(model_path)

    backoff = REQUEST_DELAY
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            acc = _try_account_from_session(session)
            if acc:
                if save_account_data_rpc(user_id, acc):
                    logger.info("Successfully fetched account data for user %s on attempt %s without captcha", username, attempt)
                    insert_log(user_id, "success")
                    add_log(f"[OK] {username}")
                    logger.info("[OK] %s", username)
                    time.sleep(REQUEST_DELAY)
                    return True

            r1 = session.get("https://adsl.yemen.net.ye/ar/login.aspx", timeout=CAPTCHA_TIMEOUT)
            r1.raise_for_status()

            soup1 = r1 and r1.text and __import__('bs4').BeautifulSoup(r1.text, 'html.parser')
            form1 = extract_form_inputs(soup1)

            ufield = next((n for n in form1 if "user" in n.lower()), None)
            pfield = next((n for n in form1 if "pass" in n.lower()), None)
            if not (ufield and pfield):
                ufield_h, pfield_h = find_username_password_fields(soup1)
                if ufield_h and pfield_h:
                    ufield, pfield = ufield_h, pfield_h

            if not ufield or not pfield:
                logger.error("Login fields not found for %s — page layout likely changed", username)
                add_log(f"[FAIL-LAYOUT] {username}")
                insert_log(user_id, "fail", "login_fields_not_found")
                return False

            form1[ufield] = username
            form1[pfield] = password

            post1 = session.post("https://adsl.yemen.net.ye/ar/login.aspx", data=form1, timeout=CAPTCHA_TIMEOUT)
            post1.raise_for_status()

            acc = extract_account_data(post1.text)
            if acc:
                if save_account_data_rpc(user_id, acc):
                    insert_log(user_id, "success")
                    add_log(f"[OK] {username}")
                    logger.info("Successfully fetched account data for user %s on attempt %s", username, attempt)
                    logger.info("[OK] %s", username)
                    time.sleep(REQUEST_DELAY)
                    return True

            soup2 = __import__('bs4').BeautifulSoup(post1.text, 'html.parser')
            cap_input = soup2.find("input", {"name": "ctl00$ContentPlaceHolder1$capres"})
            cap_img = soup2.find("img", {"id": "ContentPlaceHolder1_imgCaptcha"})
            if not (cap_input and cap_img and cap_img.get("src")):
                logger.debug("No captcha found on attempt %s for %s", attempt, username)
                time.sleep(backoff)
                backoff *= 1.5
                continue

            cap_path = download_captcha_to_temp(session, cap_img["src"] )
            try:
                with _predict_lock:
                    try:
                        captcha_value = predictor.predict_image(image_path=cap_path)
                    except Exception as e:
                        # Predictor failed (KeyError or decoding issues). Treat as empty
                        # so the outer retry/backoff logic handles it.
                        logger.warning("Predictor error for %s: %s", username, e, exc_info=True)
                        captcha_value = None
            finally:
                try:
                    os.remove(cap_path)
                except Exception:
                    pass

            if not captcha_value:
                insert_log(user_id, "fail", "empty captcha")
                logger.debug("Empty captcha result for %s", username)
                time.sleep(backoff)
                backoff *= 1.5
                continue

            form2 = extract_form_inputs(soup2)
            form2["ctl00$ContentPlaceHolder1$capres"] = captcha_value
            form2["ctl00$ContentPlaceHolder1$submitCaptch"] = "مواصلة"
            post2 = session.post("https://adsl.yemen.net.ye/ar/login.aspx", data=form2, timeout=CAPTCHA_TIMEOUT)
            post2.raise_for_status()

            acc = extract_account_data(post2.text)
            if acc:
                if save_account_data_rpc(user_id, acc):
                    insert_log(user_id, "success")
                    add_log(f"[OK] {username}")
                    logger.info("Successfully fetched account data for user %s on attempt %s", username, attempt)
                    logger.info("[OK] %s", username)
                    time.sleep(REQUEST_DELAY)
                    return True

            logger.debug("Account extraction failed for %s on attempt %s", username, attempt)
            time.sleep(backoff)
            backoff *= 1.5

        except Exception:
            logger.exception("Error processing user %s (attempt %s)", username, attempt)
            time.sleep(backoff)
            backoff *= 1.5

    insert_log(user_id, "fail", "max attempts reached")
    add_log(f"[FAIL] {username}")
    logger.info("[FAIL] %s", username)
    return False

def generate_username_candidates(adsl: str) -> list[str]:
    adsl = adsl.strip()
    base = adsl.lstrip("0")

    variants = set()

    variants.add(adsl)
    variants.add(base)

    if len(base) > 1:
        variants.add(base[1:])
        variants.add(base[2:])

    for p in ["1", "01"]:
        variants.add(p + base)
        variants.add(p + adsl)

    return [v for v in variants if v.isdigit() and 6 <= len(v) <= 9]

    # adsl = adsl.strip()

    # variants = set()

    # # original
    # variants.add(adsl)

    # # remove leading zeros
    # variants.add(adsl.lstrip("0"))

    # # add prefixes
    # if not adsl.startswith("1"):
    #     variants.add("1" + adsl.lstrip("0"))
    # if not adsl.startswith("01"):
    #     variants.add("01" + adsl.lstrip("0"))

    # # remove first digit (common in YemenNet)
    # if len(adsl) > 1:
    #     variants.add(adsl[1:])

    # # cleanup
    # logger.debug("Generated %d username candidates for ADSL %s", len(variants), adsl)
    # logger.debug("Candidates: %s", variants)
    # return [v for v in variants if v.isdigit() and 6 <= len(v) <= 8]

def try_login_once(
    username: str,
    password: str,
    predictor: PredictImageAPI,
) -> Optional[Dict[str, Any]]:
    session = get_session(pool_size=HTTP_POOL_SIZE)

    try:
        r1 = session.get("https://adsl.yemen.net.ye/ar/login.aspx", timeout=CAPTCHA_TIMEOUT)
        r1.raise_for_status()

        soup1 = __import__('bs4').BeautifulSoup(r1.text, 'html.parser')
        form1 = extract_form_inputs(soup1)

        ufield = next((n for n in form1 if "user" in n.lower()), None)
        pfield = next((n for n in form1 if "pass" in n.lower()), None)

        if not ufield or not pfield:
            return None

        form1[ufield] = username
        form1[pfield] = password

        post1 = session.post(
            "https://adsl.yemen.net.ye/ar/login.aspx",
            data=form1,
            timeout=CAPTCHA_TIMEOUT,
        )
        post1.raise_for_status()

        soup2 = __import__('bs4').BeautifulSoup(post1.text, 'html.parser')
        cap_img = soup2.find("img", {"id": "ContentPlaceHolder1_imgCaptcha"})
        cap_input = soup2.find("input", {"name": "ctl00$ContentPlaceHolder1$capres"})

        if not (cap_img and cap_input):
            return None

        cap_path = download_captcha_to_temp(session, cap_img["src"])
        try:
            captcha_value = predictor.predict_image(cap_path)
        finally:
            os.remove(cap_path)

        if not captcha_value:
            return None

        form2 = extract_form_inputs(soup2)
        form2["ctl00$ContentPlaceHolder1$capres"] = captcha_value
        form2["ctl00$ContentPlaceHolder1$submitCaptch"] = "مواصلة"

        post2 = session.post(
            "https://adsl.yemen.net.ye/ar/login.aspx",
            data=form2,
            timeout=CAPTCHA_TIMEOUT,
        )
        post2.raise_for_status()

        return extract_account_data(post2.text)

    except Exception:
        return None
    
def resolve_username_and_fetch_account(
    adsl_number: str,
    password: str,
    predictor: PredictImageAPI,
    max_workers: int = 4,
) -> Optional[Dict[str, Any]]:
    candidates = generate_username_candidates(adsl_number)
    logger.debug("Generated username candidates for %s: %s", adsl_number, candidates)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(try_login_once, u, password, predictor): u
            for u in candidates
        }

        for future in as_completed(future_map):
            username = future_map[future]
            logger.debug("Trying username candidate: %s", username)
            try:
                account_data = future.result()
                if account_data:
                    account_data["resolved_username"] = username
                    return account_data
            except Exception:
                continue

    return None

def process_single_adsl(
    adsl_number: str,
    password: str,
    predictor: PredictImageAPI,
) -> dict:
    """
    Returns:
    {
        success: bool,
        adsl: str,
        resolved_username: Optional[str],
        account_data: Optional[dict],
        error: Optional[str]
    }
    """

    account_data = resolve_username_and_fetch_account(
        adsl_number=adsl_number,
        password=password,
        predictor=predictor,
        max_workers=4,
    )
    logger.info("Processed ADSL %s, success=%s", adsl_number, account_data is not None)

    if not account_data:
        return {
            "success": False,
            "adsl": adsl_number,
            "resolved_username": None,
            "account_data": None,
            "error": "username_not_resolved"
        }

    return {
        "success": True,
        "adsl": adsl_number,
        "resolved_username": account_data["resolved_username"],
        "account_data": account_data,
        "error": None
    }

# def insert_user_account(
#     adsl: str,
#     resolved_username: str,
#     network_id: int,
#     password: str = "0000",
# ) -> Optional[int]:

#     insert_user_account

#     result = (
#         sb.table("users_accounts")
#         .insert({
#             "username": resolved_username,
#             "account_name": adsl,
#             "password": password,
#             "network_id": network_id,
#             "status": "pending"
#         })
#         .execute()
#     )

#     data = getattr(result, "data", None)
#     if data and len(data) > 0:
#         return data[0]["id"]

#     return None

def process_all_adsls(
    adsl_numbers: list[str],
    network_id: int,
    model_path: str,
    max_workers: int = 6,
) -> dict:
    """
    Returns summary:
    {
        success: int,
        failed: int,
        results: [...]
    }
    """
    from bot.utils_shared import sync_insert_user_account
    from bot.user_manager import UserManager

    def _make_adsl_key(adsl):
        if isinstance(adsl, (str, int)):
            return str(adsl)
        if isinstance(adsl, dict):
            return str(adsl.get("username") or adsl.get("adsl_number") or adsl)
        if isinstance(adsl, list):
            return "_".join(map(str, adsl))
        return str(adsl)
    predictor = get_predictor(model_path)
    results = []
    success = 0
    failed = 0
    password = "123456"
    successful_users_ids = []
    successful_adsl = []
    failed_adsl = []
    failure_reasons = {}

    # Check if users already exist
    existing_users = UserManager.users_exists(adsl_numbers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                process_single_adsl,
                adsl,
                password,
                predictor,
            ): adsl
            for adsl in adsl_numbers if adsl not in existing_users
        }

        for future in as_completed(future_map):
            result = future.result()
            adsl_key = _make_adsl_key(result["adsl"])
            results.append(result)
            logger.info("Processed ADSL %s: %s", adsl_key, "success" if result["success"] else "failed")
            logger.debug("Result details: %s", result)

            if not result["success"]:
                failed_adsl.append(adsl_key)
                failed += 1
                logger.info("ADSL %s failed: %s", adsl_key, result.get("error"))
                continue

            # 1️⃣ insert user
            user_id = sync_insert_user_account(
                result["resolved_username"],
                password,
                network_id,
                str(result["adsl"]),
            )

            # handle duplicate insertion response
            if isinstance(user_id, str) and user_id.lower() == "duplicate":
                failure_reasons[adsl_key] = "المستخدم موجود مسبقاً"
                failed_adsl.append(adsl_key)
                failed += 1
                logger.info("ADSL %s insertion skipped (duplicate username)", adsl_key)
                continue
            
            if not user_id:
                failure_reasons[adsl_key] = "فشل في إدخال المستخدم"
                failed_adsl.append(adsl_key)
                failed += 1
                logger.info("ADSL %s insertion failed", adsl_key)
                continue
            # 2️⃣ save account data
            if save_account_data_rpc(user_id, result["account_data"]):
                insert_log(user_id, "success")
                successful_users_ids.append(user_id)
                successful_adsl.append(adsl_key)
                success += 1
            else:
                insert_log(user_id, "fail", "rpc_failed")
                failed_adsl.append(adsl_key)
                failure_reasons[adsl_key] = "فشل في حفظ بيانات الحساب"
                failed += 1

    # Add existing users to failure reasons
    logger.info("Existing users detected: %s", existing_users)
    for adsl in existing_users:
        failure_reasons[adsl] = "المستخدم موجود مسبقاً"

    return {
        "success": ",".join(successful_users_ids),
        "success_adsl": ",".join(successful_adsl),
        "failed": failed,
        "failed_adsl": ",".join(failed_adsl),
        "failure_reasons": failure_reasons,
        "results": results
    }

def process_all_adsls_with_usernames(
    adsl_user_map: dict[str, str],
    network_id: int,
    model_path: str,
    max_workers: int = 6,
) -> dict:
    """
    Same as process_all_adsls but takes explicit ADSL->username map and does not
    generate username candidates.

    Returns summary:
    {
        success: str,          # comma-separated user_ids
        success_adsl: str,     # comma-separated adsl numbers
        failed: int,
        failed_adsl: str,      # comma-separated adsl numbers
        failure_reasons: dict, # adsl -> reason
        results: list          # per-adsl result objects
    }
    """
    from bot.utils_shared import sync_insert_user_account
    from bot.user_manager import UserManager

    def _make_adsl_key(adsl):
        if isinstance(adsl, (str, int)):
            return str(adsl)
        if isinstance(adsl, dict):
            return str(adsl.get("username") or adsl.get("adsl_number") or adsl)
        if isinstance(adsl, list):
            return "_".join(map(str, adsl))
        return str(adsl)

    predictor = get_predictor(model_path)
    results = []
    success = 0
    failed = 0
    password = "123456"
    successful_users_ids = []
    successful_adsl = []
    failed_adsl = []
    failure_reasons = {}

    adsl_numbers = list(adsl_user_map.keys())
    existing_users = UserManager.users_exists(adsl_numbers)

    def _process_known(adsl: str, username: str) -> dict:
        account_data = try_login_once(username, password, predictor)
        if not account_data:
            return {
                "success": False,
                "adsl": adsl,
                "resolved_username": username,
                "account_data": None,
                "error": "login_failed",
            }
        return {
            "success": True,
            "adsl": adsl,
            "resolved_username": username,
            "account_data": account_data,
            "error": None,
        }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_process_known, adsl, adsl_user_map[adsl]): adsl
            for adsl in adsl_numbers
            if adsl not in existing_users
        }

        for future in as_completed(future_map):
            result = future.result()
            adsl_key = _make_adsl_key(result["adsl"])
            results.append(result)
            logger.info("Processed ADSL %s: %s", adsl_key, "success" if result["success"] else "failed")
            logger.debug("Result details: %s", result)

            if not result["success"]:
                failed_adsl.append(adsl_key)
                failed += 1
                failure_reasons.setdefault(adsl_key, result.get("error") or "login_failed")
                continue

            user_id = sync_insert_user_account(
                result["resolved_username"],
                password,
                network_id,
                str(result["adsl"]),
            )

            if isinstance(user_id, str) and user_id.lower() == "duplicate":
                failure_reasons[adsl_key] = "المستخدم موجود مسبقاً"
                failed_adsl.append(adsl_key)
                failed += 1
                logger.info("ADSL %s insertion skipped (duplicate username)", adsl_key)
                continue

            if not user_id:
                failure_reasons[adsl_key] = "فشل في إدخال المستخدم"
                failed_adsl.append(adsl_key)
                failed += 1
                logger.info("ADSL %s insertion failed", adsl_key)
                continue

            if save_account_data_rpc(user_id, result["account_data"]):
                insert_log(user_id, "success")
                successful_users_ids.append(user_id)
                successful_adsl.append(adsl_key)
                success += 1
            else:
                insert_log(user_id, "fail", "rpc_failed")
                failed_adsl.append(adsl_key)
                failure_reasons[adsl_key] = "فشل في حفظ بيانات الحساب"
                failed += 1

    logger.info("Existing users detected: %s", existing_users)
    for adsl in existing_users:
        failure_reasons[adsl] = "المستخدم موجود مسبقاً"

    return {
        "success": ",".join(successful_users_ids),
        "success_adsl": ",".join(successful_adsl),
        "failed": failed,
        "failed_adsl": ",".join(failed_adsl),
        "failure_reasons": failure_reasons,
        "results": results,
    }

def process_adsl_range_to_accounts2(
    start_adsl: int,
    end_adsl: int,
    network_id: int,
    model_path: str,
    max_workers: int = 6,
    save_account_data: bool = False,
) -> dict:
    """
    Process ADSL numbers in a numeric range and insert valid accounts into users_accounts2.

    Returns summary:
    {
        success: str,          # comma-separated user_ids
        success_adsl: str,     # comma-separated adsl numbers
        failed: int,
        failed_adsl: str,      # comma-separated adsl numbers
        failure_reasons: dict, # adsl -> reason
        results: list          # per-adsl result objects
    }
    """
    from bot.utils_shared import sync_insert_user_account2, sync_users_exists_accounts2

    if end_adsl < start_adsl:
        start_adsl, end_adsl = end_adsl, start_adsl

    predictor = get_predictor(model_path)
    results = []
    success = 0
    failed = 0
    password = "123456"
    successful_users_ids = []
    successful_adsl = []
    failed_adsl = []
    failure_reasons = {}

    adsl_numbers = [str(n) for n in range(start_adsl, end_adsl + 1)]
    try:
        existing_resp = sync_users_exists_accounts2(adsl_numbers)
        existing_data = getattr(existing_resp, "data", None) or []
        if isinstance(existing_data, list):
            existing_users = [item.get("adsl_number") for item in existing_data if isinstance(item, dict)]
        elif isinstance(existing_data, str):
            existing_users = [existing_data]
        elif isinstance(existing_data, dict):
            existing_users = [existing_data.get("adsl_number")]
        else:
            existing_users = []
    except Exception as exc:
        logger.exception("Range processing aborted before start: %s", exc)
        failure_reasons["__error__"] = "users_accounts2 table missing or unavailable"
        return {
            "success": "",
            "success_adsl": "",
            "failed": 0,
            "failed_adsl": "",
            "failure_reasons": failure_reasons,
            "results": results,
        }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                process_single_adsl,
                adsl,
                password,
                predictor,
            ): adsl
            for adsl in adsl_numbers if adsl not in existing_users
        }

        for future in as_completed(future_map):
            result = future.result()
            adsl_key = str(result["adsl"])
            results.append(result)
            logger.info("Processed ADSL %s: %s", adsl_key, "success" if result["success"] else "failed")
            logger.debug("Result details: %s", result)

            if not result["success"]:
                failed_adsl.append(adsl_key)
                failed += 1
                failure_reasons.setdefault(adsl_key, result.get("error") or "login_failed")
                continue

            account_data = result.get("account_data") or {}
            plan_value = (account_data.get("plan") or "").strip()
            if "فيبـر نت" not in plan_value:
                failure_reasons[adsl_key] = "plan_not_allowed"
                failed_adsl.append(adsl_key)
                failed += 1
                continue

            user_id = sync_insert_user_account2(
                result["resolved_username"],
                password,
                network_id,
                str(result["adsl"]),
                account_data,
            )

            if isinstance(user_id, str) and user_id.lower() == "duplicate":
                failure_reasons[adsl_key] = "المستخدم موجود مسبقاً"
                failed_adsl.append(adsl_key)
                failed += 1
                logger.info("ADSL %s insertion skipped (duplicate username)", adsl_key)
                continue

            if not user_id:
                failure_reasons[adsl_key] = "فشل في إدخال المستخدم"
                failed_adsl.append(adsl_key)
                failed += 1
                logger.info("ADSL %s insertion failed", adsl_key)
                continue

            if save_account_data and save_account_data_rpc(user_id, result["account_data"]):
                insert_log(user_id, "success")
            elif save_account_data:
                insert_log(user_id, "fail", "rpc_failed")

            successful_users_ids.append(user_id)
            successful_adsl.append(adsl_key)
            success += 1

    logger.info("Existing users detected (users_accounts2): %s", existing_users)
    for adsl in existing_users:
        failure_reasons[adsl] = "المستخدم موجود مسبقاً"

    return {
        "success": ",".join(successful_users_ids),
        "success_adsl": ",".join(successful_adsl),
        "failed": failed,
        "failed_adsl": ",".join(failed_adsl),
        "failure_reasons": failure_reasons,
        "results": results,
    }

def start_process_adsl_range_to_accounts2_background(
    start_adsl: int,
    end_adsl: int,
    network_id: int,
    model_path: str,
    max_workers: int = 6,
    save_account_data: bool = False,
) -> threading.Thread:
    """Run range processing in a background thread and return the thread handle."""
    thread = threading.Thread(
        target=process_adsl_range_to_accounts2,
        kwargs={
            "start_adsl": start_adsl,
            "end_adsl": end_adsl,
            "network_id": network_id,
            "model_path": model_path,
            "max_workers": max_workers,
            "save_account_data": save_account_data,
        },
        daemon=True,
    )
    thread.start()
    logger.info(
        "Started background range processing for %s-%s (users_accounts2)",
        start_adsl,
        end_adsl,
    )
    return thread

def fetch_users(model_path: str, threads: int = None) -> Dict[str, bool]:
    users = fetch_active_users()
    if not users:
        logger.info("No users to process")
        return {}

    max_workers = threads or THREADS
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(process_user, user, model_path): user["username"] for user in users}
        for fut in as_completed(futures):
            uname = futures[fut]
            try:
                results[uname] = fut.result()
            except Exception:
                logger.exception("Worker failed for %s", uname)
                results[uname] = False
    return results


def fetch_single_user(username: str, model_path: str, is_admin: bool = False) -> Dict[str, bool]:
    try:
        user = fetch_user_by_username(username, is_admin=is_admin)
        if not user:
            add_log(f"[NO USER] {username}")
            logger.info("User not found: %s", username)
            return {username: False}
        ok = process_user(user, model_path)
        return {username: ok}
    except Exception:
        logger.exception("Failed to fetch/process single user %s", username)
        return {username: False}
