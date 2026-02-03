import os
import tempfile
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

logger = logging.getLogger("yemen_scraper.utils")

BASE_URL = "https://adsl.yemen.net.ye/ar/"


def absolute(url: str) -> str:
    if url.startswith("http"):
        return url
    return BASE_URL + url.lstrip("/")


def extract_form_inputs(soup: BeautifulSoup) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for inp in soup.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        data[name] = inp.get("value") or ""
    for sel in soup.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opt = sel.find("option", selected=True) or sel.find("option")
        data[name] = opt.get("value") if opt else ""
    return data


def find_username_password_fields(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    inputs = list(soup.find_all("input"))
    uname = None
    pword = None
    for inp in inputs:
        name = (inp.get("name") or "").strip()
        itype = (inp.get("type") or "").lower()
        id_ = (inp.get("id") or "").strip()
        placeholder = (inp.get("placeholder") or "").strip()

        key = " ".join([name.lower(), id_.lower(), placeholder.lower(), itype])
        if any(k in key for k in ("user", "username", "msisdn", "phone", "login")) and itype in ("", "text", "tel"):
            if not uname and name:
                uname = name
        if "pass" in key or itype == "password":
            if not pword and name:
                pword = name

    if not uname:
        for inp in inputs:
            if (inp.get("type") or "").lower() in ("", "text", "tel"):
                if inp.get("name"):
                    uname = inp.get("name")
                    break
    if not pword:
        for inp in inputs:
            if (inp.get("type") or "").lower() == "password":
                if inp.get("name"):
                    pword = inp.get("name")
                    break
    return uname, pword


def download_captcha_to_temp(session, src: str, timeout: int = 25) -> str:
    url = absolute(src)
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    tf = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
    try:
        tf.write(resp.content)
        return tf.name
    finally:
        tf.close()

def extract_labwelcome_name(html: str) -> str:
    """Extracts the username from the labWelcome span, without the 'مرحباً:' prefix."""
    soup = BeautifulSoup(html, "html.parser")
    lab = soup.find(id="labWelcome")
    if not lab:
        return ""
    text = lab.get_text(strip=True)
    # Remove 'مرحباً:' (with or without spaces)
    return text.replace("مرحباً:", "").strip()

def extract_account_data(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"cellpadding": "6"})
    if not table:
        return {}
    result = {}
    result["account_name"] = extract_labwelcome_name(html)
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) != 2:
            continue
        key = tds[0].get_text(strip=True)
        val = tds[1].get_text(strip=True)
        if "تاريخ الاشتراك" in key:
            result["subscription_date"] = val
        elif "نوع الاشتراك" in key:
            result["plan"] = val
        elif "حالة الاشتراك" in key:
            result["status"] = val
        elif "الرصيد المتاح" in key:
            result["available_balance"] = val
        elif "تاريخ انتهاء" in key:
            result["expiry_date"] = val
    return result


def add_log(message: str, tag: str = None, path: str = None) -> None:
    log_path = path or os.path.join(os.path.dirname(__file__), "filelog.txt")
    try:
        try:
            tz = ZoneInfo("Asia/Aden")
        except Exception:
            tz = timezone.utc
        dt = datetime.now(tz)
        ts = dt.strftime("%Y/%m/%d %H:%M:%S.") + f"{dt.microsecond//1000:03d}"
        entry = f"{ts} - {tag} - {message}\n" if tag else f"{ts} - {message}\n"
        with open(log_path, "a", encoding="utf-8") as lf:
            lf.write(entry)
    except Exception:
        logger.debug("Failed to write file log", exc_info=True)
