import os
import re
import asyncio
import logging
from datetime import datetime
from typing import List, Tuple, Dict, Any

from aiogram import types
import io
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError

from bot.table_report import TableReportGenerator
from bot.user_manager import UserManager
from bot.utils_shared import get_token_by_network_id
from bot.selected_network_manager import selected_network_manager,SelectedNetwork
from bot.chat_user_manager import ChatUser

import config
import shutil
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, retry_if_exception
from uuid import uuid4
from config import ADMIN_ID, ADMIN_IDS

logger = logging.getLogger("YemenNetBot.report_sender")


async def collect_saved_user_reports(users: List[Dict[str, Any]], sem_users: asyncio.Semaphore, user_manager, order_by: str = "usage") -> List[Tuple[str, Dict[str, Any]]]:
    """Read latest saved data for provided users concurrently and return list of (username, merged_data).

    order_by options:
    - usage (default): highest usage first
    - remaining_days: lowest remaining days first
    - balance: highest balance first (uses balance_value or balance)
    - adsl_number: ascending by normalized ADSL/username
    - adsl_order_index: ascending by per-ADSL order index
    """

    if order_by not in ("adsl_number", "usage", "remaining_days", "balance", "adsl_order_index"):
        logger.warning("Invalid order_by %s; defaulting to 'usage'", order_by)
        order_by = "usage"
    def _normalize_adsl(val: Any) -> str:
        s = str(val) if val is not None else ""
        s = s.strip()
        if not s:
            return s
        # If it's purely digits, left-pad to 8 to keep a consistent display
        if s.isdigit():
            try:
                n = int(s)
                return f"{n:08d}"
            except Exception:
                return s
        return s
    async def _get(u: Dict[str, Any]):
        async with sem_users:
            # Always prefer the ADSL number from the users table for identity
            uname = _normalize_adsl(u.get("adsl_number") or u.get("username"))
            try:
                latest = await user_manager.get_latest_account_data(u["id"])
                # always return a merged dict so the report includes every user
                if latest:
                    # Merge with precedence for identity fields from users table to avoid accidental overrides
                    merged = {**latest}
                    merged.update({
                        'id': u.get('id'),
                        'username': u.get('username'),
                        'adsl_number': _normalize_adsl(u.get('adsl_number') or u.get('username')),
                        'status': u.get('status', merged.get('status')),
                    })
                    return merged.get('adsl_number') or merged.get('username') or uname, merged
                else:
                    logger.debug("No saved latest for user %s (id=%s), inserting placeholder", uname, u.get('id'))
                    # create a minimal placeholder record so generator can render an empty row
                    placeholder = {
                        'id': u.get('id'),
                        'username': u.get('username'),
                        'adsl_number': _normalize_adsl(u.get('adsl_number') or u.get('username')),
                        'plan': '-',
                        'subscription_date': '-',
                        'expiry_date': '-',
                        'status': u.get('status', '-'),
                        'yesterday_balance': '-',
                        'today_balance': '-',
                        'usage': '-',
                        'remaining_days': '-',
                        'balance_value': '-',
                        'consumption_value': '-',
                        'notes': 'لا توجد بيانات'  # Arabic: no data
                    }
                    return uname, {**u, **placeholder}
            except Exception as e:
                logger.warning("Failed to read saved data for %s: %s", uname, e)
            # on error return a safe placeholder so report keeps user present
            placeholder = {
                'id': u.get('id'),
                'username': u.get('username'),
                'adsl_number': _normalize_adsl(u.get('adsl_number') or u.get('username')),
                'plan': '-',
                'subscription_date': '-',
                'expiry_date': '-',
                'status': u.get('status', '-'),
                'yesterday_balance': '-',
                'today_balance': '-',
                'usage': '-',
                'remaining_days': '-',
                'balance_value': '-',
                'consumption_value': '-',
                'notes': 'خطأ في قراءة البيانات'  # Arabic: error reading data
            }
            return uname, {**u, **placeholder}
    tasks = [asyncio.create_task(_get(u)) for u in users]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    collected = [(uname, data) for uname, data in results if data]

    def _number(val: Any, default: float) -> float:
        if val is None:
            return default
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            match = re.search(r"-?\d+(?:\.\d+)?", val.replace(",", ""))
            if match:
                try:
                    return float(match.group(0))
                except Exception:
                    pass
        return default

    def _adsl_sort_key(item: Tuple[str, Dict[str, Any]]):
        uname, data = item
        adsl = data.get("adsl_number") or uname or ""
        if isinstance(adsl, str) and adsl.strip().isdigit():
            try:
                return int(adsl.strip())
            except Exception:
                return adsl
        return adsl

    if order_by == "usage":
        collected = sorted(collected, key=lambda item: _number(item[1].get("usage"), float("-inf")), reverse=True)
    elif order_by == "remaining_days":
        collected = sorted(collected, key=lambda item: _number(item[1].get("remaining_days"), float("inf")))
    elif order_by == "balance":
        collected = sorted(collected, key=lambda item: _number(item[1].get("balance_value", item[1].get("balance")), float("-inf")), reverse=True)
    elif order_by == "adsl_order_index":
        collected = sorted(collected, key=lambda item: _number(item[1].get("order_index"), float("inf")))
    else:  # adsl_number
        collected = sorted(collected, key=_adsl_sort_key)

    # diagnostic logging to help investigate inconsistent counts between runs
    try:
        all_ids = [u.get('id') for u in users]
        all_usernames = [u.get('username') for u in users]
        reported_ids = [data.get('id') for _, data in collected if data.get('id')]
        reported_usernames = [uname for uname, _ in collected]
        missing_by_id = [uid for uid in all_ids if uid not in reported_ids]
        placeholder_users = [uname for uname, data in collected if str(data.get('notes','')).strip()]
        if missing_by_id:
            logger.warning("Some users were dropped from collected reports (by id): %s", missing_by_id)
        # list which users had placeholder notes
        if placeholder_users:
            logger.debug("Users present (may include placeholders): %s", reported_usernames)
    except Exception:
        pass

    return collected


def generate_images(user_reports: List[Tuple[str, Dict[str, Any]]], network :SelectedNetwork, chat_user:ChatUser, report_date: str = "") -> Tuple[List[str], str]:
    """Blocking function: generate table report images for user_reports using TableReportGenerator.

    Use a unique per-invocation base filename (timestamp + uuid) so concurrent calls do not produce
    colliding file paths. The TableReportGenerator will append page numbers to this base path.
    """
    gen = TableReportGenerator()
    # create a unique per-invocation directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = uuid4().hex
    out_dir = os.path.join("reports", f"financial_report_{timestamp}_{unique_id}")
    os.makedirs(out_dir, exist_ok=True)
    base_save = os.path.join(out_dir, "financial_report.jpg")
    images = gen.generate_financial_table_report(user_reports,network, chat_user, save_path=base_save, report_date=report_date)
    return images, out_dir


async def send_images(bot_instance,network:SelectedNetwork, telegram_id: str, images: List[str], user_reports: List[Tuple[str, Dict[str, Any]]], tz, cleanup_dir: str = None,isDailyReport: bool = True,sendToAdmin: bool = True, scheduled_time: Tuple[int, int, int] | None = None, report_date: str = "") -> Dict[str, Any]:
    """Send images sequentially to the chat associated with the network. Returns summary dict."""
    sent = 0
    skipped = 0
    chat_not_found = False
    
    for page, img in enumerate(images, 1):
        try:
            if not img or not os.path.exists(img):
                logger.warning("Image file missing for user %s network %s page %d: %r", network.user_name, network.network_name, page, img)
                skipped += 1
                continue

            try:
                size = os.path.getsize(img)
            except Exception:
                size = 0

            if size == 0:
                logger.warning("Image file empty for user %s network %s page %d: %r", network.user_name, network.network_name, page, img)
                skipped += 1
                try:
                    os.remove(img)
                except Exception:
                    pass
                continue

            # prepare chat_id
            try:
                chat_id_to_use = int(telegram_id)
            except Exception:
                chat_id_to_use = telegram_id

            # Read file contents into memory to avoid aiofiles path-open race during aiohttp streaming
            try:
                with open(img, "rb") as fh:
                    data = fh.read()
            except FileNotFoundError:
                logger.warning("File disappeared before send for user %s network %s page %d: %s", network.user_name, network.network_name, page, img)
                skipped += 1
                continue
            except Exception as e:
                logger.exception("Failed to read image file %s before sending: %s", img, e)
                skipped += 1
                continue

            if not data:
                logger.warning("Image file empty in-memory for user %s network %s page %d: %s", network.user_name, network.network_name, page, img)
                skipped += 1
                try:
                    os.remove(img)
                except Exception:
                    pass
                continue

            def _make_file_obj():
                return types.BufferedInputFile(data, filename=os.path.basename(img))

            file_obj = _make_file_obj()

            # send with retry for transient errors
            _reported_orphan_tokens = globals().setdefault("_reported_orphan_tokens", set())
            token_key = str(telegram_id)

            async def _send_to_partners(file_obj, page: int, imagesLength: int):
                # Do not send to partners during daily reports per requirement
                if isDailyReport or report_date:
                    return
                network_partners = await UserManager.get_network_partners(network.network_id, with_owner=True)
                partners = [p for p in network_partners if p.get("is_partner_active") and p.get("telegram_id") and p.get("receive_partnered_report") and p.get("telegram_id") != telegram_id]
                for p in partners:
                    partner_token = p.get("telegram_id")
                    chat_network_id = p.get("id")
                    # Immediate (non-daily) sends have no partner schedule gating
                    logger.info("Sending report to partner %s for user %s network %s page %d", partner_token, network.user_name, network.network_name, page)

                    @retry(
                        reraise=True,
                        stop=stop_after_attempt(3),
                        wait=wait_exponential(multiplier=1.0, min=2, max=15),
                        retry=retry_if_exception_type((TelegramNetworkError, asyncio.TimeoutError)),
                    )
                    async def _send_partner_with_retry():
                        header = (
                            "تقرير من السجل\n"
                            if report_date
                            else f"📊 {'تقرير يومي' if isDailyReport else 'تقرير فوري'} من شريك\n"
                        )
                        time_line = (
                            f"🕒 تاريخ التقرير: {report_date}\n"
                            if report_date
                            else f"🕒 وقت التقرير: {datetime.now(tz).strftime('%Y-%m-%d %H:%M')}\n"
                        )
                        image_line = (
                            f"– الصورة {page}/{imagesLength}\n" if imagesLength > 1 else ""
                        )
                        await bot_instance.send_photo(
                            chat_id=int(partner_token),
                            photo=_make_file_obj(),
                            caption=(
                                f"{header}"
                                f"🛜 الشبكة: {network.network_name}\n"
                                f"{image_line}"
                                f"👥 عدد الخطوط: {len(user_reports)}\n"
                                f"{time_line}"
                                "〰️\n"
                            ),
                            request_timeout=120
                        )

                    try:
                        await _send_partner_with_retry()
                    except TelegramBadRequest as tbe:
                        msg = str(tbe).lower()
                        if 'chat not found' in msg:
                            logger.warning("Partner chat not found (%s) for user %s network %s; deactivating partner record id=%s", partner_token, network.user_name, network.network_name, chat_network_id)
                            try:
                                await UserManager.deactivate_network_partner(chat_network_id)
                            except Exception:
                                logger.exception("Failed to deactivate partner record id=%s for network %s", chat_network_id, network.network_name)
                        else:
                            logger.exception("TelegramBadRequest sending to partner %s for user %s network %s page %d: %s", partner_token, network.user_name, network.network_name, page, tbe)
                    except Exception as e:
                        logger.exception("Failed to send report to partner %s for user %s network %s page %d: %s", partner_token, network.user_name, network.network_name, page, e)
                    await asyncio.sleep(0.2)

            @retry(reraise=True, stop=stop_after_attempt(5),
                   wait=wait_exponential(multiplier=1.0, min=2, max=20),
                   retry=retry_if_exception(lambda exc: not isinstance(exc, TelegramBadRequest)))
            async def _send_with_retry():
                imagesLength = len(images)
                try:
                    header = (
                        "تقرير من السجل\n"
                        if report_date
                        else f"📊 {'تقرير يومي' if isDailyReport else 'تقرير فوري'}\n"
                    )
                    time_line = (
                        f"🕒 تاريخ التقرير: {report_date}\n"
                        if report_date
                        else f"🕒 وقت التقرير: {datetime.now(tz).strftime('%Y-%m-%d %H:%M')}\n"
                    )
                    image_line = (
                        f"– الصورة {page}/{imagesLength}\n" if imagesLength > 1 else ""
                    )
                    await bot_instance.send_photo(
                        chat_id=chat_id_to_use,
                        photo=file_obj,
                        caption=(
                            f"{header}"
                            f"🛜 الشبكة: {network.network_name}\n"
                            f"{image_line}"
                            f"👥 عدد الخطوط: {len(user_reports)}\n"
                            f"{time_line}"
                            "〰️\n"
                        ),
                        request_timeout=120
                    )
                    # Send to partners only for non-daily reports
                    if not isDailyReport:
                        await _send_to_partners(file_obj, page, imagesLength)

                    # if sendToAdmin:
                    #     admin_id = SECONDARY_ADMIN
                    #     await bot_instance.send_photo(
                    #         chat_id=admin_id,
                    #         photo=file_obj,
                    #         caption=(f"📊 {'تقرير يومي' if isDailyReport else 'تقرير فوري'} تم إرساله إلى الشبكة {network.network_name}\n"
                    #                 f"{'الشركاء الذين تم الإرسال إليهم:' + ', '.join(tokens_sent) if tokens_sent else ''}\n"
                    #                 f"👤 المستخدم: {network.user_name}\n"
                    #                 f"💬 معرف المستخدم: {telegram_id}\n"
                    #                 f"{(f'– الصورة {page}/{imagesLength}\n') if imagesLength > 1 else ''}"
                    #                 f"👥 عدد الخطوط: {len(user_reports)}\n"
                    #                 f"🕒 وقت التقرير: {datetime.now(tz).strftime('%Y-%m-%d %H:%M')}\n"
                    #                 f"〰️\n"
                    #         )
                    #     )
                except TelegramBadRequest as tb_inner:
                    msg = str(tb_inner).lower()
                    if 'file must be non-empty' in msg:
                        # permanent failure for this image
                        logger.error("Telegram rejected empty file for user %s network %s page %d: %s", network.user_name, network.network_name, page, tb_inner)
                        raise
                    if 'chat not found' in msg:
                        # notify admin once per token across the process lifetime
                        if token_key not in _reported_orphan_tokens:
                            admin_targets = ADMIN_IDS or ([ADMIN_ID] if ADMIN_ID else [])
                            kb = types.InlineKeyboardMarkup(inline_keyboard=[
                                [types.InlineKeyboardButton(
                                    text="🚫 تعطيل الإرسال لهذا المستخدم",
                                    callback_data=f"admin:network:deactivate:{network.network_id}"
                                )]
                            ])
                            for admin_id in admin_targets:
                                try:
                                    await bot_instance.send_message(
                                        admin_id,
                                        (
                                            "⚠️ تعذّر الإرسال: لا يمكن الوصول إلى محادثة هذا المستخدم.\n"
                                            f"👤 المستخدم: {network.user_name}\n"
                                            f"🛜 الشبكة: {network.network_name}\n"
                                            "الإجراء المقترح: تعطيل الإرسال لهذا المستخدم لمنع المحاولات المستقبلية."
                                        ),
                                        reply_markup=kb
                                    )
                                    _reported_orphan_tokens.add(token_key)
                                except Exception:
                                    logger.exception("Failed to notify admin %s about chat not found for user %s network %s", admin_id, network.user_name, network.network_name)
                        # re-raise to stop retrying (decorator excludes TelegramBadRequest)
                        raise
                    # re-raise to allow retry for other transient errors
                    raise

            try:
                await _send_with_retry()
                sent += 1
                logger.info("Sent page %d/%d to user %s network %s", page, len(images), network.user_name, network.network_name)
            except TelegramBadRequest as tb_final:
                msg = str(tb_final).lower()
                if 'chat not found' in msg:
                    chat_not_found = True
                    # If owner's chat is invalid, only attempt partner sends for non-daily reports
                    if not isDailyReport:
                        try:
                            imagesLength = len(images)
                            await _send_to_partners(file_obj, page, imagesLength)
                        except Exception:
                            logger.exception("Failed to send to partners after owner chat-not-found for user %s network %s page %d", network.user_name, network.network_name, page)
                    # Continue to next image rather than breaking entire loop
                    continue
                else:
                    logger.exception("TelegramBadRequest sending page %d to user %s network %s after retries: %s", page, network.user_name, network.network_name, tb_final)
                    skipped += 1
                    # don't attempt per-file deletions here; cleanup is done atomically after
                    pass
            except Exception as final_exc:
                logger.exception("Failed to send page %d to user %s network %s after retries: %s", page, network.user_name, network.network_name, final_exc)
                skipped += 1
                # keep files for debugging; atomic cleanup happens below
                pass
        except Exception:
            logger.exception("Failed to send daily report page %d to user %s network %s", page, network.user_name, network.network_name)
        # finished sending this page; continue to next

    # small delay between pages to help Telegram
    await asyncio.sleep(0.7)

    # Atomic cleanup: remove the entire invocation directory if provided
    if cleanup_dir:
        try:
            # remove directory and all its contents
            shutil.rmtree(cleanup_dir)
            logger.debug("Cleaned up report directory: %s", cleanup_dir)
        except Exception:
            logger.exception("Failed to cleanup report directory: %s", cleanup_dir)

    return {"sent": sent, "skipped": skipped, "chat_not_found": chat_not_found}
