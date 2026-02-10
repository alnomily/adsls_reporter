from typing import Any, Dict, List, Optional
import logging
from bot.utils_shared import (
    activate_chat_user,
    activate_network,
    approve_registration,
    change_chat_networks_times_to_send_reports,
    change_order_by,
    change_receive_partnered_reports,
    change_users_network,
    change_warning_and_danger_settings,
    deactivate_chat_user,
    deactivate_network,
    delete_users_by_ids,
    get_all_users_for_admin,
    get_chat_users_tokens,
    get_chats_users,
    get_network_by_id,
    get_network_by_network_id,
    get_selected_network,
    get_user_data_db,
    insert_user_account,
    get_users_by_network_db,
    get_latest_account_data_db,
    get_user_logs_db,
    get_all_users_by_network_id,
    remove_network,
    set_selected_network,
    get_all_tokens,
    get_token_by_network_id,
    sync_users_exists,
    update_chat_user,
    get_chat_user,
    update_network,
    get_networks_for_user,
    activate_users,
    add_network_partner,
    get_all_partnered_networks, 
    activate_partnered_networks,
    deactivate_partnered_networks,
    delete_partnered_networks,
    change_partner_permissions,
    get_daily_reports_for_users,
    get_available_report_dates,
    get_adsls_order_indexed,
    get_adsl_order_index,
    update_adsl_order_index,


)
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("YemenNetBot.user_manager")


def _extract_success_message(result: Any) -> tuple[Optional[bool], Optional[str]]:
    """Normalize success/message extraction from varied response shapes.
    Handles dicts, objects with attributes, lists, and nested 'data'."""
    success: Optional[bool] = None
    message: Optional[str] = None

    def consider(obj: Any) -> None:
        nonlocal success, message
        if isinstance(obj, dict):
            if success is None and "success" in obj:
                success = obj.get("success")
            if not message and obj.get("message"):
                message = obj.get("message")

    try:
        consider(result)
    except Exception:
        pass

    # Attributes on object-like results
    try:
        if success is None and hasattr(result, "success"):
            success = getattr(result, "success", None)
        if not message and hasattr(result, "message"):
            message = getattr(result, "message", None)
    except Exception:
        pass

    # Inspect nested data
    data = None
    try:
        data = result.get("data") if isinstance(result, dict) else getattr(result, "data", None)
    except Exception:
        data = None

    if data is not None:
        if isinstance(data, dict):
            consider(data)
        elif isinstance(data, list):
            for item in data:
                consider(item)
                if success:
                    break

    # If the result itself is a list
    if success is None and isinstance(result, list):
        for item in result:
            consider(item)
            if success:
                break

    # Normalize common success representations
    if isinstance(success, (int, float)):
        success = bool(int(success))
    elif isinstance(success, str):
        success = success.strip().lower() in {"1", "true", "yes", "ok", "success"}

    return success, message


class UserManager:
    @staticmethod
    async def get_user_data(username: str, network_id: str, is_admin: bool = False) -> Optional[Dict[str, Any]]:
        try:
            resp = await get_user_data_db(username, network_id, is_admin)
            data = getattr(resp, "data", None) or []
            return data[0] if data else None
        except Exception as e:
            logger.error(f"get_user_data error: {e}")
            return None

    @staticmethod
    async def insert_user(username: str, password: str, network_id: str, adsl: Optional[str] = None):
        return await insert_user_account(username, password, network_id, adsl)
    

    @staticmethod
    async def get_users_by_network(network_id: str):
        try:
            resp = await get_users_by_network_db(network_id)
            return getattr(resp, "data", None) or []
        except Exception as e:
            logger.error(f"get_users_by_network error for network {network_id}: {e}")
            return []
        
    @staticmethod
    async def get_all_users_for_admin():
        try:
            resp = await get_all_users_for_admin()
            logger.info("get_all_users_for_admin fetched %d users", len(getattr(resp, "data", None) or []))
            return getattr(resp, "data", None) or []
        except Exception as e:
            logger.error(f"get_all_users_for_admin error: {e}")
            return []

    @staticmethod
    async def get_latest_account_data(user_id: str, is_admin: bool = False) -> Optional[Dict[str, Any]]:
        try:
            # Call the utils_shared helper which already performs retries/backoff
            resp = await get_latest_account_data_db(user_id, is_admin = is_admin)
            data = getattr(resp, "data", None) or []
            return data[0] if data else None
        except Exception as e:
            # Convert persistent failures into None so callers insert placeholders
            logger.debug(f"get_latest_account_data persistent error for user_id {user_id}: {e}")
            return None

    @staticmethod
    async def get_available_report_dates(user_ids: List[str], limit: int = 120) -> List[str]:
        try:
            resp = await get_available_report_dates(user_ids, limit)
            data = getattr(resp, "data", None) or []
            logger.info("get_available_report_dates fetched %d records for user_ids %s", len(data), user_ids)
            logger.info("get_available_report_dates raw data: %s", data)
            dates = [row.get("report_date") for row in data if row.get("report_date")]
            logger.info("get_available_report_dates extracted dates: %s", dates)
            return sorted(set(dates), reverse=True)
        except Exception as e:
            logger.error(f"get_available_report_dates error for user_ids {user_ids}: {e}")
            return []
        
    @staticmethod
    async def activate_users(users_ids: list):
        try:
            resp = await activate_users(users_ids)
            return resp
        except Exception as e:
            logger.error(f"activate_users error: {e}")
            return None

    @staticmethod
    async def get_user_logs(user_id: str, limit: int = 5):
        try:
            resp = await get_user_logs_db(user_id, limit)
            return getattr(resp, "data", None) or []
        except Exception as e:
            logger.error(f"get_user_logs error for user {user_id}: {e}")
            return []

    @staticmethod
    async def get_all_users_data_by_network_id(network_id: str):
        try:
            resp = await get_all_users_by_network_id(network_id)
            return getattr(resp, "data", None) or []
        except Exception as e:
            logger.error(f"get_all_users_data_by_network_id error for network {network_id}: {e}")
            return []
    
    @staticmethod
    async def set_selected_network(chat_network_id: int, chat_user_id: int):
        return await set_selected_network(chat_network_id, chat_user_id)

    @staticmethod
    async def get_selected_network(telegram_id: str) -> Optional[tuple[int,int, str, str, int, str, int, int, int, int, bool, str, str, int, str, str]]:
        try:
            resp = await get_selected_network(telegram_id)
            logger.info(f"get_selected_network response for telegram_id {telegram_id}: {resp}")
            data = getattr(resp, "data", None) or []
            if isinstance(data, dict) and data:
                return (
                    int(data.get("id")),
                    int(data.get("network_id")),
                    str(data.get("network_name")),
                    str(data.get("user_name")),
                    int(data.get("times_to_send_reports") or 15),
                    int(data.get("warning_count_remaining_days") or 7),
                    int(data.get("danger_count_remaining_days") or 3),
                    int(data.get("warning_percentage_remaining_balance") or 30),
                    int(data.get("danger_percentage_remaining_balance") or 10),
                    bool(data.get("is_active") or False),
                    str(data.get("expiration_date") or ""),
                    str(data.get("telegram_id")),
                    int(data.get("chat_user_id")),
                    str(data.get("network_type")),
                    str(data.get("permissions")),

                )
            elif isinstance(data, list) and data:
                d = data[0]
                return (
                    int(d.get("id")),
                    int(d.get("network_id")),
                    str(d.get("network_name")),
                    str(d.get("user_name")),
                    int(d.get("times_to_send_reports") or 15),
                    int(d.get("warning_count_remaining_days") or 7),
                    int(d.get("danger_count_remaining_days") or 3),
                    int(d.get("warning_percentage_remaining_balance") or 30),
                    int(d.get("danger_percentage_remaining_balance") or 10),
                    bool(d.get("is_active") or False),
                    str(d.get("expiration_date") or ""),
                    str(d.get("telegram_id")),
                    int(d.get("chat_user_id")),
                    str(d.get("network_type")),
                    str(d.get("permissions")),
                )
            return None
        except Exception as e:
            logger.error(f"get_selected_network_id error for telegram_id {telegram_id}: {e}")
            return None

    @staticmethod
    async def get_token_by_network_id(network_id: str) -> Optional[str]:
        try:
            resp = await get_token_by_network_id(network_id)
            data = getattr(resp, "data", None) or []
            if data and len(data) > 0:
                return data["chats_users"]["telegram_id"]
            return None
        except Exception as e:
            logger.error(f"get_token_by_network_id error for network_id {network_id}: {e}")
            return None
        
    @staticmethod
    async def get_all_tokens() -> List[str]:
        try:
            resp = await get_all_tokens()
            logger.info("get_all_tokens response: %s", resp)
            data = getattr(resp, "data", None) or []
            logger.info("get_all_tokens fetched %d tokens", len(data))
            tokens = [item["telegram_id"] for item in data if "telegram_id" in item]
            return tokens
        except Exception as e:
            logger.error(f"get_all_tokens error: {e}")
            return []
        
    @staticmethod
    async def get_chat_user(telegram_id: str) -> Optional[tuple[int, str, bool,str]]:
        try:
            resp = await get_chat_user(telegram_id)

            data = getattr(resp, "data", None) or []
            if isinstance(data, dict) and data:
                return (
                    int(data.get("chat_user_id") or data.get("id")),
                    str(data.get("user_name")),
                    bool(data.get("receive_partnered_report") or False),
                    bool(data.get("is_active") or False),
                    str(data.get("order_by") or ""),
                )
            elif isinstance(data, list) and data:
                d = data[0]
                return (
                    int(d.get("chat_user_id") or d.get("id")),
                    str(d.get("user_name")),
                    bool(d.get("receive_partnered_report") or False),
                    bool(d.get("is_active") or False),
                    str(d.get("order_by") or ""),
                )
            return None
        except Exception as e:
            logger.error(f"get_chat_user error for telegram_id {telegram_id}: {e}")
            return None
        
    @staticmethod
    async def get_chats_users() -> List[Dict[str, Any]]:
        try:
            resp = await get_chats_users()
            logger.info("get_chats_users response: %s", resp)
            data = getattr(resp, "data", None) or []
            logger.info("get_chats_users fetched %d users", len(data))
            return data
        except Exception as e:
            logger.error(f"get_chats_users error: {e}")
            return []
        
    @staticmethod
    async def get_chats_users_tokens(chats_users_ids: list) -> List[str]:
        try:
            resp = await get_chat_users_tokens(chats_users_ids)
            logger.info("get_chat_users_tokens response: %s", resp)
            data = getattr(resp, "data", None) or []
            logger.info("get_chat_users_tokens fetched %d tokens", len(data))
            tokens = [item["telegram_id"] for item in data if "telegram_id" in item]
            return tokens
        except Exception as e:
            logger.error(f"get_chat_users_tokens error for chats_users_ids {chats_users_ids}: {e}")
            return []
        
    @staticmethod
    async def update_chat_user(telegram_id: str, user_name: str) -> Optional[bool]:
        try:
            await update_chat_user(telegram_id, user_name)
            return True
        except Exception as e:
            logger.error(f"update_chat_user error for telegram_id {telegram_id}: {e}")
            return False
    
    @staticmethod
    async def activate_chat_user(telegram_id: str) -> Optional[bool]:
        try:
            await activate_chat_user(telegram_id)
            return True
        except Exception as e:
            logger.error(f"activate_chat_user error for telegram_id {telegram_id}: {e}")
            return False
    
    @staticmethod
    async def deactivate_chat_user(telegram_id: str) -> Optional[bool]:
        try:
            await deactivate_chat_user(telegram_id)
            return True
        except Exception as e:
            logger.error(f"deactivate_chat_user error for telegram_id {telegram_id}: {e}")
            return False
        
    @staticmethod
    async def update_network(network_id: int, network_name: str, times_to_send_reports: Optional[int] = None) -> Optional[bool]:
        try:
            # call the shared helper (imported as update_network)
            result = await update_network(network_id, network_name, times_to_send_reports)
            logger.info("update_network result raw for network_id %s: %s", network_id, result)
            success, message = _extract_success_message(result)

            if bool(success):
                logger.info("update_network succeeded for network_id %s", network_id)
                return True

            if message:
                logger.error("update_network failed for network_id %s: %s", network_id, message)
            else:
                logger.error("update_network did not indicate success for network_id %s", network_id)
            return False
        except Exception as e:
            logger.error(f"update_network error for network_id {network_id}: {e}")
            return False
    
    @staticmethod
    async def update_user_networks_times_to_send_reports(chat_network_id: int, times_to_send_reports: int) -> bool:
        try:
            result = await change_chat_networks_times_to_send_reports(chat_network_id, times_to_send_reports)
            success, message = _extract_success_message(result)
            if bool(success):
                return True
            if message:
                logger.error(
                    f"update_user_networks_times_to_send_reports failed for chat_network_id {chat_network_id}: {message}"
                )
            return False
        except Exception as e:
            logger.error(f"update_user_networks_times_to_send_reports error for chat_network_id {chat_network_id}: {e}")
            return False
        
    @staticmethod
    async def change_warning_and_danger_settings(chat_network_id: int, warning_count_remaining_days: int, danger_count_remaining_days: int, warning_percentage_remaining_balance: int, danger_percentage_remaining_balance: int) -> bool:
        try:
            result = await change_warning_and_danger_settings(chat_network_id, warning_count_remaining_days, danger_count_remaining_days, warning_percentage_remaining_balance, danger_percentage_remaining_balance)
            success, message = _extract_success_message(result)
            if bool(success):
                return True
            if message:
                logger.error(
                    f"change_warning_and_danger_settings failed for chat_network_id {chat_network_id}: {message}"
                )
            return False
        except Exception as e:
            logger.error(f"change_warning_and_danger_settings error for chat_network_id {chat_network_id}: {e}")
            return False
        
    @staticmethod
    async def update_chat_and_network(telegram_id: str, user_name: str, network_name: str) -> Optional[bool]:
        try:
            chat_user = await UserManager.update_chat_user(telegram_id, user_name)
            if not chat_user:
                return False
            chat_user_id = chat_user.get("id")
            if not chat_user_id:
                return False
            network = await UserManager.update_network(chat_user_id, network_name)
            return network
        except Exception as e:
            logger.error(f"update_chat_and_network error for telegram_id {telegram_id}: {e}")
            return False
    @staticmethod
    async def get_networks_for_user(chat_user_id: str) -> List[Dict[str, Any]]:
        try:
            resp = await get_networks_for_user(chat_user_id)
            return getattr(resp, "data", None) or []
        except Exception as e:
            logger.error(f"get_networks_for_user error for chat_user_id {chat_user_id}: {e}")
            return []
        
    @staticmethod
    async def change_users_network(users_ids: list, old_network_id: int, new_network_id: int) -> bool:
        try:
            await change_users_network(users_ids, old_network_id, new_network_id)
            return True
        except Exception as e:
            logger.error(f"change_users_network error for users_ids {users_ids}: {e}")
            return False
        
    @staticmethod
    async def add_network_partner(network_id: int, chat_user_id: int, permissions: int = 1) -> bool:
        try:
            resp = await add_network_partner(network_id, chat_user_id, permissions)
            return resp
        except Exception as e:
            logger.error(f"add_network_partner error for network_id {network_id}, chat_user_id {chat_user_id}: {e}")
            return False
    
    @staticmethod
    async def get_network_partners(network_id: int,with_owner: bool=False) -> List[Dict[str, Any]]:
        try:
            resp = await get_all_partnered_networks(network_id,with_owner)
            data = getattr(resp, "data", None) or []
            if isinstance(data, dict):
                return data
            elif isinstance(data, list):
                return data
        except Exception as e:
            logger.error(f"get_network_parteners error for network_id {network_id}: {e}")
            return []
        
    @staticmethod
    async def remove_network_partner(chat_network_id: int) -> bool:
        try:
            resp = await delete_partnered_networks(chat_network_id)
            return resp
        except Exception as e:
            logger.error(f"remove_network_partner  error for chat_network_id {chat_network_id}: {e}")
            return False
        
    @staticmethod
    async def update_network_partner_permissions(chat_network_id: int, permissions: int):
        try:
            resp = await change_partner_permissions(chat_network_id, permissions)
            return resp
        except Exception as e:
            logger.error(f"update_network_partner_permissions error for chat_network_id {chat_network_id}: {e}")
            return False
        
    @staticmethod
    async def activate_network_partner(chat_network_id: int) -> bool:
        try:
            resp = await activate_partnered_networks(chat_network_id)
            return resp
        except Exception as e:
            logger.error(f"activate_network_partner error for chat_network_id {chat_network_id}: {e}")
            return False
        
    @staticmethod
    async def change_receive_partnered_reports(chat_user_id: int, receive_partnered_report: bool) -> bool:
        try:
            resp = await change_receive_partnered_reports(chat_user_id, receive_partnered_report)
            return resp
        except Exception as e:
            logger.error(f"change_receive_partnered_reports error for chat_user_id {chat_user_id}: {e}")
            return False

    @staticmethod
    async def get_daily_reports_for_users(user_ids: list, report_date: str):
        try:
            resp = await get_daily_reports_for_users(user_ids, report_date)
            return getattr(resp, "data", None) or []
        except Exception as e:
            logger.error(f"get_daily_reports_for_users error for {user_ids} at {report_date}: {e}")
            return []
    
    @staticmethod
    async def deactivate_network_partner(chat_network_id: int) -> bool:
        try:
            resp = await deactivate_partnered_networks(chat_network_id)
            return resp
        except Exception as e:
            logger.error(f"deactivate_network_partner error for chat_network_id {chat_network_id}: {e}")
            return False
        
    @staticmethod
    async def delete_users_by_ids(users_ids: list):
        try:
            resp = await delete_users_by_ids(users_ids)
            return resp
        except Exception as e:
            logger.error(f"delete_users_by_ids error for users_ids {users_ids}: {e}")
            return None
    
    @staticmethod
    async def remove_network(network_id: int):
        try:
            resp = await remove_network(network_id)
            logger.info(f"remove_network response for network_id {network_id}: {resp}")
            return resp
        except Exception as e:
            logger.error(f"remove_network error for network_id {network_id}: {e}")
            return None

    @staticmethod
    async def get_network_by_id(network_id: int) -> Optional[Dict[str, Any]]:
        try:
            resp = await get_network_by_id(network_id)
            data = getattr(resp, "data", None) or []
            logger.info(f"get_network_by_id response for network_id {network_id}: {data}")
            if isinstance(data, dict) and data:
                logger.info(f"get_network_by_id found network for network_id {network_id}: {data}")
                return data
            if isinstance(data, list) and data:
                logger.info(f"get_network_by_id found network for network_id {network_id}: {data[0]}")
                return data[0]
            return None
        except Exception as e:
            logger.error(f"get_network_by_id error for network_id {network_id}: {e}")
            return None
        
    @staticmethod
    async def get_network_by_network_id(network_id: int) -> Optional[Dict[str, Any]]:
        try:
            resp = await get_network_by_network_id(network_id)
            data = getattr(resp, "data", None) or []
            logger.info(f"get_network_by_network_id response for network_id {network_id}: {data}")
            if isinstance(data, dict) and data:
                logger.info(f"get_network_by_network_id found network for network_id {network_id}: {data}")
                return data
            if isinstance(data, list) and data:
                logger.info(f"get_network_by_network_id found network for network_id {network_id}: {data[0]}")
                return data[0]
            return None
        except Exception as e:
            logger.error(f"get_network_by_network_id error for network_id {network_id}: {e}")
            return None
        
    @staticmethod
    def users_exists(adsls: list) -> List[str]:
        try:
            resp = sync_users_exists(adsls)
            logger.info("users_exists response: %s", resp)
            data = getattr(resp, "data", None) or []
            logger.info("users_exists raw data: %s", data)
            if isinstance(data, list):
                return [item.get("adsl_number") for item in data]
            if isinstance(data, str):
                return [data]
            if isinstance(data, dict):
                return [data.get("adsl_number")]
            return []
        except Exception as e:
            logger.error(f"users_exists error for adsls {adsls}: {e}")
            return []
    
    @staticmethod
    async def activate_network(network_id: int) -> bool:
        try:
            result = await activate_network(network_id)
            success, message = _extract_success_message(result)

            if bool(success):
                return True
            
            if message:
                logger.error(
                    f"network activate failed for network_id network_id {network_id}: {message}"
                )
            else:
                logger.error(
                    f"network activate failed for network_id {network_id}: unknown error response {result}"
                )
            return False
        except Exception as e:
            logger.error(f"activate_network error for network_id {network_id}: {e}")
            return False
        
    @staticmethod
    async def deactivate_network(network_id: int) -> bool:
        try:
            result = await deactivate_network(network_id)
            success, message = _extract_success_message(result)

            if bool(success):
                return True
            if message:
                logger.error(f"deactivate_network failed for network_id {network_id}: {message}")
            else:
                logger.error(f"deactivate_network failed for network_id {network_id}: unknown error response {result}")
            return False
        except Exception as e:
            logger.error(f"deactivate_network error for network_id {network_id}: {e}")
            return False

    @staticmethod
    async def approve_registration(
        users_ids: list,
        telegram_id: str,
        payer_chat_user_id: int,
        network_id: int,
        expiration_date: str,
        amount: int,
        payment_method: str,
    ) -> bool:
        try:
            result = await approve_registration(
                users_ids,
                telegram_id,
                payer_chat_user_id,
                network_id,
                expiration_date,
                amount,
                payment_method,
            )
            success, message = _extract_success_message(result)

            if bool(success):
                return True

            if message:
                logger.error(
                    f"approve_registration failed for users_ids {users_ids}, telegram_id {telegram_id}, network_id {network_id}: {message}"
                )
            else:
                logger.error(
                    f"approve_registration failed for users_ids {users_ids}, telegram_id {telegram_id}, network_id {network_id}: unknown error response {result}"
                )
            return False
        except Exception as e:
            logger.error(f"approve_registration error for users_ids {users_ids}: {e}")
            return False
        
    @staticmethod
    async def change_order_by(telegram_id: str, order_by: str) -> Optional[bool]:
        try:
            logger.info(f"Changing order_by for telegram_id {telegram_id} to {order_by}")
            result = await change_order_by(telegram_id, order_by)
            success, message = _extract_success_message(result)
            if bool(success):
                logger.info(f"change_order_by succeeded for telegram_id {telegram_id}")
                return True
            if message:
                logger.error(f"change_order_by failed for telegram_id {telegram_id}: {message}")
            else:
                logger.error(f"change_order_by did not indicate success for telegram_id {telegram_id}")
            return False
        except Exception as e:
            logger.error(f"change_order_by error for telegram_id {telegram_id}: {e}")
            return False
        
    @staticmethod
    async def get_adsls_order_indexed(network_id: int)-> Optional[list[tuple[str, str,int]]]:
        try:
            resp = await get_adsls_order_indexed(network_id)
            data = getattr(resp, "data", None) or []
            logger.info(f"get_adsls_order_indexed response for network_id {network_id}: {data}")
            if isinstance(data, list) and data:
                result = []
                for d in data:
                    result.append((
                        str(d.get("id")),
                        str(d.get("adsl_number")),
                        int(d.get("order_index") or -1),
                    ))
                return result
            return None
        except Exception as e:
            logger.error(f"get_adsls_order_indexed error for network_id {network_id}: {e}")
            return None
    
    @staticmethod
    async def get_adsl_order_index(id: str)-> Optional[int]:
        try:
            resp = await get_adsl_order_index(id)
            data = getattr(resp, "data", None) or []
            logger.info(f"get_adsl_order_index response for id {id}: {data}")
            if isinstance(data, dict) and data:
                return int(data.get("order_index") or 0)
            elif isinstance(data, list) and data:
                d = data[0]
                return int(d.get("order_index") or 0)
            return None
        except Exception as e:
            logger.error(f"get_adsl_order_index error for id {id}: {e}")
            return None
        
    @staticmethod
    async def update_adsl_order_index(id: str, order_index: int) -> Optional[bool]:
        try:
            resp = await update_adsl_order_index(id, order_index)
            logger.info(f"update_adsl_order_index response for id {id}: {resp}")
            success, message = _extract_success_message(resp)
            if bool(success):
                logger.info(f"update_adsl_order_index succeeded for id {id}")
                return True
            if message:
                logger.error(f"update_adsl_order_index failed for id {id}: {message}")
            else:
                logger.error(f"update_adsl_order_index did not indicate success for id {id}")
            return False
        except Exception as e:
            logger.error(f"update_adsl_order_index error for id {id}: {e}")
            return False
