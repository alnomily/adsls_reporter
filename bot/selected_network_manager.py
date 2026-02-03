import asyncio
from typing import Optional, Dict, Tuple
from bot.user_manager import UserManager
import logging

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("YemenNetBot.selected_network_manager")

class SelectedNetwork:
    def __init__(self,id: int, network_id: int, network_name: str, user_name: str, times_to_send_reports: int,warning_count_remaining_days: int,danger_count_remaining_days: int,warning_percentage_remaining_balance: int,danger_percentage_remaining_balance: int,is_active: bool,expiration_date: str, telegram_id: str = "", chat_user_id: int = 0, network_type: str = "", permissions: str = ""):
        self.id = id
        self.network_id = network_id
        self.network_name = network_name
        self.user_name = user_name
        self.times_to_send_reports = times_to_send_reports
        self.warning_count_remaining_days = warning_count_remaining_days
        self.danger_count_remaining_days = danger_count_remaining_days
        self.warning_percentage_remaining_balance = warning_percentage_remaining_balance
        self.danger_percentage_remaining_balance = danger_percentage_remaining_balance
        self.is_active = is_active
        self.expiration_date = expiration_date
        self.telegram_id = telegram_id
        self.chat_user_id = chat_user_id
        self.network_type = network_type
        self.permissions = permissions

    @staticmethod
    def from_bitmask_to_times_list(bitmask: int) -> list[str]:
        # Bitwise mapping: 1=06:00:00, 2=12:00:00, 4=18:00:00, 8=23:50:00
        mapping = [
            (1, "06:00:00"),
            (2, "12:00:00"),
            (4, "18:00:00"),
            (8, "23:50:00"),
        ]
        times = []
        for bit, time_str in mapping:
            if bitmask & bit:
                times.append(time_str)
        return times
    @staticmethod
    def from_long_times_to_short_times_list(bitmask: int) -> list[str]:
        long_times_list = SelectedNetwork.from_bitmask_to_times_list(bitmask)
        mapping = {
            "06:00:00": "6 صباحًا",
            "12:00:00": "12 ظهرًا",
            "18:00:00": "6 مساءً",
            "23:50:00": "11:50 مساءً",
        }
        short_times = [mapping.get(time_str, time_str) for time_str in long_times_list]
        return short_times
    @staticmethod
    def from_times_list_to_bitmask(times_list: list[str]) -> int:
        mapping = {
            "06:00:00": 1,
            "12:00:00": 2,
            "18:00:00": 4,
            "23:50:00": 8,
        }
        bitmask = 0
        for time_str in times_list:
            bitmask |= mapping.get(time_str, 0)
        logger.info(f"Converted times list {times_list} to bitmask {bitmask}")
        return bitmask

    def __repr__(self):
        return f"<id: {self.id} network_id: {self.network_id} network_name: {self.network_name} user: {self.user_name} times_to_send_reports: {self.times_to_send_reports} telegram_id: {self.telegram_id} chat_user_id: {self.chat_user_id} network_type: {self.network_type} permissions: {self.permissions}>"        
    
class SelectedNetworkManager:
    _instance = None
    _lock = asyncio.Lock()
    _selected_networks: Dict[str, SelectedNetwork] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SelectedNetworkManager, cls).__new__(cls)
        return cls._instance

    async def get(self, telegram_id: str) -> Optional[SelectedNetwork]:
        # Check cache first
        if telegram_id in self._selected_networks:
            return self._selected_networks[telegram_id]
        # Otherwise, fetch from DB
        async with self._lock:
            result = await UserManager.get_selected_network(telegram_id)
            logger.info(f"Fetched selected network for {telegram_id}: {result}")
            if result:
                selected_network = SelectedNetwork(*result)
                self._selected_networks[telegram_id] = selected_network
                return selected_network
            return None

    async def set(self,chat_network_id: int, chat_user_id: int,telegram_id: str) -> bool:
        async with self._lock:
            isSet = await UserManager.set_selected_network(chat_network_id, chat_user_id)
            logger.info(f"Set selected network {chat_network_id} for chat_user {chat_user_id}")
            # Refresh cache
            result = await UserManager.get_selected_network(telegram_id)
            if result:
                self._selected_networks[telegram_id] = SelectedNetwork(*result)
                logger.info(f"Updated cache for chat_user {chat_user_id} with network {chat_network_id}")
            return isSet
    
    async def update(self, telegram_id: str, network_name: str, user_name: str) -> Optional[SelectedNetwork]:
        async with self._lock:
            selected_network = self._selected_networks.get(telegram_id)
            if not selected_network:
                logger.warning(f"No selected network in cache for {telegram_id} to update")
                return None
            selected_network.network_name = network_name
            selected_network.user_name = user_name

            self._selected_networks[telegram_id] = selected_network
            logger.info(f"Updated selected network in cache for {telegram_id}: {selected_network}")
            return selected_network
        
    async def change_times_to_send_report(self, network: SelectedNetwork, times_to_send_reports: list[str]) -> bool:
        async with self._lock:
            if not network:
                logger.warning(f"No selected network in cache for {telegram_id} to update times_to_send_reports")
                return False
            times_to_send_report_bitmask = SelectedNetwork.from_times_list_to_bitmask(times_to_send_reports)
            logger.info(f"Changing times_to_send_reports for {network.telegram_id} to {times_to_send_reports} (bitmask {times_to_send_report_bitmask})")
            result = await UserManager.update_user_networks_times_to_send_reports(network.id, times_to_send_report_bitmask)
            logger.info(f"Updated network in DB for {network.telegram_id}: {result}")
            if result:
                network.times_to_send_reports = times_to_send_report_bitmask
                logger.info(f"Updated times_to_send_reports for {network.telegram_id} to {times_to_send_reports}")
                self._selected_networks[network.telegram_id] = network
                return True
            return False
    async def change_warning_and_danger_settings(self, network: SelectedNetwork,warning_count_remaining_days: int,danger_count_remaining_days: int,warning_percentage_remaining_balance: int,danger_percentage_remaining_balance: int) -> bool:
        async with self._lock:
            if not network:
                logger.warning(f"No selected network in cache for {telegram_id} to update warning and danger settings")
                return False
            logger.info(f"Changing warning and danger settings for {network.telegram_id} to {warning_count_remaining_days}, {danger_count_remaining_days}, {warning_percentage_remaining_balance}, {danger_percentage_remaining_balance}")
            result = await UserManager.change_warning_and_danger_settings(network.id,warning_count_remaining_days,danger_count_remaining_days,warning_percentage_remaining_balance,danger_percentage_remaining_balance)
            logger.info(f"Updated network in DB for {network.telegram_id}: {result}")
            if result:
                network.warning_count_remaining_days = warning_count_remaining_days
                network.danger_count_remaining_days = danger_count_remaining_days
                network.warning_percentage_remaining_balance = warning_percentage_remaining_balance
                network.danger_percentage_remaining_balance = danger_percentage_remaining_balance
                logger.info(f"Updated warning and danger settings for {network.telegram_id} to {warning_count_remaining_days}, {danger_count_remaining_days}, {warning_percentage_remaining_balance}, {danger_percentage_remaining_balance}")
                self._selected_networks[network.telegram_id] = network
                return True
            return False

    def clear(self, telegram_id: str):
        self._selected_networks.pop(telegram_id, None)

# Create a global instance
selected_network_manager = SelectedNetworkManager()