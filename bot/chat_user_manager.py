import asyncio
from typing import Optional, Dict
from bot.selected_network_manager import selected_network_manager
from bot.user_manager import UserManager
import logging


LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("YemenNetBot.chat_user_manager")

class ChatUser:
    def __init__(self, chat_user_id: int, telegram_id: str,user_name: str, receive_partnered_reports: bool,is_active: bool,order_by:str):
        self.chat_user_id = chat_user_id
        self.telegram_id = telegram_id
        self.user_name = user_name
        self.receive_partnered_reports = receive_partnered_reports
        self.is_active = is_active
        # Preserve legacy field name from DB but expose a consistent attribute
        self.order_by = order_by or "usage"
    def __repr__(self):
        return f"<chat_user: {self.user_name} telegram_id: {self.telegram_id} user_name: {self.user_name}>"
    
class ChatUserManager:
    _instance = None
    _lock = asyncio.Lock()
    _chat_users: Dict[str, ChatUser] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ChatUserManager, cls).__new__(cls)
        return cls._instance

    async def get(self, telegram_id: str) -> Optional[ChatUser]:
        # Check cache first
        if telegram_id in self._chat_users:
            return self._chat_users[telegram_id]
        # Otherwise, fetch from DB
        async with self._lock:
            result = await UserManager.get_chat_user(telegram_id)
            logger.info(f"Fetched chat user for {telegram_id}: {result}")
            if result:
                chat_user = ChatUser(result[0], telegram_id, result[1], result[2], result[3], result[4])
                self._chat_users[telegram_id] = chat_user
                return chat_user
            return None
        
    async def set(self, telegram_id: str, user_name: str) -> Optional[ChatUser]:
        async with self._lock:
            ok = await UserManager.update_chat_user(telegram_id, user_name)
            logger.info(f"Updated chat user for {telegram_id}: {ok}")
            if not ok:
                return None

            # If we already have the user cached, update it and return
            old_network = await selected_network_manager.get(telegram_id)
            cached = self._chat_users.get(telegram_id)
            if cached:
                cached.user_name = user_name
                self._chat_users[telegram_id] = cached
                if old_network:
                    await selected_network_manager.update(telegram_id, old_network.network_name, user_name)
                return cached

            # Otherwise, fetch fresh data from DB
            record = await UserManager.get_chat_user(telegram_id)
            if record:
                try:
                    chat_user = ChatUser(record[0], telegram_id, record[1], record[2], record[3], record[4])
                except Exception:
                    chat_user = ChatUser(int(record[0]), telegram_id, str(record[1]), bool(record[2]), bool(record[3]), str(record[4]))
                self._chat_users[telegram_id] = chat_user
                if old_network:
                    await selected_network_manager.update(telegram_id, old_network.network_name, record[1])
                return chat_user

            return None
    
    async def update(self, telegram_id: str, chat_user_id: int, user_name: str, receive_partnered_reports: bool = False, is_active: bool = False, order_by: str = "usage") -> Optional[ChatUser]:
        async with self._lock:
            chat_user = ChatUser(chat_user_id, telegram_id, user_name, receive_partnered_reports, is_active, order_by)
            self._chat_users[telegram_id] = chat_user
            logger.info(f"Updated chat user in cache for {telegram_id}: {chat_user}")
            return chat_user
    
    async def change_receive_partnered_reports(self, telegram_id: str, receive_partnered_report: bool) -> bool:
        async with self._lock:
            chat_user = self._chat_users.get(telegram_id)
            if not chat_user:
                logger.warning(f"No chat user in cache for {telegram_id} to update receive_partnered_reports")
                return False
            isChanged = await UserManager.change_receive_partnered_reports(chat_user.chat_user_id, receive_partnered_report)
            if isChanged:
                chat_user.receive_partnered_reports = receive_partnered_report
                self._chat_users[telegram_id] = chat_user
                logger.info(f"Updated receive_partnered_reports in cache for {telegram_id} to {receive_partnered_report}")
            return isChanged
        
    async def activate_chat_user(self, telegram_id: str) -> bool:
        async with self._lock:
            chat_user = self._chat_users.get(telegram_id)
            if not chat_user:
                # Try to fetch from DB if not cached
                record = await UserManager.get_chat_user(telegram_id)
                if not record:
                    logger.warning(f"No chat user found for {telegram_id} to activate")
                    return False
                try:
                    chat_user = ChatUser(record[0], telegram_id, record[1], record[2], record[3], record[4])
                except Exception:
                    # Fallback if record shape differs
                    chat_user = ChatUser(int(record[0]), telegram_id, str(record[1]), bool(record[2]), bool(record[3]), str(record[4]))
                self._chat_users[telegram_id] = chat_user

            # Activate by telegram_id via UserManager
            isChanged = await UserManager.activate_chat_user(telegram_id)
            if isChanged:
                chat_user.is_active = True
                self._chat_users[telegram_id] = chat_user
                logger.info(f"Activated chat user for {telegram_id}")
            return isChanged
        
    async def activate_chat_user_in_cache(self, telegram_id: str) -> bool:
        async with self._lock:
            chat_user = self._chat_users.get(telegram_id)
            if not chat_user:
                logger.warning(f"No chat user in cache for {telegram_id} to activate in cache")
                return False
            chat_user.is_active = True
            self._chat_users[telegram_id] = chat_user
            logger.info(f"Activated chat user in cache for {telegram_id}")
            return True
    
    async def deactivate_chat_user(self, telegram_id: str) -> bool:
        async with self._lock:
            chat_user = self._chat_users.get(telegram_id)
            if not chat_user:
                # Try to fetch from DB if not cached
                record = await UserManager.get_chat_user(telegram_id)
                if not record:
                    logger.warning(f"No chat user found for {telegram_id} to deactivate")
                    return False
                try:
                    chat_user = ChatUser(record[0], telegram_id, record[1], record[2], record[3], record[4])
                except Exception:
                    # Fallback if record shape differs
                    chat_user = ChatUser(int(record[0]), telegram_id, str(record[1]), bool(record[2]), bool(record[3]), str(record[4]))
                self._chat_users[telegram_id] = chat_user

            # Deactivate by telegram_id via UserManager
            isChanged = await UserManager.deactivate_chat_user(telegram_id)
            if isChanged:
                chat_user.is_active = False
                self._chat_users[telegram_id] = chat_user
                logger.info(f"Deactivated chat user for {telegram_id}")
            return isChanged
    
    async def change_order_by(self, telegram_id: str, order_by: str) -> bool:
        async with self._lock:
            chat_user = self._chat_users.get(telegram_id)
            if not chat_user:
                logger.warning(f"No chat user in cache for {telegram_id} to update order_by")
                return False
            isChanged = await UserManager.change_order_by(telegram_id, order_by)
            if isChanged:
                chat_user.order_by = order_by
                self._chat_users[telegram_id] = chat_user
                logger.info(f"Updated order_by in cache for {telegram_id} to {order_by}")
            return isChanged
        
chat_user_manager = ChatUserManager()