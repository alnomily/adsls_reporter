from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from enum import Enum


class AccountStatus(Enum):
    ACTIVE = "نشط"
    INACTIVE = "غير نشط"
    EXPIRED = "منتهي"
    SUSPENDED = "موقوف"


@dataclass
class AccountData:
    username: str
    account_type: Optional[str] = None
    status: Optional[str] = None
    expiry_date: Optional[str] = None
    remaining_days: Optional[str] = None
    package: Optional[str] = None
    balance: Optional[str] = None
    available_balance: Optional[str] = None
    subscription_date: Optional[str] = None
    plan: Optional[str] = None
    last_update: Optional[datetime] = None
    scraped_at: Optional[str] = None
    
    def is_active(self) -> bool:
        return self.status and "نشط" in self.status
    
    def get_status_emoji(self) -> str:
        if self.is_active():
            return "🟢"
        elif self.status and any(word in self.status for word in ["منتهي", "موقوف"]):
            return "🔴"
        else:
            return "🟡"
    
    def get_balance_emoji(self) -> str:
        if self.available_balance and any(char.isdigit() for char in self.available_balance):
            try:
                balance_num = float(self.available_balance.split()[0].replace(',', ''))
                if balance_num > 0:
                    return "💳"
            except:
                pass
        return "❌"


@dataclass
class UserReport:
    account: AccountData
    requested_by: Optional[str] = None
    fetched_at: datetime = None
    is_fresh: bool = False
    
    def __post_init__(self):
        if self.fetched_at is None:
            self.fetched_at = datetime.now()