# report_generator.py
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from PIL import Image, ImageDraw, ImageFont

# إعداد الاتصال مع Supabase
from bot.utils_shared import run_blocking, sync_get_users_ordered, sync_get_account_available_balance


# بيانات الحساب
@dataclass
class AccountRecord:
    user_id: str
    username: str
    adsl_number: str
    available_balance: float
    previous_balance: Optional[float]
    daily_usage: Optional[float]
    days_left: Optional[int]
    balance_diff: Optional[float]
    expiry_date: Optional[str]
    updated_at: Optional[str]


# مولد التقرير
class ReportGenerator:
    def __init__(self):
        self.font_regular = "assets/fonts/arial.ttf"
        self.font_bold = "assets/fonts/arialbd.ttf"
        os.makedirs("reports", exist_ok=True)

    def generate(self):
        raise NotImplementedError("Will be implemented in next part")
    def get_users_data(self) -> List[AccountRecord]:
        """
        Fetch users with their latest available balance from Supabase
        """
        try:
            # sync_get_users_ordered is a synchronous helper that returns the supabase response
            users_resp = sync_get_users_ordered()
            # run_blocking returns the supabase response object

            account_records: List[AccountRecord] = []

            for user in users_resp.data:
                user_id = user["id"]
                username = user.get("username", "N/A")
                adsl = user.get("adsl_number", "N/A")
                expiry = user.get("expiry_date")
                updated_at = user.get("updated_at")

                latest_balance = self.get_latest_balance(user_id)
                prev_balance = self.get_previous_day_balance(user_id)

                daily_usage = None
                balance_diff = None
                days_left = None

                if latest_balance is not None and prev_balance is not None:
                    daily_usage = max(prev_balance - latest_balance, 0)
                    balance_diff = abs(prev_balance - latest_balance)
                    days_left = int(latest_balance / daily_usage) if daily_usage > 0 else None

                record = AccountRecord(
                    user_id=user_id,
                    username=username,
                    adsl_number=adsl,
                    available_balance=latest_balance if latest_balance else 0,
                    previous_balance=prev_balance,
                    daily_usage=daily_usage,
                    days_left=days_left,
                    balance_diff=balance_diff,
                    expiry_date=expiry,
                    updated_at=updated_at
                )
                account_records.append(record)
            return account_records

        except Exception as e:
            print(f"[ERROR] get_users_data: {e}")
            return []

    def get_latest_balance(self, user_id: str) -> Optional[float]:
        """
        Get latest available balance for a user
        """
        try:
            resp = sync_get_account_available_balance(user_id, 0)
            data = getattr(resp, "data", None) or []
            if data:
                return self.extract_gb_value(data[0].get("available_balance"))
            return None
        except:
            return None

    def get_previous_day_balance(self, user_id: str) -> Optional[float]:
        """
        Get balance from yesterday to calculate daily usage
        """
        try:
            resp = sync_get_account_available_balance(user_id, 1)
            data = getattr(resp, "data", None) or []
            if data:
                return self.extract_gb_value(data[0].get("available_balance"))
            return None
        except:
            return None

    def extract_gb_value(self, balance_str: str) -> float:
        """
        Extract numeric GB from balance text like '245.98 جيجابايت'
        """
        try:
            return float(balance_str.split(" ")[0].replace(",", ""))
        except:
            return 0.0
    def generate_report_image(self, records: List[AccountRecord], output_path="reports/report.png"):
        # إعداد مقاسات الصورة
        width = 1600
        row_height = 70
        header_height = 200
        height = header_height + row_height * (len(records) + 2)
        bg_color = (245, 245, 245)  # خلفية رمادية خفيفة

        # إنشاء الصورة
        img = Image.new("RGB", (width, height), bg_color)
        draw = ImageDraw.Draw(img)

        # تحميل الخطوط
        font_title = ImageFont.truetype(self.font_bold, 48)
        font_header = ImageFont.truetype(self.font_bold, 30)
        font_cell = ImageFont.truetype(self.font_regular, 28)

        # ===== عنوان التقرير =====
        draw.text((width // 2 - 200, 30), "📡 SAM NET REPORT", fill="black", font=font_title)

        # ===== رأس الجدول =====
        headers = [
            "الرقم", "المستخدم", "ADSL", "الرصيد الحالي (GB)",
            "الاستهلاك اليومي (GB)", "الأيام المتبقية", "فرق الرصيد", "تاريخ آخر تحديث"
        ]
        x_positions = [50, 180, 380, 600, 850, 1100, 1300, 1470]

        for i, header in enumerate(headers):
            draw.text((x_positions[i], 130), header, fill="black", font=font_header)

        # ===== البيانات =====
        y = 200
        for idx, rec in enumerate(records, start=1):
            values = [
                str(idx),
                rec.username,
                rec.adsl_number,
                f"{rec.available_balance:.2f}",
                f"{rec.daily_usage:.2f}" if rec.daily_usage else "-",
                str(rec.days_left) if rec.days_left else "-",
                f"{rec.balance_diff:.2f}" if rec.balance_diff else "-",
                str(rec.updated_at).split("T")[0] if rec.updated_at else "-"
            ]
            for i, val in enumerate(values):
                draw.text((x_positions[i], y), val, fill="black", font=font_cell)
            y += row_height

        img.save(output_path)
        return output_path
    def build_and_export(self):
        """
        Full process:
        1. Fetch users + balances
        2. Calculate usage
        3. Generate image report
        """
        print("🔄 Fetching users data...")
        records = self.get_users_data()
        if not records:
            print("⚠️ No data found!")
            return None

        print(f"✅ {len(records)} users loaded. Generating report image...")
        path = self.generate_report_image(records)
        print(f"📄 Report saved -> {path}")
        return path
