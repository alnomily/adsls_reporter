from PIL import Image, ImageDraw, ImageFont, ImageEnhance
import os
from datetime import datetime
from typing import List, Tuple, Dict, Optional
import arabic_reshaper
from bidi.algorithm import get_display
from bot.chat_user_manager import ChatUser
from bot.font_manager import font_manager
import re
from bot.selected_network_manager import selected_network_manager, SelectedNetwork
import logging

logger = logging.getLogger(__name__)
# from bot.chat_user_manager import chat_user_manager

class TableReportGenerator:
    def __init__(self):
        self.fonts = {}
        self.colors = {
            'bg_primary': (248, 249, 250),
            'header_bg': (52, 73, 94),
            'header_text': (255, 255, 255),
            'row_bg1': (255, 255, 255),
            'row_bg2': (242, 242, 242),
            'text_primary': (44, 62, 80),
            'text_secondary': (127, 140, 141),
            'success': (25, 144, 76),
            'warning': (211, 166, 5),
            'danger': (191, 56, 40),
            'border': (189, 195, 199),
            'row_border': (44, 62, 80),
            'accent': (41, 128, 185),
            'active_green': (36, 174, 83),
            'inactive_red': (191, 56, 40),
            'suspended_orange': (200, 106, 24),
            'positive_green': (39, 174, 96),
            'negative_red': (231, 76, 60),
            'neutral_blue': (52, 152, 219),
            'footer_row': (44, 62, 80),
            "black": (0, 0, 0)
        }
        self._load_fonts()
        self.max_rows_per_page = 30
        self.image_quality = 100
        self.image_width = 2339
        self.image_height = 1654
        self.right_margin = 50
        self.left_margin = 50
        # Optional attributes for footer/header
        self.username = None
        self.status = None
        self.total_sales = None
        self.after_total_sales = None
        self.profits = None
        self.client_name = None
        self.client_chat_id = None
        self.day_num = None

    def _load_fonts(self):
        self.fonts = {
            'title': font_manager.get_font('arabic_bold', 44),
            'table_header': font_manager.get_font('arabic_bold', 34),
            'header': font_manager.get_font('arabic_bold', 34),
            'bold': font_manager.get_font('arabic_bold', 30),
            'regular': font_manager.get_font('arabic', 20),
            'small': font_manager.get_font('arabic', 15),
            'medium': font_manager.get_font('arabic', 18),
            # Better English digits
            'digits': font_manager.get_font('digits', 20),
            'digits_bold': font_manager.get_font('digits_bold', 30),
        }

    def _contains_arabic(self, text: str) -> bool:
        try:
            s = str(text)
            return any(
                ('\u0600' <= ch <= '\u06FF')  # Arabic
                or ('\u0750' <= ch <= '\u077F')  # Arabic Supplement
                or ('\u08A0' <= ch <= '\u08FF')  # Arabic Extended-A
                or ('\uFB50' <= ch <= '\uFDFF')  # Arabic Presentation Forms-A
                or ('\uFE70' <= ch <= '\uFEFF')  # Arabic Presentation Forms-B
                for ch in s
            )
        except Exception:
            return False

    def _should_use_digits_font(self, text: str) -> bool:
        """Use digits font only when the full text is a numeric string (English digits).

        If the text contains Arabic letters (or any other non-numeric content), keep Arabic font.
        """
        try:
            s = str(text)
        except Exception:
            return False
        if self._contains_arabic(s):
            return False
        s = s.strip()
        if not s:
            return False
        # Allow common numeric formatting characters.
        # Examples allowed: 123, 1,234, 123.45, -12, +12, 1 234
        return re.fullmatch(r"[-+]?[0-9][0-9,\.\s]*", s) is not None

    def _pick_font_for_text(self, text: str, font: ImageFont.ImageFont) -> ImageFont.ImageFont:
        if not self._should_use_digits_font(text):
            return font
        size = getattr(font, 'size', None) or 20
        is_bold = font in (self.fonts.get('bold'), self.fonts.get('table_header'), self.fonts.get('header'), self.fonts.get('title'))
        return font_manager.get_font('digits_bold' if is_bold else 'digits', int(size))

    def _get_text_bbox_draw(self, draw: ImageDraw.Draw, text: str, font: ImageFont.ImageFont):
        try:
            if hasattr(draw, 'textbbox'):
                return draw.textbbox((0, 0), text, font=font)
        except Exception:
            pass
        return self._get_text_bbox(text, font)

    def _process_arabic_text(self, text: str) -> str:
        if text is None:
            return "-"
        try:
            s = str(text)
            # If the text doesn't contain Arabic characters, return as-is
            if not any('\u0600' <= ch <= '\u06FF' for ch in s):
                return s
            reshaped_text = arabic_reshaper.reshape(s)
            bidi_text = get_display(reshaped_text)
            return bidi_text
        except Exception:
            # Fallback to original text
            try:
                return str(text)
            except Exception:
                return "-"

        
    def _clean_text(self, text: str, max_length: int = 20) -> str:
        if not text:
            return "-"
        processed_text = self._process_arabic_text(text)
        if len(processed_text) > max_length:
            return processed_text[:max_length-2] + ".."
        return processed_text

    def _clean_numeric(self, value, is_currency: bool = False) -> str:
        if not value or value == '-':
            return "-"
        try:
            clean_value = re.sub(r'[^\d.]', '', str(value))
            if not clean_value:
                return "-"
            num_value = int(clean_value)
            if is_currency:
                return f"{num_value:,.2f}"
            else:
                return f"{num_value:,.0f}" if num_value == int(num_value) else f"{num_value:,.2f}"
        except Exception:
            return str(value)

    def _calculate_text_width(self, text: str, font: ImageFont.ImageFont) -> int:
        try:
            bbox = font.getbbox(text) if hasattr(font, 'getbbox') else font.getmask(text).getbbox()
            return bbox[2] - bbox[0] if bbox else len(text) * 10
        except Exception:
            return len(text) * 10

    def _get_text_bbox(self, text: str, font: ImageFont.ImageFont):
        try:
            # Use a small temporary image and ImageDraw.textbbox when available for accurate measurement
            tmp = Image.new('RGB', (10, 10))
            draw = ImageDraw.Draw(tmp)
            if hasattr(draw, 'textbbox'):
                return draw.textbbox((0, 0), text, font=font)
            if hasattr(font, 'getbbox'):
                return font.getbbox(text)
            mask_bbox = font.getmask(text).getbbox()
            return mask_bbox
        except Exception:
            return (0, 0, len(str(text)) * 12, 28)

    def _truncate_to_width(self, text: str, font: ImageFont.ImageFont, max_width: int) -> str:
        """Truncate text so it fits within max_width (pixels) using binary search on character length."""
        if not text:
            return text
        text = str(text)
        bbox = self._get_text_bbox(text, font)
        width = bbox[2] - bbox[0] if bbox else len(text) * 8
        if width <= max_width:
            return text
        ellipsis = '...'
        lo, hi = 0, len(text)
        best = ''
        while lo < hi:
            mid = (lo + hi) // 2
            candidate = text[:mid].rstrip() + ellipsis
            bbox_c = self._get_text_bbox(candidate, font)
            w = bbox_c[2] - bbox_c[0] if bbox_c else len(candidate) * 8
            if w <= max_width:
                best = candidate
                lo = mid + 1
            else:
                hi = mid
        return best or (text[:max(1, len(text)//2)].rstrip() + ellipsis)

    def _draw_rtl_table_header(self, draw: ImageDraw.Draw, y_pos: int):
        columns = [
            {"name": "م", "width": 40},
            {"name": "رقم الخط", "width": 180},
            {"name": "الباقة", "width": 150},
            {"name": "سعر الباقة", "width": 150},
            {"name": "الحالة", "width": 150},
            {"name": "رصيد الأمس", "width": 170},
            {"name": "الرصيد الحالي ", "width": 170},
            {"name": "الاستهلاك", "width": 150},
            {"name": "صلاحية الأيام", "width": 150},
            {"name": "تقدير انتهاء الرصيد", "width": 250},
            {"name": "قيمة الرصيد", "width": 190},
            {"name": "قيمة الاستهلاك", "width": 190},
            {"name": "ملاحظات", "width": 298},
        ]
        header_height = 50  # Slightly increased for bigger font
        draw.rectangle([self.left_margin, y_pos, self.image_width - self.right_margin, y_pos + header_height],
                    fill=self.colors['accent'])
        current_x = self.image_width - self.right_margin
        for col in columns:
            col_width = col["width"]
            col_left = current_x - col_width
            ar_text = self._process_arabic_text(col["name"])
            ar_bbox = self._get_text_bbox_draw(draw, ar_text, self.fonts['table_header'])
            tb_x0, tb_y0, tb_x1, tb_y1 = ar_bbox
            ar_width = tb_x1 - tb_x0
            ar_height = tb_y1 - tb_y0
            ar_x = col_left + (col_width - ar_width) // 2 - tb_x0
            ar_y = y_pos + max(0, (header_height - ar_height) // 2) - tb_y0
            draw.text((ar_x, ar_y), ar_text, fill=self.colors['header_text'], font=self.fonts['table_header'])
            current_x = col_left
        draw.line([(self.left_margin, y_pos), (self.left_margin, y_pos + header_height)],
                fill=self.colors['border'], width=1)
        return header_height, columns

    def _draw_rtl_table_footer(self, draw: ImageDraw.Draw, y_pos: int, totals: Optional[Dict] = None):
        columns = [
            {"name": "", "width": 40},
            {"name": "الاجمالي", "width": 180},
            {"name": "", "width": 150},
            {"name": "", "width": 150},
            {"name": "", "width": 150},
            {"name": "", "width": 170},
            {"name": "", "width": 170},
            {"name": "", "width": 150},
            {"name": "", "width": 150},
            {"name": "", "width": 250},
            {"name": "", "width": 190},
            {"name": "", "width": 190},
            {"name": "", "width": 298},
        ]
        header_height = 40
        draw.rectangle([self.left_margin, y_pos, self.image_width - self.right_margin, y_pos + header_height],
                    fill=self.colors['accent'])
        current_x = self.image_width - self.right_margin
        for col in columns:
            col_width = col["width"]
            col_left = current_x - col_width
            ar_text = self._process_arabic_text(col["name"])
            ar_bbox = self._get_text_bbox_draw(draw, ar_text, self.fonts['table_header'])
            tb_x0, tb_y0, tb_x1, tb_y1 = ar_bbox
            ar_width = tb_x1 - tb_x0
            ar_height = tb_y1 - tb_y0
            ar_x = col_left + (col_width - ar_width) // 2 - tb_x0
            ar_y = y_pos + max(0, (header_height - ar_height) // 2) - tb_y0
            draw.text((ar_x, ar_y), ar_text, fill=self.colors['header_text'], font=self.fonts['table_header'])
            current_x = col_left

        # Show totals in the correct columns
        if totals:
            # mapping of column index -> (totals_key, is_currency)
            col_totals_map = {
                5: ('yesterday_balance', True),
                6: ('today_balance', True),
                7: ('usage', True),
                10: ('balance_value', True),
                11: ('consumption_value', True),
            }
            footer_y = y_pos + 5
            current_x = self.image_width - self.right_margin
            for i, col in enumerate(columns):
                col_width = col['width']
                col_left = current_x - col_width
                if i in col_totals_map:
                    key, is_currency = col_totals_map[i]
                    val = totals.get(key, 0.0)
                    text = self._clean_numeric(str(val), is_currency=is_currency)
                    font = self._pick_font_for_text(text, self.fonts['bold'])
                    text_bbox = self._get_text_bbox_draw(draw, text, font)
                    tb_x0, tb_y0, tb_x1, tb_y1 = text_bbox
                    text_w = tb_x1 - tb_x0
                    text_h = tb_y1 - tb_y0
                    tx = col_left + max(2, (col_width - text_w) // 2) - tb_x0
                    ty = y_pos + max(0, (header_height - text_h) // 2) - tb_y0
                    draw.text((tx, ty), text, fill=self.colors['header_text'], font=font)
                current_x = col_left

        return header_height, columns

    def _calculate_page_totals(self, lines_data: List[Tuple[str, Dict]]):
        totals = {
            'yesterday_balance': 0.0,
            'today_balance': 0.0,
            'usage': 0.0,
            'balance_value': 0.0,
            'consumption_value': 0.0,
        }
        total_lines = len(lines_data) if lines_data else 0
        for _, acct in lines_data:
            try:
                raw = acct.get('yesterday_balance', '-')
                cleaned = re.sub(r'[^\d.\-]', '', str(raw))
                if cleaned not in ['', '-', None]:
                    totals['yesterday_balance'] += float(cleaned)
            except Exception:
                pass
            try:
                raw = acct.get('today_balance', '-')
                cleaned = re.sub(r'[^\d.\-]', '', str(raw))
                if cleaned not in ['', '-', None]:
                    totals['today_balance'] += float(cleaned)
            except Exception:
                pass
            try:
                raw = acct.get('usage', '-')
                cleaned = re.sub(r'[^\d.\-]', '', str(raw))
                if cleaned not in ['', '-', None]:
                    totals['usage'] += float(cleaned)
            except Exception:
                pass
            try:
                raw = acct.get('balance_value', '-')
                cleaned = re.sub(r'[^\d.\-]', '', str(raw))
                if cleaned not in ['', '-', None]:
                    totals['balance_value'] += float(cleaned)
            except Exception:
                pass
            try:
                raw = acct.get('usage_value', '-')
                cleaned = re.sub(r'[^\d.\-]', '', str(raw))
                if cleaned not in ['', '-', None]:
                    totals['consumption_value'] += float(cleaned)
            except Exception:
                pass
            for k in totals:
                totals[k] = round(totals[k], 2)
       
        return totals

    def _draw_rtl_table_row(self, draw: ImageDraw.Draw, network:SelectedNetwork, row_data: Tuple[str, Dict], y_pos: int,
                            columns: List[Dict], row_index: int):
        line_number, account_data = row_data
        bg_color = self.colors['row_bg1'] if row_index % 2 == 0 else self.colors['row_bg2']
        row_height = 40
        draw.rectangle([self.left_margin, y_pos, self.image_width - self.right_margin, y_pos + row_height], fill=bg_color)
        current_x = self.image_width - self.right_margin
        col_data = []
        # id
        id_text = self._clean_text(account_data.get('order_index', '-'), 18)
        col_data.append((id_text, self.colors['text_primary'], self.fonts['bold']))
        # line number
        line_text = self._clean_text(line_number, 15)
        col_data.append((line_text, self.colors['accent'], self.fonts['bold']))
        # plan
        plan_text = self._clean_text(account_data.get('plan_limit', '-'), 18)
        col_data.append((plan_text, self.colors['text_primary'], self.fonts['bold']))
        # price plan
        price_plan_text = self._clean_text(account_data.get('plan_price', '-'), 18)
        col_data.append((price_plan_text, self.colors['text_primary'], self.fonts['bold']))
        # status
        status_raw = account_data.get('account_status', '-')
        status_text = self._clean_text(status_raw, 12)
        status_norm = str(status_raw).lower()
        if any(k in status_norm for k in ['حساب نشط', 'active']):
            status_color = self.colors['active_green']
        elif any(k in status_norm for k in ['بلا رصيد', 'معلق', 'suspend', 'suspended']):
            status_color = self.colors['suspended_orange']
        elif any(k in status_norm for k in ['فصلت الخدمة', 'وقف', 'غير', 'inactive', 'disabled', 'stop']):
            status_color = self.colors['inactive_red']
        else:
            status_color = self.colors['neutral_blue']
        col_data.append((status_text, status_color, self.fonts['bold']))
        # yesterday balance
        yb_raw = str(account_data.get('yesterday_balance', '-') or '-')
        yesterday_balance = self._clean_numeric(yb_raw.replace('جيجابايت', ''), True)
        col_data.append((yesterday_balance, self.colors['text_primary'], self.fonts['bold']))
        # current balance
        tb_raw = str(account_data.get('today_balance', '-') or '-')
        current_balance = self._clean_numeric(tb_raw.replace('جيجابايت', ''), True)
        if current_balance != '-' and any(c.isdigit() for c in current_balance):
            try:
                balance_num = float(current_balance.replace(',', ''))
                data_limit = account_data.get('plan_limit', None)
                if data_limit and data_limit != '-':
                    data_limit_num = float(re.sub(r'[^\d.]', '', str(data_limit)))
                    if data_limit_num > 0:
                        usage_ratio = balance_num / data_limit_num
                        if usage_ratio <= network.danger_percentage_remaining_balance / 100.0:
                            balance_color = self.colors['danger']
                        elif usage_ratio <= network.warning_percentage_remaining_balance / 100.0:
                            balance_color = self.colors['warning']
                        else:
                            balance_color = self.colors['positive_green']
                    else:
                        balance_color = self.colors['text_primary']
                else:
                    balance_color = self.colors['text_primary']
            except Exception as e:
                logger.error("Error calculating balance color for line %s: %s", line_number, e)
                balance_color = self.colors['text_primary']
        else:
            balance_color = self.colors['text_primary']
        col_data.append((current_balance, balance_color, self.fonts['bold']))
        # consumption
        consumption = self._clean_numeric(account_data.get('usage', '-'), True)
        col_data.append((consumption, self.colors['text_primary'], self.fonts['bold']))
        # days validity
        days_validity = account_data.get('remaining_days', '-')
        days_text = str(days_validity) if days_validity != '-' else '-'
        if days_text.isdigit():
            days_num = int(days_text)
            if days_num <= network.danger_count_remaining_days:
                days_validity_color = self.colors['danger']
            elif days_num <= network.warning_count_remaining_days:
                days_validity_color = self.colors['warning']
            else:
                days_validity_color = self.colors['success']
        else:
            days_validity_color = self.colors['text_primary']
        col_data.append((days_text, days_validity_color, self.fonts['bold']))
        # finishing balance estimate
        finishing_balance_estimate_text = account_data.get('finishing_balance_estimate', '-')
        try:
            if finishing_balance_estimate_text != '-' and any(c.isdigit() for c in finishing_balance_estimate_text):
                finishing_balance_estimate = int(finishing_balance_estimate_text.replace(',', ''))
            else:
                finishing_balance_estimate = -1
        except Exception:
            finishing_balance_estimate = -1
        finishing_balance_estimate_text = str(finishing_balance_estimate) if finishing_balance_estimate != '-' else '-'
        if finishing_balance_estimate != -1:
            days_num = finishing_balance_estimate
            if days_num <= network.danger_count_remaining_days:
                days_color = self.colors['danger']
            elif days_num <= network.warning_count_remaining_days:
                days_color = self.colors['warning']
            else:
                days_color = self.colors['success']
        else:
            finishing_balance_estimate_text = '-'
            days_color = self.colors['text_primary']
        col_data.append((finishing_balance_estimate_text, days_color, self.fonts['bold']))
        # balance value
        balance_value = self._clean_numeric(account_data.get('balance_value', 0.0), False)
        col_data.append((balance_value, self.colors['positive_green'], self.fonts['bold']))
        # consumption value
        consumption_value = self._clean_numeric(account_data.get('usage_value', 0.0), False)
        col_data.append((consumption_value, self.colors['negative_red'], self.fonts['bold']))
        # notes
        raw_notes = str(account_data.get('notes', '') or '').strip()
        notes_text = self._clean_text(raw_notes if raw_notes else '-', 50)
        special_note = None
        note_color = None
        # If no raw notes provided, infer from balance/days colors
        if not raw_notes:
            if balance_color == self.colors['danger']:
                special_note = self._clean_text("رصيد منخفض جداً", 50)
                note_color = self.colors['danger']
            elif balance_color == self.colors['warning']:
                special_note = self._clean_text("رصيد منخفض", 50)
                note_color = self.colors['warning']
            elif days_validity_color == self.colors['danger']:
                special_note = self._clean_text("على وشك انتهاء الصلاحية", 50)
                note_color = self.colors['danger']
            elif days_validity_color == self.colors['warning']:
                special_note = self._clean_text("قارب على انتهاء الصلاحية", 50)
                note_color = self.colors['warning']
        if special_note:
            col_data.append((special_note, note_color, self.fonts['bold']))
        else:
            # Decide color using raw (unprocessed) Arabic text to avoid reshape/truncation mismatches
            rn = raw_notes
            if "لا يوجد رصيد في الخط" in rn:
                note_color = self.colors['danger']
            elif "أول تسجيل للرصيد" in rn:
                note_color = self.colors['warning']
            elif "تم تسديد" in rn:
                note_color = self.colors['success']
            # Handle confiscation-related notes
            elif "تم مصادرة الخط اليوم" in rn:
                note_color = self.colors['danger']
            elif "سيتم مصادرة الخط اليوم" in rn:
                note_color = self.colors['warning']
            elif "تم مصادرة الخط" in rn:
                note_color = self.colors['danger']
            elif "متبقي يوم لمصادرة الخط" in rn:
                note_color = self.colors['warning']
            elif "متبقي يومين لمصادرة الخط" in rn:
                note_color = self.colors['warning']
            elif re.search(r"متبقي \d+ يوم لمصادرة الخط", rn):
                note_color = self.colors['warning']
            else:
                note_color = self.colors['text_secondary']
            col_data.append((notes_text, note_color, self.fonts['bold']))
        
        for i, (text, color, font) in enumerate(col_data):
            font = self._pick_font_for_text(text, font)
            col_width = columns[i]["width"]
            col_left = current_x - col_width

            # Reserve a small padding inside the cell
            padding = 6
            max_text_w = max(10, col_width - padding * 2)

            safe_text = self._truncate_to_width(text, font, max_text_w)
            text_bbox = self._get_text_bbox_draw(draw, safe_text, font)
            # bbox may be (x0, y0, x1, y1) where y0 can be negative for some scripts/fonts
            if text_bbox:
                tb_x0, tb_y0, tb_x1, tb_y1 = text_bbox
                text_width = tb_x1 - tb_x0
                text_height = tb_y1 - tb_y0
                # Center within padded area, compensating for bbox bearings (x0/y0)
                avail_w = max_text_w
                text_x = col_left + padding + max(0, (avail_w - text_width) // 2) - tb_x0
                text_y = y_pos + max(0, (row_height - text_height) // 2) - tb_y0
            else:
                text_width = len(safe_text) * 8
                text_height = 18
                text_x = col_left + padding + max(0, (max_text_w - text_width) // 2)
                text_y = y_pos + max(2, (row_height - text_height) // 2)

            draw.text((text_x, text_y), safe_text, fill=color, font=font)
            current_x = col_left

        # Subtle separator to avoid thick artifacts
        draw.line([(self.left_margin, y_pos + row_height), (self.image_width - self.right_margin, y_pos + row_height)],
                  fill=self.colors['border'], width=1)
        return row_height

    def _calculate_remaining_days(self, expiry_date: str) -> str:
        """Return remaining days until expiry_date. Supports YYYY-MM-DD and DD/MM/YYYY."""
        if not expiry_date or expiry_date == '-':
            logger.info("Expiry date is empty or invalid: %s", expiry_date)
            return "-"
        text = str(expiry_date).strip()
        logger.info("Parsing expiry date: %s", text)

        parsed = None
        # Try ISO first (YYYY-MM-DD)
        try:
            parsed = datetime.fromisoformat(text).date()
            logger.info("Parsed ISO date successfully: %s", parsed)
        except Exception as e:
            logger.info("Failed to parse ISO date: %s, error: %s", text, e)
            pass

        # Try DD/MM/YYYY if ISO failed
        if parsed is None:
            try:
                date_match = re.search(r'(\d{1,2}-\d{1,2}-\d{4})', text)
                if date_match:
                    day, month, year = map(int, date_match.group(1).split('-'))
                    parsed = datetime(year, month, day).date()
            except Exception as e:
                logger.info("Failed to parse DD-MM-YYYY date: %s, error: %s", text, e)
                parsed = None

        if parsed is None:
            return "-"

        today = datetime.now().date()
        delta = (parsed - today).days
        return str(max(delta, 0))

    def _draw_summary_footer(self, draw: ImageDraw.Draw, y_pos: int, total_lines: int, current_page: int, total_pages: int):
        footer_height = 60
        draw.rectangle([self.left_margin, y_pos, self.image_width - self.right_margin, y_pos + footer_height],
                       fill=self.colors['accent'], outline=self.colors['border'], width=2)

        # Prepare texts and fonts
        page_suffix = (
            f" | الصفحة: {current_page} من {total_pages}" if total_pages > 1 else ""
        )
        summary_text = self._process_arabic_text(f"إجمالي الخطوط: {total_lines}{page_suffix}")
        summary_font = self.fonts.get('header', self.fonts.get('header'))

        sales_text = None
        sales_font = self.fonts.get('regular')
        if getattr(self, "status", None):
            sales_text = self._process_arabic_text(
                f"--اجمالي المبيعات {self.total_sales or ''} ريال يمني ارباح نقاط بيع الكروت {self.after_total_sales or ''} ريال يمني ارباحك {self.profits or ''}"
            )

        # Measure bboxes using the chosen fonts
        sb_bbox = self._get_text_bbox_draw(draw, summary_text, summary_font)
        if sb_bbox:
            sb_x0, sb_y0, sb_x1, sb_y1 = sb_bbox
            sb_w = sb_x1 - sb_x0
            sb_h = sb_y1 - sb_y0
        else:
            sb_w = len(summary_text) * 8
            sb_h = 18
            sb_x0 = sb_y0 = 0

        sales_w = sales_h = sales_x0 = sales_y0 = 0
        if sales_text:
            s_bbox = self._get_text_bbox_draw(draw, sales_text, sales_font)
            if s_bbox:
                sales_x0, sales_y0, sx1, sy1 = s_bbox
                sales_w = sx1 - sales_x0
                sales_h = sy1 - sales_y0
            else:
                sales_w = len(sales_text) * 8
                sales_h = 14
                sales_x0 = sales_y0 = 0

        # Compute total block height (summary + optional spacing + sales)
        spacing = 6 if sales_text else 0
        block_h = sb_h + (sales_h + spacing if sales_text else 0)

        # Top of the block to center within footer rectangle
        block_top = y_pos + max(0, (footer_height - block_h) // 2)

        # Draw summary centered
        sx = (self.image_width - sb_w) // 2 - sb_x0
        sy = block_top - sb_y0
        draw.text((sx, sy), summary_text, fill=self.colors['header_text'], font=summary_font)

        # Draw sales text (if any) below the summary, centered
        if sales_text:
            sales_x = (self.image_width - sales_w) // 2 - sales_x0
            sales_y = block_top + sb_h + spacing - sales_y0
            draw.text((sales_x, sales_y), sales_text, fill=self.colors['black'], font=sales_font)

        return footer_height

    def _draw_report_header(self, draw: ImageDraw.Draw, network: SelectedNetwork, chat_user: ChatUser, current_page: int, total_pages: int, report_date: str = "") -> int:
        top_y = 12
        padding_x = 20
        left_x = self.left_margin + padding_x
        right_x = self.image_width - self.right_margin - padding_x

        # Left: remaining days
        logger.info("Expiry value for network %s: %s", network.network_name, network.expiration_date)
        left_days = self._calculate_remaining_days(network.expiration_date) if network.expiration_date else "-"
        day_count = f"الأيام المتبقية لانتهاء الاشتراك: {left_days}" if left_days not in ("-", "", None) else ""
        day_count_ar = self._process_arabic_text(day_count)
        header_font = self.fonts['header']
        dc_bbox = self._get_text_bbox_draw(draw, day_count_ar, header_font)
        dc_x0, dc_y0, dc_x1, dc_y1 = dc_bbox
        dc_h = dc_y1 - dc_y0
        draw.text((left_x - dc_x0, top_y - dc_y0), day_count_ar, fill=self.colors['text_secondary'], font=header_font)

        # Right: client info
        clints = self.client_name or network.user_name
        clint_name_ar = self._process_arabic_text(f"اسم المشترك : {clints}")
        cn_bbox = self._get_text_bbox_draw(draw, clint_name_ar, header_font)
        cn_x0, cn_y0, cn_x1, cn_y1 = cn_bbox
        cn_w = cn_x1 - cn_x0
        cn_h = cn_y1 - cn_y0
        clint_name_x = right_x - cn_w - cn_x0
        clint_name_y = top_y - cn_y0
        draw.text((clint_name_x, clint_name_y), clint_name_ar, fill=self.colors['text_secondary'], font=header_font)

        clints_chat = chat_user.chat_user_id or self.client_chat_id or "----"
        clint_chat_id_ar = self._process_arabic_text(f"معرف المشترك : {clints_chat}")
        cc_bbox = self._get_text_bbox_draw(draw, clint_chat_id_ar, header_font)
        cc_x0, cc_y0, cc_x1, cc_y1 = cc_bbox
        cc_w = cc_x1 - cc_x0
        cc_h = cc_y1 - cc_y0
        spacing = 6
        chat_x = right_x - cc_w - cc_x0
        chat_y = (clint_name_y + cn_h + spacing) - cc_y0
        draw.text((chat_x, chat_y), clint_chat_id_ar, fill=self.colors['text_secondary'], font=header_font)

        # Center: title + subtitle
        title_ar = self._process_arabic_text("تقرير خطوط النت لشبكة {}".format(network.network_name))
        title_bbox = self._get_text_bbox_draw(draw, title_ar, self.fonts['title'])
        t_x0, t_y0, t_x1, t_y1 = title_bbox
        t_w = t_x1 - t_x0
        t_h = t_y1 - t_y0
        title_y = top_y
        title_x = (self.image_width - t_w) // 2 - t_x0
        draw.text((title_x, title_y - t_y0), title_ar, fill=self.colors['text_primary'], font=self.fonts['title'])

        subtitle_ar = self._process_arabic_text("تفاصيل الرصيد والاستهلاك")
        subtitle_bbox = self._get_text_bbox_draw(draw, subtitle_ar, self.fonts['header'])
        s_x0, s_y0, s_x1, s_y1 = subtitle_bbox
        s_w = s_x1 - s_x0
        s_h = s_y1 - s_y0
        subtitle_y = title_y + t_h + 6
        subtitle_x = (self.image_width - s_w) // 2 - s_x0
        draw.text((subtitle_x, subtitle_y - s_y0), subtitle_ar, fill=self.colors['text_secondary'], font=self.fonts['header'])

        # Timestamp (right aligned, under subtitle)
        timestamp = datetime.now().strftime("%Y/%m/%d   الساعة : %H:%M:%S") if not report_date else report_date
        timestamp_ar = self._process_arabic_text(f"تاريخ التقرير: {timestamp}")
        timestamp_y = subtitle_y + s_h + 10
        ts_bbox = self._get_text_bbox_draw(draw, timestamp_ar, header_font)
        ts_x0, ts_y0, ts_x1, ts_y1 = ts_bbox
        ts_w = ts_x1 - ts_x0
        ts_h = ts_y1 - ts_y0
        timestamp_x = right_x - ts_w - ts_x0
        draw.text((timestamp_x, timestamp_y - ts_y0), timestamp_ar, fill=self.colors['text_secondary'], font=header_font)

        bottom = max(
            timestamp_y + ts_h,
            chat_y + cc_h,
            top_y + dc_h,
            subtitle_y + s_h,
            title_y + t_h,
        )
        return int(bottom + 12)

    def _enhance_image_quality(self, image: Image.Image) -> Image.Image:
        enhancer = ImageEnhance.Sharpness(image)
        image = enhancer.enhance(1.3)
        enhancer = ImageEnhance.Contrast(image)
        image = enhancer.enhance(1.15)
        return image

    def  generate_financial_table_report(self, lines_data: List[Tuple[str, Dict]], network: SelectedNetwork, chat_user: ChatUser, save_path: str = None, report_date: str = "") -> List[str]:
        # Set optional attributes if provided in lines_data (for compatibility)
        # If lines_data is a tuple as in your new design, unpack accordingly
        if isinstance(lines_data, tuple) and len(lines_data) == 3:
            self.username = lines_data[0]
            self.status = bool(lines_data[2].get("card_price"))
            self.total_sales = lines_data[2].get("total_sales", None)
            self.after_total_sales = lines_data[2].get("after_total_sales", None)
            self.profits = lines_data[2].get("profits", None)
            self.client_name = lines_data[2].get("client_name", None)
            self.client_chat_id = lines_data[2].get("client_chat_id", None)
            self.day_num = lines_data[2].get("day_num", None)
            lines = lines_data[1]
        else:
            lines = lines_data
        if not lines:
            return []
        # Calculate totals for all lines and store in self.totals
        self.totals = self._calculate_page_totals(lines)
        data_chunks = [
            lines[i:i + self.max_rows_per_page]
            for i in range(0, len(lines), self.max_rows_per_page)
        ]
        total_pages = len(data_chunks)
        image_paths = []
        for page_num, data_chunk in enumerate(data_chunks, 1):
            page_save_path = None
            if save_path:
                base, ext = os.path.splitext(save_path)
                # normalize to .jpg output
                page_save_path = f"{base}_page{page_num}.jpg"
            image_path = self._generate_single_page(
                data_chunk, page_num, total_pages, len(lines), network, chat_user, page_save_path, report_date
            )
            image_paths.append(image_path)
        return image_paths


    def _generate_single_page(self, lines_data: List[Tuple[str, Dict]], current_page: int,
                          total_pages: int, lines_count: int, network: SelectedNetwork, chat_user: ChatUser, save_path: str = None, report_date: str = "") -> str:
        if save_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_path = f"reports/financial_report_{timestamp}_page{current_page}.jpg"
        os.makedirs("reports", exist_ok=True)
        width = self.image_width
        height = self.image_height
        image = Image.new("RGB", (width, height), self.colors['bg_primary'])
        draw = ImageDraw.Draw(image)
        header_bottom = self._draw_report_header(draw,network, chat_user, current_page, total_pages, report_date)
        table_start_y = max(175, header_bottom + 20)
        header_h, columns = self._draw_rtl_table_header(draw, table_start_y)
        current_y = table_start_y + header_h
        max_table_height = height - 200
        for i, line_data in enumerate(lines_data):
            logger.info("Drawing row %d for line %s", i + 1, line_data)
            if current_y + 25 > max_table_height:
                break
            row_h = self._draw_rtl_table_row(draw,network, line_data, current_y, columns, i)
            current_y += row_h

        # Use self.totals for the footer (totals for all lines)
        try:
            page_totals = self.totals
        except Exception:
            page_totals = None
        self._draw_rtl_table_footer(draw, current_y, totals=page_totals)
        footer_y = height - 100
        self._draw_summary_footer(draw, footer_y, lines_count, current_page, total_pages)
        draw.rectangle([3, 3, width-4, height-4], outline=self.colors['border'], width=3)
        draw.rectangle([6, 6, width-7, height-7], outline=self.colors['accent'], width=1)
        image = self._enhance_image_quality(image)
        # ensure RGB and save as JPEG to maximize Telegram compatibility
        if image.mode != 'RGB':
            image = image.convert('RGB')
        image.save(save_path, "JPEG", optimize=True, quality=self.image_quality)
        return save_path


def extract_date(text):
    match = re.search(r'\b(\d{1,2}/\d{1,2}/\d{4})\b', text)
    return match.group(1) if match else None
