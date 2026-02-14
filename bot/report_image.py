from PIL import Image, ImageDraw, ImageFont
import os
from datetime import datetime
from bot.user_report import AccountData, UserReport
from typing import Optional
import arabic_reshaper
from bidi.algorithm import get_display
from bot.font_manager import font_manager

# pip install arabic-reshaper python-bidi


class ReportImageGenerator:
    def __init__(self):
        self.fonts = {}
        self.colors = {
            'bg_primary': (245, 247, 250),
            'bg_header': (41, 128, 185),  # Blue header
            'bg_success': (39, 174, 96),   # Green for active
            'bg_warning': (241, 196, 15),  # Yellow for warning
            'bg_danger': (231, 76, 60),    # Red for inactive
            'text_primary': (44, 62, 80),
            'text_secondary': (127, 140, 141),
            'text_white': (255, 255, 255),
            'border': (189, 195, 199),
            'accent_blue': (52, 152, 219),
            'accent_green': (46, 204, 113),
            'accent_orange': (230, 126, 34)
        }
        self._load_fonts()
    
    def _load_fonts(self):
        """Load fonts with Arabic support"""
        # Use the shared FontManager so Docker gets the best available offline Arabic font
        # (e.g., Amiri if installed) and local mounted fonts take precedence.
        self.fonts = {
            'title_bold': font_manager.get_font('arabic_bold', 28),
            'header': font_manager.get_font('arabic_bold', 22),
            'bold': font_manager.get_font('arabic_bold', 18),
            'regular': font_manager.get_font('arabic', 16),
            'small': font_manager.get_font('arabic', 14),
            'large': font_manager.get_font('arabic_bold', 32),
        }
    
    def _process_arabic_text(self, text: str) -> str:
        """Process Arabic text for proper rendering"""
        if not text:
            return "N/A"
        
        # Check if text contains Arabic characters
        arabic_chars = any('\u0600' <= char <= '\u06FF' for char in str(text))
        
        if arabic_chars:
            try:
                # Reshape and apply bidirectional algorithm for Arabic
                reshaped_text = arabic_reshaper.reshape(str(text))
                return get_display(reshaped_text)
            except:
                # Fallback if Arabic processing fails
                return str(text)
        return str(text)
    
    def _clean_text(self, text: str, max_length: int = 25) -> str:
        """Clean and truncate text for display"""
        if not text:
            return "غير متوفر"
        
        processed_text = self._process_arabic_text(text)
        if len(processed_text) > max_length:
            return processed_text[:max_length-3] + "..."
        return processed_text
    
    def _draw_rounded_rectangle(self, draw: ImageDraw.Draw, x1: int, y1: int, x2: int, y2: int, radius: int, fill: tuple):
        """Draw a rounded rectangle"""
        # Main rectangle
        draw.rectangle([x1 + radius, y1, x2 - radius, y2], fill=fill)
        draw.rectangle([x1, y1 + radius, x2, y2 - radius], fill=fill)
        
        # Corners
        draw.ellipse([x1, y1, x1 + radius*2, y1 + radius*2], fill=fill)
        draw.ellipse([x2 - radius*2, y1, x2, y1 + radius*2], fill=fill)
        draw.ellipse([x1, y2 - radius*2, x1 + radius*2, y2], fill=fill)
        draw.ellipse([x2 - radius*2, y2 - radius*2, x2, y2], fill=fill)
    
    def _draw_header(self, draw: ImageDraw.Draw, width: int):
        """Draw report header with Arabic title"""
        header_height = 100
        
        # Gradient background
        for i in range(header_height):
            ratio = i / header_height
            r = int(self.colors['bg_header'][0] * (1 - ratio) + 30 * ratio)
            g = int(self.colors['bg_header'][1] * (1 - ratio) + 60 * ratio)
            b = int(self.colors['bg_header'][2] * (1 - ratio) + 90 * ratio)
            draw.line([(0, i), (width, i)], fill=(r, g, b))
        
        # Arabic title
        title_ar = "تقرير شبكة اليمن نت"
        title_ar_processed = self._process_arabic_text(title_ar)
        
        title_bbox = draw.textbbox((0, 0), title_ar_processed, font=self.fonts['large'])
        title_width = title_bbox[2] - title_bbox[0]
        title_x = (width - title_width) // 2
        draw.text((title_x, 25), title_ar_processed, fill=self.colors['text_white'], font=self.fonts['large'])
        
        # English subtitle
        subtitle = "YemenNet Account Report"
        subtitle_bbox = draw.textbbox((0, 0), subtitle, font=self.fonts['bold'])
        subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
        subtitle_x = (width - subtitle_width) // 2
        draw.text((subtitle_x, 65), subtitle, fill=self.colors['text_white'], font=self.fonts['bold'])
        
        return header_height
    
    def _draw_user_info_section(self, draw: ImageDraw.Draw, start_y: int, width: int, account: AccountData):
        """Draw user information section"""
        current_y = start_y + 20
        
        # Section header
        section_title = self._process_arabic_text("معلومات الحساب")
        draw.text((width - 150, current_y), section_title, fill=self.colors['accent_blue'], font=self.fonts['header'])
        current_y += 35
        
        # User info in a box
        box_margin = 20
        box_height = 120
        self._draw_rounded_rectangle(draw, box_margin, current_y, width - box_margin, current_y + box_height, 15, (255, 255, 255))
        
        # Draw border
        draw.rounded_rectangle([box_margin, current_y, width - box_margin, current_y + box_height], 
                             radius=15, outline=self.colors['border'], width=2)
        
        info_y = current_y + 15
        
        # User details - arranged for Arabic (right to left)
        details = [
            ("اسم المستخدم", account.username),
            ("الحالة", f"{self._get_status_emoji(account)} {self._clean_text(account.status)}"),
            ("الباقة", self._clean_text(account.plan)),
            ("الرصيد", f"{self._get_balance_emoji(account)} {self._clean_text(account.available_balance)}"),
        ]
        
        for i, (label_ar, value) in enumerate(details):
            y_pos = info_y + (i * 25)
            
            # Arabic label (right aligned)
            label_processed = self._process_arabic_text(label_ar)
            label_bbox = draw.textbbox((0, 0), label_processed, font=self.fonts['bold'])
            label_width = label_bbox[2] - label_bbox[0]
            draw.text((width - box_margin - 20 - label_width, y_pos), label_processed, 
                     fill=self.colors['text_secondary'], font=self.fonts['bold'])
            
            # Value (left of label)
            value_processed = self._process_arabic_text(value)
            value_bbox = draw.textbbox((0, 0), value_processed, font=self.fonts['regular'])
            value_width = value_bbox[2] - value_bbox[0]
            draw.text((width - box_margin - 40 - label_width - value_width, y_pos), value_processed,
                     fill=self.colors['text_primary'], font=self.fonts['regular'])
        
        return current_y + box_height + 20
    
    def _draw_account_details_section(self, draw: ImageDraw.Draw, start_y: int, width: int, account: AccountData):
        """Draw account details section"""
        current_y = start_y
        
        # Section header
        section_title = self._process_arabic_text("تفاصيل الاشتراك")
        draw.text((width - 150, current_y), section_title, fill=self.colors['accent_blue'], font=self.fonts['header'])
        current_y += 35
        
        # Details box
        box_margin = 20
        box_height = 100
        self._draw_rounded_rectangle(draw, box_margin, current_y, width - box_margin, current_y + box_height, 15, (255, 255, 255))
        draw.rounded_rectangle([box_margin, current_y, width - box_margin, current_y + box_height], 
                             radius=15, outline=self.colors['border'], width=2)
        
        info_y = current_y + 15
        
        # Account details
        details = [
            ("تاريخ الاشتراك", self._clean_text(account.subscription_date)),
            ("تاريخ الانتهاء", self._clean_text(account.expiry_date)),
            ("نوع الحساب", self._clean_text(account.account_type)),
            ("الباقة", self._clean_text(account.package)),
        ]
        
        for i, (label_ar, value) in enumerate(details):
            y_pos = info_y + (i * 22)
            
            # Arabic label (right aligned)
            label_processed = self._process_arabic_text(label_ar)
            label_bbox = draw.textbbox((0, 0), label_processed, font=self.fonts['bold'])
            label_width = label_bbox[2] - label_bbox[0]
            draw.text((width - box_margin - 20 - label_width, y_pos), label_processed,
                     fill=self.colors['text_secondary'], font=self.fonts['bold'])
            
            # Value
            value_processed = self._process_arabic_text(value)
            value_bbox = draw.textbbox((0, 0), value_processed, font=self.fonts['regular'])
            value_width = value_bbox[2] - value_bbox[0]
            draw.text((width - box_margin - 40 - label_width - value_width, y_pos), value_processed,
                     fill=self.colors['text_primary'], font=self.fonts['regular'])
        
        return current_y + box_height + 20
    
    def _draw_status_card(self, draw: ImageDraw.Draw, start_y: int, width: int, account: AccountData):
        """Draw status card with visual indicators"""
        card_width = width - 40
        card_height = 80
        card_x = 20
        
        # Determine status color
        if account.is_active():
            status_color = self.colors['bg_success']
            status_text_ar = "نشط"
            status_text_en = "ACTIVE"
        else:
            status_color = self.colors['bg_danger']
            status_text_ar = "غير نشط"
            status_text_en = "INACTIVE"
        
        # Draw status card
        self._draw_rounded_rectangle(draw, card_x, start_y, card_x + card_width, start_y + card_height, 20, status_color)
        
        # Status text (Arabic)
        status_ar_processed = self._process_arabic_text(status_text_ar)
        status_ar_bbox = draw.textbbox((0, 0), status_ar_processed, font=self.fonts['header'])
        status_ar_width = status_ar_bbox[2] - status_ar_bbox[0]
        draw.text((card_x + card_width - 30 - status_ar_width, start_y + 20), status_ar_processed,
                 fill=self.colors['text_white'], font=self.fonts['header'])
        
        # Status text (English)
        status_en_bbox = draw.textbbox((0, 0), status_text_en, font=self.fonts['bold'])
        status_en_width = status_en_bbox[2] - status_en_bbox[0]
        draw.text((card_x + card_width - 30 - status_en_width, start_y + 50), status_text_en,
                 fill=self.colors['text_white'], font=self.fonts['bold'])
        
        # Status icon
        icon = "🟢" if account.is_active() else "🔴"
        draw.text((card_x + 30, start_y + 25), icon, fill=self.colors['text_white'], font=self.fonts['header'])
        
        return start_y + card_height + 20
    
    def _draw_footer(self, draw: ImageDraw.Draw, width: int, height: int, report: UserReport):
        """Draw report footer"""
        footer_y = height - 40
        
        # Data source
        source_ar = "بيانات مباشرة" if report.is_fresh else "بيانات مخزنة"
        source_processed = self._process_arabic_text(source_ar)
        source_emoji = "🆕" if report.is_fresh else "📦"
        
        draw.text((30, footer_y), f"{source_emoji} {source_processed}", 
                 fill=self.colors['text_secondary'], font=self.fonts['small'])
        
        # Timestamp
        timestamp = report.fetched_at.strftime("%Y-%m-%d %H:%M:%S")
        timestamp_bbox = draw.textbbox((0, 0), timestamp, font=self.fonts['small'])
        timestamp_width = timestamp_bbox[2] - timestamp_bbox[0]
        draw.text((width - timestamp_width - 30, footer_y), timestamp,
                 fill=self.colors['text_secondary'], font=self.fonts['small'])
        
        # Requested by (if available)
        if report.requested_by:
            requester_text = f"طلب بواسطة: {report.requested_by}"
            requester_bbox = draw.textbbox((0, 0), requester_text, font=self.fonts['small'])
            requester_width = requester_bbox[2] - requester_bbox[0]
            draw.text((width - requester_width - 30, footer_y - 20), requester_text,
                     fill=self.colors['text_secondary'], font=self.fonts['small'])
    
    def _get_status_emoji(self, account: AccountData) -> str:
        """Get status emoji"""
        if account.is_active():
            return "🟢"
        elif account.status and any(word in account.status for word in ["منتهي", "موقوف"]):
            return "🔴"
        else:
            return "🟡"
    
    def _get_balance_emoji(self, account: AccountData) -> str:
        """Get balance emoji"""
        if account.available_balance and any(char.isdigit() for char in account.available_balance):
            try:
                balance_num = float(account.available_balance.split()[0].replace(',', ''))
                if balance_num > 0:
                    return "💳"
            except:
                pass
        return "❌"
    
    def generate_user_report_image(self, report: UserReport, save_path: Optional[str] = None) -> str:
        """Generate a professional Arabic-supporting user report image"""
        if save_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_path = f"reports/user_report_{report.account.username}_{timestamp}.png"
        
        # Create reports directory
        os.makedirs("reports", exist_ok=True)
        
        # Image dimensions
        width, height = 800, 650
        image = Image.new("RGB", (width, height), self.colors['bg_primary'])
        draw = ImageDraw.Draw(image)
        
        # Draw components
        header_height = self._draw_header(draw, width)
        
        # Draw status card
        status_end_y = self._draw_status_card(draw, header_height + 20, width, report.account)
        
        # Draw user info section
        user_info_end_y = self._draw_user_info_section(draw, status_end_y, width, report.account)
        
        # Draw account details section
        account_details_end_y = self._draw_account_details_section(draw, user_info_end_y, width, report.account)
        
        # Draw footer
        self._draw_footer(draw, width, height, report)
        
        # Add outer border
        draw.rectangle([5, 5, width-6, height-6], outline=self.colors['border'], width=2)
        
        # Save image
        image.save(save_path, "PNG", optimize=True)
        return save_path


# Install required packages for Arabic support
# pip install arabic-reshaper python-bidi