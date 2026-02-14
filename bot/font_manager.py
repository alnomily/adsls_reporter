from PIL import ImageFont
import os
import requests
from pathlib import Path



class FontManager:
    def __init__(self):
        self.fonts_dir = Path("fonts")
        self.fonts_dir.mkdir(exist_ok=True)
        # Preferred local filenames inside the project.
        # If you download fonts manually, place them at:
        #   fonts/arabic.ttf
        #   fonts/arabic_bold.ttf
        # Optional (for better English digits rendering):
        #   fonts/digits.ttf
        #   fonts/digits_bold.ttf
        self.font_files = {
            "arabic": str(self.fonts_dir / "arabic.ttf"),
            "arabic_bold": str(self.fonts_dir / "arabic_bold.ttf"),
            "digits": str(self.fonts_dir / "digits.ttf"),
            "digits_bold": str(self.fonts_dir / "digits_bold.ttf"),
        }
        self._ensure_fonts()
    
    def _ensure_fonts(self):
        """Ensure Arabic fonts are available"""
        # Prefer local fonts when present (works in Docker via volume mount).
        # Arabic and digits are treated independently.
        local_arabic_ok = all(os.path.exists(self.font_files[k]) for k in ("arabic", "arabic_bold"))
        local_digits_ok = all(os.path.exists(self.font_files[k]) for k in ("digits", "digits_bold"))
        if local_arabic_ok and local_digits_ok:
            return

        # For Windows, use system fonts
        windows_fonts = {
            'arabic': "C:/Windows/Fonts/arial.ttf",
            'arabic_bold': "C:/Windows/Fonts/arialbd.ttf",
            'digits': "C:/Windows/Fonts/arial.ttf",
            'digits_bold': "C:/Windows/Fonts/arialbd.ttf",
        }
        
        # For Linux/Docker, use packaged fonts (preferred) before trying downloads.
        linux_fonts_candidates = [
            {
                "arabic": "/usr/share/fonts/opentype/fonts-hosny-amiri/Amiri-Regular.ttf",
                "arabic_bold": "/usr/share/fonts/opentype/fonts-hosny-amiri/Amiri-Bold.ttf",
                "digits": "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "digits_bold": "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            },
            {
                "arabic": "/usr/share/fonts/truetype/amiri/Amiri-Regular.ttf",
                "arabic_bold": "/usr/share/fonts/truetype/amiri/Amiri-Bold.ttf",
                "digits": "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "digits_bold": "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            },
            {
                "arabic": "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "arabic_bold": "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "digits": "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "digits_bold": "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            },
            {
                "arabic": "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
                "arabic_bold": "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
                "digits": "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "digits_bold": "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            },
        ]
        
        # Check if Windows fonts exist (Arabic/digits)
        if os.path.exists(windows_fonts['arabic']):
            # Only fill missing local files; do not override user-provided fonts/
            for k, v in windows_fonts.items():
                if not self.font_files.get(k) or not os.path.exists(self.font_files[k]):
                    self.font_files[k] = v
            return

        picked = None
        for candidate in linux_fonts_candidates:
            if os.path.exists(candidate.get("arabic", "")):
                picked = candidate
                break
        if picked:
            for k, v in picked.items():
                if not self.font_files.get(k) or not os.path.exists(self.font_files[k]):
                    self.font_files[k] = v
            # Continue (do not return) because we may still decide about downloads for Arabic

        # In Docker, prefer not to do network downloads (ISPs can block GitHub).
        in_docker = os.path.exists("/.dockerenv")
        downloads_disabled = os.getenv("DISABLE_FONT_DOWNLOAD", "").strip().lower() in {"1", "true", "yes"}
        if in_docker or downloads_disabled:
            # Never disable digits fonts; they are expected to come from OS packages.
            if not os.path.exists(str(self.font_files.get("arabic") or "")):
                self.font_files['arabic'] = None
            if not os.path.exists(str(self.font_files.get("arabic_bold") or "")):
                self.font_files['arabic_bold'] = None
            return

        # Try to download Arabic fonts
        self._download_arabic_fonts()
    
    def _download_arabic_fonts(self):
        """Download Arabic-supporting fonts"""
        font_urls = {
            'arabic': 'https://github.com/rastikerdar/vazir-font/raw/master/dist/Vazir-Regular.ttf',
            'arabic_bold': 'https://github.com/rastikerdar/vazir-font/raw/master/dist/Vazir-Bold.ttf',
        }
        
        for font_type, url in font_urls.items():
            font_path = self.fonts_dir / f"{font_type}.ttf"
            if not font_path.exists():
                try:
                    print(f"Downloading {font_type} font...")
                    response = requests.get(url, timeout=30)
                    if response.status_code == 200:
                        with open(font_path, 'wb') as f:
                            f.write(response.content)
                        self.font_files[font_type] = str(font_path)
                    else:
                        print(f"Failed to download {font_type} font")
                except Exception as e:
                    print(f"Error downloading font: {e}")
        
        # If downloads failed, use fallback
        if not any(os.path.exists(f) for f in self.font_files.values()):
            self.font_files = {
                'arabic': None,
                'arabic_bold': None,
            }
    
    def get_font(self, font_type: str, size: int):
        """Get font with specified size"""
        font_path = self.font_files.get(font_type)
        try:
            if font_path and os.path.exists(font_path):
                return ImageFont.truetype(font_path, size)
            else:
                return ImageFont.load_default()
        except:
            return ImageFont.load_default()


# Global font manager instance
font_manager = FontManager()