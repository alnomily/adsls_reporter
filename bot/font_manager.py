from PIL import ImageFont
import os
import requests
from pathlib import Path



class FontManager:
    def __init__(self):
        self.fonts_dir = Path("fonts")
        self.fonts_dir.mkdir(exist_ok=True)
        self.font_files = {
            'arabic': 'arial.ttf',  # We'll use Arial which supports Arabic
            'arabic_bold': 'arialbd.ttf',
        }
        self._ensure_fonts()
    
    def _ensure_fonts(self):
        """Ensure Arabic fonts are available"""
        # For Windows, use system fonts
        windows_fonts = {
            'arabic': "C:/Windows/Fonts/arial.ttf",
            'arabic_bold': "C:/Windows/Fonts/arialbd.ttf",
        }
        
        # For Linux, try to download or use fallbacks
        linux_fonts = {
            'arabic': "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            'arabic_bold': "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        }
        
        # Check if Windows fonts exist
        if os.path.exists(windows_fonts['arabic']):
            self.font_files = windows_fonts
        elif os.path.exists(linux_fonts['arabic']):
            self.font_files = linux_fonts
        else:
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