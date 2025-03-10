import re
import os
from typing import Dict, Optional

class VideoRenamer:
    def __init__(self):
        self.name_cache = {}
        self.ignored_terms = ['x264', 'x265', 'HEVC', 'WEB-DL', 'BluRay', 'CRX']
        self.quality_patterns = {
            '4K': r'2160p|4K|UHD',
            '1080p': r'1080p|1920x1080|FHD|1080',
            '720p': r'720p|1280x720|HD|720',
            '480p': r'480p|854x480|SD|480'
        }

    def _detect_quality(self, filename: str) -> str:
        filename = filename.upper()
        for quality, pattern in self.quality_patterns.items():
            if re.search(pattern, filename, re.IGNORECASE):
                return quality
        return '480p'  # Default quality
    
    def parse_name(self, filename: str) -> Dict[str, str]:
        try:
            # Pattern for [SXX-EXX] or episode number
            season_ep_pattern = r'\[?S?(\d{1,2})?-?E?(\d{1,3})\]?'
            # Pattern for audio and subs info
            audio_pattern = r'\[(Dual Audio|Multi Audio)\]'
            sub_pattern = r'\[(Eng Sub|Multi Sub|MultiSub)\]'
            
            # Clean the filename first
            clean_name = self._clean_filename(filename)
            
            # Extract components
            season_ep = re.search(season_ep_pattern, clean_name)
            season = season_ep.group(1) if season_ep and season_ep.group(1) else None
            episode = season_ep.group(2) if season_ep else None
            
            # Get title (text before season/episode)
            title = clean_name.split('[')[0].strip()
            if season_ep:
                title = re.split(season_ep_pattern, clean_name)[0].strip()
            
            # Get audio and sub info
            has_dual_audio = bool(re.search(audio_pattern, filename, re.IGNORECASE))
            has_eng_sub = bool(re.search(sub_pattern, filename, re.IGNORECASE))
            
            # Add quality detection
            quality = self._detect_quality(filename)
            
            return {
                'title': title,
                'season': season,
                'episode': episode,
                'dual_audio': has_dual_audio,
                'eng_sub': has_eng_sub,
                'quality': quality
            }
        except Exception:
            return self._fallback_parse(filename)

    def _clean_filename(self, filename: str) -> str:
        # Remove quality terms and technical info
        clean = filename
        for term in self.ignored_terms:
            clean = re.sub(rf'\[?{term}\]?', '', clean, flags=re.IGNORECASE)
        
        # Remove extensions
        clean = os.path.splitext(clean)[0]
        return clean.strip()

    def _fallback_parse(self, filename: str) -> Dict[str, str]:
        # Basic episode number extraction
        ep_match = re.search(r'E?(\d{1,3})', filename)
        clean_name = self._clean_filename(filename)
        
        return {
            'title': clean_name.split('E')[0].strip(),
            'season': '1',
            'episode': ep_match.group(1) if ep_match else None,
            'dual_audio': False,
            'eng_sub': False
        }

    def generate_filename(self, original_name: str, quality: str) -> str:
        parsed = self.parse_name(original_name)
        
        # Build the filename components
        season_ep = f"[S{int(parsed['season']):02d}-E{int(parsed['episode']):02d}]" if parsed['season'] else f"[E{int(parsed['episode']):02d}]"
        audio = "[Dual Audio]" if parsed['dual_audio'] else ""
        subs = "[Eng Sub]" if parsed['eng_sub'] else ""
        
        # Combine components
        return f"{season_ep} {parsed['title']} {audio} {subs} [{quality}].mkv".replace('  ', ' ')
