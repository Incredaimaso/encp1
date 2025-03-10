import aiohttp
import json
from pymediainfo import MediaInfo
from typing import Dict, Optional
import telegraph
import os

class MediaInfoGenerator:
    def __init__(self):
        self.graph_api = "https://graph.org/api/upload"
        self.telegraph = telegraph.Telegraph()
        self.telegraph.create_account(short_name='AnimeEncoderBot')
        
    def get_media_info(self, file_path: str) -> Dict:
        media_info = MediaInfo.parse(file_path)
        
        info = {
            'general': {},
            'video': {},
            'audio': {},
            'quality': None
        }
        
        for track in media_info.tracks:
            if track.track_type == 'General':
                info['general'] = {
                    'format': track.format,
                    'size': f"{float(track.file_size)/(1024*1024):.2f} MB",
                    'duration': f"{float(track.duration)/1000:.2f} sec"
                }
            elif track.track_type == 'Video':
                # Detect quality from resolution
                height = track.height
                if height:
                    if height >= 2160:
                        info['quality'] = '4K'
                    elif height >= 1080:
                        info['quality'] = '1080p'
                    elif height >= 720:
                        info['quality'] = '720p'
                    else:
                        info['quality'] = '480p'
                
                info['video'] = {
                    'codec': track.codec,
                    'resolution': f"{track.width}x{track.height}",
                    'fps': track.frame_rate,
                    'bitrate': f"{float(track.bit_rate)/1000:.2f} Kbps" if track.bit_rate else 'N/A'
                }
            elif track.track_type == 'Audio':
                info['audio'] = {
                    'codec': track.codec,
                    'channels': track.channel_s,
                    'bitrate': f"{float(track.bit_rate)/1000:.2f} Kbps"
                }
        
        return info

    async def upload_to_graph(self, content: str) -> Optional[str]:
        try:
            async with aiohttp.ClientSession() as session:
                data = aiohttp.FormData()
                data.add_field('file', content, filename='mediainfo.txt')
                
                async with session.post(self.graph_api, data=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        return f"https://graph.org{result[0]['src']}"
        except Exception as e:
            print(f"Graph upload error: {e}")
        return None

    async def upload_to_telegraph(self, media_info: Dict, input_path: str, output_path: str) -> Optional[str]:
        try:
            html_content = f"""
            <h3>📊 Detailed Media Information</h3>
            <p><b>File:</b> {os.path.basename(output_path)}</p>
            
            <h4>General Info</h4>
            <p>Format: {media_info['general'].get('format', 'N/A')}<br>
            Duration: {media_info['general'].get('duration', 'N/A')}<br>
            Container: {media_info['general'].get('container', 'N/A')}</p>
            
            <h4>Video Stream</h4>
            <p>Codec: {media_info['video'].get('codec', 'N/A')}<br>
            Resolution: {media_info['video'].get('resolution', 'N/A')}<br>
            FPS: {media_info['video'].get('fps', 'N/A')}<br>
            Bitrate: {media_info['video'].get('bitrate', 'N/A')}</p>
            
            <h4>Audio Stream</h4>
            <p>Codec: {media_info['audio'].get('codec', 'N/A')}<br>
            Channels: {media_info['audio'].get('channels', 'N/A')}<br>
            Bitrate: {media_info['audio'].get('bitrate', 'N/A')}</p>
            
            <h4>Encoding Info</h4>
            <p>Original Size: {os.path.getsize(input_path)/(1024*1024):.2f} MB<br>
            Encoded Size: {os.path.getsize(output_path)/(1024*1024):.2f} MB<br>
            Compression: {(1 - os.path.getsize(output_path)/os.path.getsize(input_path))*100:.1f}%</p>
            """

            response = await self.telegraph.create_page(
                title=f"Media Info - {os.path.basename(output_path)}",
                html_content=html_content
            )
            return f"https://telegra.ph/{response['path']}"
        except Exception as e:
            print(f"Telegraph upload error: {e}")
            return None

    def format_info(self, info: Dict, original_size: float, new_size: float) -> str:
        template = (
            "📊 <b>Media Info</b>\n\n"
            "<b>General</b>\n"
            f"Format: {info['general'].get('format', 'N/A')}\n"
            f"Quality: {info.get('quality', 'N/A')}\n"
            f"Duration: {info['general'].get('duration', 'N/A')}\n\n"
            "<b>Video</b>\n"
            f"Codec: {info['video'].get('codec', 'N/A')}\n"
            f"Resolution: {info['video'].get('resolution', 'N/A')}\n"
            f"FPS: {info['video'].get('fps', 'N/A')}\n"
            f"Bitrate: {info['video'].get('bitrate', 'N/A')}\n\n"
            "<b>Size</b>\n"
            f"Before: {original_size:.2f} MB\n"
            f"After: {new_size:.2f} MB\n"
            f"Saved: {((original_size-new_size)/original_size)*100:.1f}%"
        )
        return template
