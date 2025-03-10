import aria2p
from pyrogram import Client
from typing import Tuple
import os
import asyncio
import time
import re
import backoff  # Add to requirements.txt

class Downloader:
    def __init__(self, aria2_host: str, aria2_port: int, aria2_secret: str):
        self.aria2_host = aria2_host
        self.aria2_port = aria2_port
        self.aria2_secret = aria2_secret
        self.max_retries = 3
        self.retry_delay = 5
        self.aria2 = None
        self.SUPPORTED_FORMATS = ['.mkv', '.mp4', '.avi', '.webm']
        self.setup_aria2()
        self.max_connection_retries = 5
        self.connection_retry_delay = 5
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 3
    
    def setup_aria2(self):
        try:
            # Fix the host URL format
            host = self.aria2_host if self.aria2_host.startswith('http') else f'http://{self.aria2_host}'
            self.aria2 = aria2p.API(
                aria2p.Client(
                    host=host,
                    port=self.aria2_port,
                    secret=self.aria2_secret
                )
            )
            
            # Test connection with version check
            version = self.aria2.client.call('aria2.getVersion')
            print(f"Connected to aria2 version: {version['version']}")
            return True
            
        except Exception as e:
            print(f"Failed to connect to aria2: {e}")
            self.aria2 = None
            return False

    async def download_telegram_file(self, client: Client, message, progress_callback, download_dir):
        try:
            return await message.download(
                file_name=os.path.join(download_dir, message.document.file_name if message.document else "video.mp4"),
                progress=lambda current, total: asyncio.ensure_future(
                    progress_callback(current, total, "Downloading")
                )
            )
        except Exception as e:
            raise Exception(f"Telegram download failed: {str(e)}")

    def _sanitize_filename(self, filename: str) -> str:
        # Remove invalid characters and sanitize filename
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        return filename.strip()

    def _get_file_extension(self, filename: str, content_type: str = None) -> str:
        # Try to get extension from filename first
        ext = os.path.splitext(filename)[1].lower()
        if ext in ['.mkv', '.mp4', '.avi', '.webm']:
            return ext

        # Fallback to content type mapping
        content_map = {
            'video/x-matroska': '.mkv',
            'video/mp4': '.mp4',
            'video/x-msvideo': '.avi',
            'video/webm': '.webm'
        }
        return content_map.get(content_type, '.mkv')  # Default to .mkv

    async def _verify_download(self, file_path: str, max_attempts: int = 30) -> bool:
        """Enhanced verification with multiple attempts and detailed checks"""
        
        # Remove [METADATA] from file path if present
        clean_path = file_path.replace("[METADATA]", "")
        if clean_path != file_path:
            print(f"Cleaned file path: {clean_path}")
            file_path = clean_path

        for attempt in range(max_attempts):
            try:
                if not os.path.exists(file_path):
                    alt_path = os.path.join(os.path.dirname(file_path), 
                                          os.path.basename(file_path))
                    print(f"Trying alternative path: {alt_path}")
                    if os.path.exists(alt_path):
                        file_path = alt_path
                    else:
                        print(f"Attempt {attempt+1}: File not found")
                        await asyncio.sleep(1)
                        continue

                # Check if file is still being written
                initial_size = os.path.getsize(file_path)
                await asyncio.sleep(2)
                current_size = os.path.getsize(file_path)
                
                if current_size > 0 and initial_size == current_size:
                    # Check if .aria2 file is gone
                    if not os.path.exists(f"{file_path}.aria2"):
                        print(f"Found valid file: {file_path} ({current_size/(1024*1024):.1f}MB)")
                        return True
                    else:
                        print("Waiting for .aria2 file to be removed...")
                
                await asyncio.sleep(1)

            except Exception as e:
                print(f"Verification error: {e}")
                await asyncio.sleep(1)

        return False

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, ConnectionResetError),
        max_tries=5,
        giveup=lambda e: "not found" in str(e).lower()
    )
    async def _safe_download_call(self, func, *args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except (ConnectionError, ConnectionResetError) as e:
            print(f"Connection error: {e}, attempting to reconnect...")
            if self.setup_aria2():
                return await func(*args, **kwargs)
            raise

    async def download_aria2(self, url: str, progress_callback, download_dir) -> Tuple[str, float]:
        for attempt in range(self.max_reconnect_attempts):
            try:
                if not self.aria2:
                    if not self.setup_aria2():
                        raise Exception("Could not establish aria2 connection")

                # Clean existing downloads
                try:
                    downloads = self.aria2.get_downloads()
                    for download in downloads:
                        if download.is_active():
                            self.aria2.remove([download])
                            await asyncio.sleep(1)
                except Exception as e:
                    print(f"Error cleaning downloads: {e}")

                options = {
                    'dir': os.path.abspath(download_dir),
                    'max-connection-per-server': 16,
                    'split': 16,
                    'seed-time': 0,
                    'bt-stop-timeout': 100,
                    'follow-torrent': True,
                    'bt-tracker-connect-timeout': 10,
                    'bt-max-peers': 0,
                    'max-download-limit': '0',
                    'allow-overwrite': True,
                    'auto-file-renaming': False,
                    'continue': True,
                    'max-tries': 5,
                    'retry-wait': 5,
                    'connect-timeout': 30,
                    'timeout': 30,
                    'piece-length': '1M'
                }

                if url.startswith('magnet:'):
                    # Special handling for magnet links
                    await progress_callback(0, 100, "🧲 Initializing magnet download...")
                    
                    download = await self._safe_download_call(
                        self.aria2.add_magnet, url, options=options
                    )
                    
                    # Wait for metadata
                    metadata_timeout = 60
                    metadata_start = time.time()
                    while time.time() - metadata_start < metadata_timeout:
                        try:
                            await self._safe_download_call(download.update)
                            if download.has_failed:
                                raise Exception("Magnet download failed")
                            if download.followed_by_ids:
                                download = self.aria2.get_download(download.followed_by_ids[0])
                                print("Metadata received, starting download...")
                                break
                            await progress_callback(0, 100, 
                                f"🔍 Getting metadata...\n"
                                f"🌐 Connected peers: {download.connections}\n"
                                f"⏳ Timeout in: {int(metadata_timeout-(time.time()-metadata_start))}s"
                            )
                        except Exception as e:
                            print(f"Metadata update error: {e}")
                        await asyncio.sleep(1)
                else:
                    download = await self._safe_download_call(
                        self.aria2.add_uris, [url], options=options
                    )

                # Monitor download
                last_update = 0
                while True:
                    try:
                        await self._safe_download_call(download.update)
                        
                        if download.has_failed:
                            raise Exception(f"Download failed: {download.error_message}")
                            
                        if download.is_complete:
                            file_path = os.path.join(
                                download_dir,
                                os.path.basename(download.files[0].path)
                            )
                            if await self._verify_file(file_path):
                                return file_path, os.path.getsize(file_path)/(1024*1024)
                            
                        # Progress update
                        if time.time() - last_update >= 1:
                            total = download.total_length
                            completed = download.completed_length
                            if total > 0:
                                await progress_callback(completed, total,
                                    f"⬇️ Downloading: {download.name}\n"
                                    f"📊 Progress: {(completed/total)*100:.1f}%\n"
                                    f"⚡ Speed: {self._format_speed(download.download_speed)}\n"
                                    f"🌐 Peers: {download.connections}"
                                )
                            last_update = time.time()
                            
                    except (ConnectionError, ConnectionResetError) as e:
                        print(f"Connection error: {e}, retrying...")
                        await asyncio.sleep(self.reconnect_delay)
                        continue
                        
                    await asyncio.sleep(1)

            except Exception as e:
                print(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < self.max_reconnect_attempts - 1:
                    await asyncio.sleep(self.reconnect_delay)
                    continue
                raise

    def _format_speed(self, speed: int) -> str:
        units = ['B/s', 'KB/s', 'MB/s', 'GB/s']
        unit_index = 0
        speed_float = float(speed)
        
        while speed_float > 1024 and unit_index < len(units) - 1:
            speed_float /= 1024
            unit_index += 1
            
        return f"{speed_float:.2f} {units[unit_index]}"

    def _format_eta(self, seconds: float) -> str:
        if seconds < 0:
            return "∞"
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"
