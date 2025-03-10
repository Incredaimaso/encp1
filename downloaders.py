import aria2p
from pyrogram import Client
from typing import Tuple
import os
import asyncio
import time
import re

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

    async def download_aria2(self, url: str, progress_callback, download_dir) -> Tuple[str, float]:
        try:
            if not self.aria2:
                if not self.setup_aria2():
                    raise Exception("Could not establish aria2 connection")

            print(f"Starting download in {download_dir}")
            download_dir = os.path.abspath(download_dir)
            
            # Clean any existing downloads with same hash
            if url.startswith('magnet:'):
                try:
                    downloads = self.aria2.get_downloads()
                    for download in downloads:
                        if download.is_active():
                            self.aria2.remove([download])
                            await asyncio.sleep(1)
                except Exception as e:
                    print(f"Error cleaning downloads: {e}")

            options = {
                'dir': download_dir,
                'max-connection-per-server': 16,
                'split': 16,
                'seed-time': 0,
                'bt-stop-timeout': 100,
                'follow-torrent': True,
                'bt-tracker-connect-timeout': 10,
                'bt-max-peers': 0,
                'max-download-limit': '0',
                'allow-overwrite': True,
                'auto-file-renaming': False
            }
            
            print(f"Adding download: {url[:100]}...")
            download = self.aria2.add_uris([url], options=options)
            gid = download.gid
            print(f"Download started with GID: {gid}")

            timeout = 600  # 10 minutes timeout
            start_time = time.time()
            last_size = 0
            stall_count = 0

            if url.startswith('magnet:'):
                await progress_callback(0, 100, "🔍 Getting metadata...")
                # Wait for metadata
                metadata_timeout = 60  # 1 minute timeout for metadata
                metadata_start = time.time()
                
                while True:
                    if time.time() - metadata_start > metadata_timeout:
                        raise Exception("Metadata fetch timeout")
                    
                    await asyncio.sleep(1)
                    download.update()
                    
                    if download.followed_by_ids:
                        print("Metadata received, starting main download...")
                        # Switch to the main download
                        download = self.aria2.get_download(download.followed_by_ids[0])
                        break
                    
                    if download.status == 'error':
                        raise Exception(f"Metadata download failed: {download.error_message}")
                    
                    await progress_callback(0, 100, 
                        f"🔍 Getting metadata...\n"
                        f"🌐 Connected to {download.connections} peers\n"
                        f"⏳ Time elapsed: {int(time.time() - metadata_start)}s"
                    )

            # Main download loop
            last_status_update = 0
            while True:
                await asyncio.sleep(0.5)
                try:
                    download.update()
                    current_time = time.time()
                    
                    # Update status every second
                    if current_time - last_status_update >= 1:
                        downloaded = download.completed_length
                        total = download.total_length
                        
                        if total > 0:
                            progress = (downloaded / total) * 100
                            speed = download.download_speed
                            eta = (total - downloaded) / speed if speed > 0 else 0
                            
                            status_text = (
                                f"⬇️ Downloading file\n"
                                f"📊 Progress: {progress:.1f}%\n"
                                f"📦 Size: {downloaded/(1024*1024):.1f}MB / {total/(1024*1024):.1f}MB\n"
                                f"🚀 Speed: {self._format_speed(speed)}\n"
                                f"⏱️ ETA: {self._format_eta(eta)}\n"
                                f"📡 Peers: {download.connections}"
                            )
                            await progress_callback(downloaded, total, status_text)
                            last_status_update = current_time

                    if download.status == 'complete':
                        print("Download marked as complete")
                        # Get file immediately
                        file_path = os.path.join(
                            download_dir,
                            os.path.basename(download.files[0].path)
                        )
                        
                        # Wait for file to be fully written
                        for i in range(30):  # 30 seconds timeout
                            if os.path.exists(file_path):
                                size = os.path.getsize(file_path)
                                if size > 0 and not os.path.exists(f"{file_path}.aria2"):
                                    print(f"Download completed: {file_path} ({size/(1024*1024):.1f}MB)")
                                    return file_path, size/(1024*1024)
                            await progress_callback(1, 1, f"⌛ Finalizing download... ({i+1}/30)")
                            await asyncio.sleep(1)
                        
                        raise Exception("File verification failed")

                except Exception as e:
                    print(f"Status check error: {str(e)}")
                    raise

        except Exception as e:
            print(f"Download error: {str(e)}")
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
