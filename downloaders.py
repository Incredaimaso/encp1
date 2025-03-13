import aria2p
from pyrogram import Client
from typing import Tuple
import os
import asyncio
import time
import re
import backoff  # Add to requirements.txt
import socket
import websockets
import urllib3

class Downloader:
    def __init__(self, aria2_host: str, aria2_port: int, aria2_secret: str):
        self.aria2_host = aria2_host
        self.aria2_port = aria2_port
        self.aria2_secret = aria2_secret
        self.aria2 = None
        self.completion_check_interval = 0.5  # Check every 0.5 seconds
        self.stall_timeout = 10  # Consider stalled after 10 seconds
        self.completion_timeout = 30  # Max wait for completion verification
        self.download_check_interval = 1  # Check every second
        self.min_download_wait = 5  # Minimum wait for download to start

    def setup_aria2(self):
        try:
            host = self.aria2_host if self.aria2_host.startswith('http') else f'http://{self.aria2_host}'
            self.aria2 = aria2p.API(
                aria2p.Client(
                    host=host,
                    port=self.aria2_port,
                    secret=self.aria2_secret
                )
            )
            version = self.aria2.client.call('aria2.getVersion')
            print(f"Connected to aria2 version: {version['version']}")
            return True
        except Exception as e:
            print(f"Failed to connect to aria2: {e}")
            return False

    async def download_aria2(self, url: str, progress_callback, download_dir) -> Tuple[str, float]:
        if not self.aria2:
            if not self.setup_aria2():
                raise Exception("Could not connect to aria2")

        try:
            # Basic options
            options = {
                'dir': os.path.abspath(download_dir),
                'continue': 'true',
                'max-connection-per-server': '16',
                'split': '16'
            }

            # Add download
            if url.startswith('magnet:'):
                download = self.aria2.add_magnet(url, options=options)
            else:
                download = self.aria2.add_uris([url], options=options)

            # Monitor progress with improved download detection
            last_progress_time = time.time()
            last_size = 0
            download_started = False
            download_complete = False

            while True:
                try:
                    download.update()
                    
                    if download.followed_by:
                        download = download.followed_by[0]
                        continue

                    total = download.total_length
                    completed = download.completed_length

                    # Wait for download to actually start
                    if not download_started and completed > 0:
                        download_started = True
                        print("Download started")
                        await asyncio.sleep(self.min_download_wait)

                    if download_started:
                        # Check completion only after download has started
                        if download.is_complete or (total > 0 and completed == total):
                            # Add extra wait time for filesystem
                            await asyncio.sleep(2)
                            print("Download appears complete, verifying...")
                            
                            file_path = os.path.join(download_dir, download.files[0].path)
                            if os.path.exists(file_path):
                                current_size = os.path.getsize(file_path)
                                if current_size > 0:
                                    print(f"File verified: {file_path} ({current_size/(1024*1024):.2f}MB)")
                                    return file_path, current_size/(1024*1024)
                            
                            print("File verification failed, continuing download...")

                        # Update progress
                        current_time = time.time()
                        if current_time - last_progress_time >= 1:
                            if total > 0:
                                speed = download.download_speed
                                eta = (total - completed) / speed if speed > 0 else 0
                                await progress_callback(
                                    completed, 
                                    total,
                                    f"⬇️ Downloading: {download.name}\n"
                                    f"📊 Progress: {(completed/total)*100:.1f}%\n"
                                    f"⚡ Speed: {self._format_speed(speed)}\n"
                                    f"⏱️ ETA: {self._format_eta(eta)}"
                                )
                            last_progress_time = current_time
                            last_size = completed

                except Exception as e:
                    print(f"Progress update error: {e}")
                
                await asyncio.sleep(self.download_check_interval)

        except Exception as e:
            print(f"❌ Download error: {e}")
            raise

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

    async def _reconnect_aria2(self):
        for i in range(self.max_retries):
            if self.setup_aria2():
                return True
            print(f"Aria2 reconnection attempt {i+1}/{self.max_retries}")
            await asyncio.sleep(self.retry_delay)
        self.aria2 = None
        return await self._ensure_connection()

    async def _ensure_connection(self):
        for attempt in range(self.max_retries):
            try:
                if not self.aria2:
                    self.setup_aria2()
                # Test connection
                version = self.aria2.client.call('aria2.getVersion')
                return True
            except Exception as e:
                print(f"Connection attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(self.retry_delay)
                continue
        return False

    async def _ensure_active_session(self):
        for i in range(self.max_session_retries):
            try:
                if not self.aria2:
                    await self._reconnect_aria2()
                await asyncio.wait_for(
                    self.aria2.client.call('aria2.getVersion'),
                    timeout=self.session_timeout
                )
                return True
            except Exception as e:
                print(f"Session check failed (attempt {i+1}): {e}")
                await asyncio.sleep(self.retry_delay)
        return False

    async def _monitor_download(self, download, progress_callback):
        while not download.is_complete and not download.has_failed:
            try:
                download.update()
                total = download.total_length
                completed = download.completed_length
                
                if total > 0:
                    await progress_callback(completed, total,
                        f"⬇️ Downloading: {download.name}\n"
                        f"📊 Progress: {(completed/total)*100:.1f}%\n"
                        f"⚡ Speed: {self._format_speed(download.download_speed)}\n"
                        f"🌐 Peers: {download.connections}"
                    )
            except Exception as e:
                print(f"Download monitor error: {e}")
            await asyncio.sleep(1)

    async def _safe_rpc_call(self, method, *args):
        for retry in range(self.max_retries):
            try:
                if not self.aria2:
                    if not await self._reconnect_aria2():
                        raise Exception("Failed to reconnect to aria2")

                response = await asyncio.wait_for(
                    self.aria2.client.call(method, *args),
                    timeout=self.rpc_timeout
                )
                return response
            except (ConnectionError, socket.timeout, asyncio.TimeoutError) as e:
                print(f"RPC call failed (attempt {retry + 1}): {e}")
                await asyncio.sleep(self.retry_delay)
                if retry == self.max_retries - 1:
                    raise
                continue

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
