import aria2p
from pyrogram import Client
from typing import Tuple, Optional, Dict, Any, Callable, Union, Awaitable
import os
import asyncio
import time
import re
import logging
import socket
import urllib.parse
import contextlib
from enum import Enum
from dataclasses import dataclass
from pathlib import Path


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Downloader")


class DownloadError(Exception):
    """Base exception for download-related errors"""
    pass


class ConnectionError(DownloadError):
    """Error when connecting to download service"""
    pass


class DownloadInitError(DownloadError):
    """Error when initializing a download"""
    pass


class DownloadProgressError(DownloadError):
    """Error when monitoring download progress"""
    pass


class DownloadVerificationError(DownloadError):
    """Error when verifying downloaded file"""
    pass


class DownloadStatus(Enum):
    """Enum representing download status"""
    PENDING = "pending"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETE = "complete"
    ERROR = "error"
    CANCELED = "canceled"


@dataclass
class DownloadProgress:
    """Data class for download progress information"""
    completed: int
    total: int
    speed: int
    status: DownloadStatus
    filename: str
    eta: float
    connections: int = 0
    message: str = ""

    @property
    def percentage(self) -> float:
        """Calculate percentage of completion"""
        if self.total <= 0:
            return 0
        return (self.completed / self.total) * 100


# Type for progress callback functions
ProgressCallback = Callable[[int, int, str], Awaitable[None]]


class Downloader:
    """Improved aria2-based file downloader with robust error handling and retry mechanisms"""

    def __init__(
        self, 
        aria2_host: str, 
        aria2_port: int, 
        aria2_secret: str,
        download_dir: str = "./downloads",
        max_retries: int = 3,
        retry_delay: int = 5,
        connection_timeout: int = 10,
        download_timeout: int = 3600,  # 1 hour default timeout
        progress_interval: float = 1.0,
        verification_timeout: int = 30,
        verify_wait: int = 2,
        stall_timeout: int = 15
    ):
        """
        Initialize the downloader with configurable parameters
        
        Args:
            aria2_host: Hostname of aria2 RPC server
            aria2_port: Port of aria2 RPC server
            aria2_secret: Secret token for aria2 RPC authentication
            download_dir: Directory to save downloaded files
            max_retries: Maximum retry attempts for failed operations
            retry_delay: Delay between retry attempts in seconds
            connection_timeout: Timeout for connection attempts in seconds
            download_timeout: Maximum time allowed for a download in seconds
            progress_interval: Interval for progress updates in seconds
            verification_timeout: Maximum time to verify download completion
            verify_wait: Wait time for file system operations during verification
            stall_timeout: Time after which a non-progressing download is considered stalled
        """
        # Connection parameters
        self.aria2_host = aria2_host
        self.aria2_port = aria2_port
        self.aria2_secret = aria2_secret
        
        # Instance configuration
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Timing parameters
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection_timeout = connection_timeout
        self.download_timeout = download_timeout
        self.progress_interval = progress_interval
        self.verification_timeout = verification_timeout
        self.verify_wait = verify_wait
        self.stall_timeout = stall_timeout
        
        # Initialize connection state
        self.aria2: Optional[aria2p.API] = None
        self.active_downloads: Dict[str, Any] = {}
        
        logger.info(f"Downloader initialized with host={aria2_host}, port={aria2_port}")

    async def connect(self) -> bool:
        """
        Establish connection to aria2 RPC server with proper error handling
        
        Returns:
            bool: True if connection successful, False otherwise
        
        Raises:
            ConnectionError: If connection fails after max retries
        """
        for attempt in range(self.max_retries):
            try:
                # Ensure host format is correct
                host = self.aria2_host
                if not host.startswith(('http://', 'https://')):
                    host = f'http://{host}'
                
                logger.info(f"Connecting to aria2 at {host}:{self.aria2_port} (attempt {attempt+1}/{self.max_retries})")
                
                # Create API client with timeout
                client = aria2p.Client(
                    host=host,
                    port=self.aria2_port,
                    secret=self.aria2_secret,
                    timeout=self.connection_timeout
                )
                
                # Test connection with version check
                version_info = await asyncio.to_thread(client.call, 'aria2.getVersion')
                
                self.aria2 = aria2p.API(client)
                logger.info(f"Connected to aria2 version: {version_info['version']}")
                return True
                
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.critical(f"Failed to connect to aria2 after {self.max_retries} attempts")
                    raise ConnectionError(f"Failed to connect to aria2: {str(e)}") from e
        
        return False  # Should never reach here due to exception above

    async def ensure_connected(self) -> None:
        """Ensure connection to aria2 is established"""
        if self.aria2 is None:
            await self.connect()

    async def download(
        self, 
        url: str, 
        progress_callback: Optional[ProgressCallback] = None,
        custom_dir: Optional[str] = None,
        filename_override: Optional[str] = None
    ) -> Tuple[str, float]:
        """
        Download a file using aria2
        
        Args:
            url: URL or magnet link to download
            progress_callback: Optional callback for progress updates
            custom_dir: Optional custom download directory
            filename_override: Optional filename override
            
        Returns:
            Tuple[str, float]: (file path, file size in MB)
            
        Raises:
            DownloadError: If download fails after all retries
        """
        download_dir = Path(custom_dir) if custom_dir else self.download_dir
        download_dir.mkdir(parents=True, exist_ok=True)
        
        for retry in range(self.max_retries):
            try:
                # Ensure connection is established
                await self.ensure_connected()
                
                # Generate options with optimal performance settings
                options = self._get_download_options(download_dir, filename_override)
                
                # Add download with proper validation
                download = await self._add_download(url, options)
                gid = download.gid
                self.active_downloads[gid] = download
                
                logger.info(f"Download started: {url} (GID: {gid})")
                
                # Setup monitoring with timeout enforcement
                download_task = asyncio.create_task(
                    self._monitor_download(download, progress_callback)
                )
                
                try:
                    # Wait for download with timeout
                    file_path, file_size = await asyncio.wait_for(
                        download_task,
                        timeout=self.download_timeout
                    )
                    
                    # Verify the download completion
                    is_verified = await self._verify_download(file_path)
                    if not is_verified:
                        raise DownloadVerificationError(f"Downloaded file verification failed: {file_path}")
                    
                    logger.info(f"Download completed and verified: {file_path} ({file_size:.2f}MB)")
                    return file_path, file_size
                    
                except asyncio.TimeoutError:
                    # Handle download timeout
                    if gid in self.active_downloads:
                        await asyncio.to_thread(
                            self.aria2.client.call, 
                            'aria2.remove', 
                            gid
                        )
                        del self.active_downloads[gid]
                    
                    raise DownloadError(f"Download timed out after {self.download_timeout} seconds")
            
            except DownloadVerificationError as e:
                # This is a verification issue, might be worth retrying
                logger.warning(f"Download verification failed (attempt {retry+1}): {str(e)}")
                
            except DownloadError as e:
                # Handle specific download errors
                logger.error(f"Download error (attempt {retry+1}): {str(e)}")
                
            except Exception as e:
                # Handle unexpected errors
                logger.exception(f"Unexpected error during download (attempt {retry+1}): {str(e)}")
            
            # Only sleep between retries if not the last attempt
            if retry < self.max_retries - 1:
                await asyncio.sleep(self.retry_delay)
        
        # If we get here, all retries failed
        raise DownloadError(f"Download failed after {self.max_retries} attempts")

    async def download_telegram_file(
        self, 
        client: Client, 
        message: Any, 
        progress_callback: Optional[ProgressCallback] = None,
        custom_dir: Optional[str] = None
    ) -> str:
        """
        Download a file from Telegram
        
        Args:
            client: Pyrogram client instance
            message: Telegram message containing file
            progress_callback: Optional callback for progress updates
            custom_dir: Optional custom download directory
            
        Returns:
            str: Path to downloaded file
            
        Raises:
            DownloadError: If download fails
        """
        download_dir = Path(custom_dir) if custom_dir else self.download_dir
        download_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Determine filename
            if hasattr(message, 'document') and message.document:
                filename = self._sanitize_filename(message.document.file_name)
            else:
                filename = f"telegram_video_{int(time.time())}.mp4"
            
            file_path = download_dir / filename
            
            # Create wrapper for progress callback
            async def progress_wrapper(current: int, total: int):
                if progress_callback:
                    progress_text = (
                        f"⬇️ Downloading: {filename}\n"
                        f"📊 Progress: {(current/total)*100:.1f}%"
                    )
                    await progress_callback(current, total, progress_text)
            
            # Download file with retries
            for attempt in range(self.max_retries):
                try:
                    result = await message.download(
                        file_name=str(file_path),
                        progress=progress_wrapper
                    )
                    
                    if result and os.path.exists(result):
                        logger.info(f"Telegram download complete: {result}")
                        return result
                    
                    raise DownloadError("Download completed but file not found")
                    
                except Exception as e:
                    logger.error(f"Telegram download attempt {attempt+1} failed: {str(e)}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.retry_delay)
                    else:
                        raise DownloadError(f"Telegram download failed: {str(e)}") from e
            
        except Exception as e:
            logger.exception(f"Failed to download Telegram file: {str(e)}")
            raise DownloadError(f"Telegram download error: {str(e)}") from e

    def _get_download_options(self, download_dir: Path, filename_override: Optional[str] = None) -> Dict[str, str]:
        """
        Get optimized aria2 download options
        
        Args:
            download_dir: Directory for download
            filename_override: Optional custom filename
            
        Returns:
            Dict[str, str]: Aria2 download options
        """
        options = {
            'dir': str(download_dir.absolute()),
            'continue': 'true',
            'max-connection-per-server': '16',
            'split': '16',
            'min-split-size': '10M',
            'max-concurrent-downloads': '10',
            'max-tries': '5',
            'retry-wait': '5',
            'connect-timeout': '10',
            'timeout': '10',
            'seed-time': '0',
            'max-upload-limit': '1K',  # Limit upload to prevent stalling
            'auto-file-renaming': 'true',
            'allow-overwrite': 'true',
            'file-allocation': 'none',  # Faster startup
            'disk-cache': '64M'  # Use memory cache for better performance
        }
        
        # Add filename if provided
        if filename_override:
            options['out'] = self._sanitize_filename(filename_override)
            
        return options

    async def _add_download(self, url: str, options: Dict[str, str]) -> Any:
        """
        Add download to aria2 with proper error handling
        
        Args:
            url: URL or magnet link to download
            options: Download options
            
        Returns:
            aria2p.Download: Download object
            
        Raises:
            DownloadInitError: If download fails to initialize
        """
        try:
            if not self.aria2:
                raise DownloadInitError("Aria2 client not initialized")
            
            download = None
            if url.startswith('magnet:'):
                logger.info(f"Adding magnet download: {url[:60]}...")
                download = await asyncio.to_thread(
                    self.aria2.add_magnet,
                    url, 
                    options=options
                )
            else:
                logger.info(f"Adding direct download: {url[:60]}...")
                download = await asyncio.to_thread(
                    self.aria2.add_uris,
                    [url], 
                    options=options
                )
            
            if not download:
                raise DownloadInitError("Failed to create download (null response)")
                
            return download
            
        except Exception as e:
            logger.exception(f"Failed to add download: {str(e)}")
            raise DownloadInitError(f"Failed to add download: {str(e)}") from e

    async def _monitor_download(
        self, 
        download: Any, 
        progress_callback: Optional[ProgressCallback]
    ) -> Tuple[str, float]:
        """
        Monitor download progress with stall detection and error handling
        
        Args:
            download: Aria2 download object
            progress_callback: Optional callback for progress updates
            
        Returns:
            Tuple[str, float]: (file path, file size in MB)
            
        Raises:
            DownloadProgressError: If download monitoring fails
            DownloadError: If download fails or stalls
        """
        last_progress_time = time.time()
        last_completed = 0
        last_progress_update = 0
        download_started = False
        stall_start_time = None
        
        try:
            while True:
                # Update download info
                try:
                    await asyncio.to_thread(download.update)
                except Exception as e:
                    logger.error(f"Failed to update download status: {str(e)}")
                    # Try to reconnect if API call fails
                    if await self._try_reconnect():
                        continue
                    else:
                        raise DownloadProgressError(f"Failed to update download status: {str(e)}")
                
                # Handle download follow-up (usually for magnets)
                if download.followed_by:
                    logger.info(f"Download followed to new download: {download.followed_by[0].gid}")
                    download = download.followed_by[0]
                    self.active_downloads[download.gid] = download
                    continue
                
                # Get current metrics
                total = download.total_length
                completed = download.completed_length
                speed = download.download_speed
                
                # Check for download start
                if not download_started and completed > 0:
                    download_started = True
                    logger.info(f"Download actually started: {download.name}")
                
                # Check for completion
                if download.is_complete or (total > 0 and completed == total):
                    logger.info(f"Download appears complete: {download.name}")
                    
                    try:
                        # In case of metadata downloads that generate .torrent files
                        if download.files and len(download.files) > 0:
                            file_path = download.files[0].path
                            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert to MB
                            return file_path, file_size
                        else:
                            raise DownloadError("Download completed but no files found")
                    except Exception as e:
                        logger.error(f"Error getting file info: {str(e)}")
                        raise DownloadProgressError(f"Error getting file info: {str(e)}")
                
                # Send progress updates at specified interval
                current_time = time.time()
                if progress_callback and (current_time - last_progress_update >= self.progress_interval):
                    if total > 0:
                        eta = (total - completed) / max(speed, 1)  # Avoid division by zero
                        
                        progress = DownloadProgress(
                            completed=completed,
                            total=total,
                            speed=speed,
                            status=DownloadStatus.ACTIVE,
                            filename=download.name,
                            eta=eta,
                            connections=download.connections
                        )
                        
                        progress_text = (
                            f"⬇️ Downloading: {download.name}\n"
                            f"📊 Progress: {progress.percentage:.1f}%\n"
                            f"⚡ Speed: {self._format_speed(speed)}\n"
                            f"⏱️ ETA: {self._format_eta(eta)}\n"
                            f"🌐 Connections: {download.connections}"
                        )
                        
                        await progress_callback(completed, total, progress_text)
                    
                    last_progress_update = current_time
                
                # Check for download stall
                if download_started and completed == last_completed and speed == 0:
                    if stall_start_time is None:
                        stall_start_time = current_time
                        logger.warning(f"Download may be stalled: {download.name}")
                    elif current_time - stall_start_time > self.stall_timeout:
                        # Try to restart if stalled for too long
                        logger.warning(f"Download stalled for {self.stall_timeout}s, attempting to resume: {download.gid}")
                        try:
                            await asyncio.to_thread(download.pause)
                            await asyncio.sleep(1)
                            await asyncio.to_thread(download.resume)
                            stall_start_time = None  # Reset stall timer
                        except Exception as e:
                            logger.error(f"Failed to resume stalled download: {str(e)}")
                else:
                    stall_start_time = None  # Reset stall detection if progress made
                
                # Update tracking variables
                last_completed = completed
                
                # Sleep before next check
                await asyncio.sleep(self.progress_interval)
                
                # Check if download failed or was removed
                if download.has_failed:
                    error_message = f"Download failed: {download.error_message}"
                    logger.error(error_message)
                    raise DownloadError(error_message)
                
        except DownloadError:
            # Re-raise specific download errors
            raise
        except Exception as e:
            logger.exception(f"Error monitoring download: {str(e)}")
            raise DownloadProgressError(f"Error monitoring download: {str(e)}") from e

    async def _verify_download(self, file_path: str) -> bool:
        """
        Verify that the downloaded file exists and is complete
        
        Args:
            file_path: Path to downloaded file
            
        Returns:
            bool: True if file exists and is complete
        """
        # Clean file path if necessary
        clean_path = file_path.replace("[METADATA]", "").strip()
        file_path = clean_path if clean_path != file_path else file_path
        path_obj = Path(file_path)
        
        verification_start = time.time()
        
        while time.time() - verification_start < self.verification_timeout:
            try:
                # Check if file exists
                if not path_obj.exists():
                    alt_path = path_obj.parent / path_obj.name
                    logger.info(f"Original path not found, trying: {alt_path}")
                    
                    if alt_path.exists():
                        path_obj = alt_path
                    else:
                        logger.warning(f"File not found at: {path_obj} or {alt_path}")
                        await asyncio.sleep(1)
                        continue
                
                # Check if .aria2 control file is gone (indicates complete download)
                aria2_control = Path(f"{path_obj}.aria2")
                if aria2_control.exists():
                    logger.info(f"Waiting for .aria2 control file to be removed: {aria2_control}")
                    await asyncio.sleep(1)
                    continue
                
                # Check file size and stability
                initial_size = path_obj.stat().st_size
                await asyncio.sleep(self.verify_wait)
                current_size = path_obj.stat().st_size
                
                if current_size > 0 and initial_size == current_size:
                    logger.info(f"File verified: {path_obj} ({current_size/(1024*1024):.2f}MB)")
                    return True
                
                logger.info(f"File size changed during verification: {initial_size} -> {current_size}")
                
            except Exception as e:
                logger.error(f"Verification error: {str(e)}")
            
            await asyncio.sleep(1)
        
        logger.error(f"Verification timeout after {self.verification_timeout}s: {file_path}")
        return False

    async def _try_reconnect(self) -> bool:
        """
        Attempt to reconnect to aria2
        
        Returns:
            bool: True if reconnection successful
        """
        logger.warning("Attempting to reconnect to aria2")
        try:
            self.aria2 = None
            return await self.connect()
        except Exception as e:
            logger.error(f"Reconnection failed: {str(e)}")
            return False

    def _sanitize_filename(self, filename: str) -> str:
        """
        Remove invalid characters from filename
        
        Args:
            filename: Original filename
            
        Returns:
            str: Sanitized filename
        """
        # Remove invalid characters and sanitize filename
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        return filename.strip()

    def _format_speed(self, speed_bytes: int) -> str:
        """
        Format download speed for display
        
        Args:
            speed_bytes: Speed in bytes per second
            
        Returns:
            str: Formatted speed string
        """
        units = ['B/s', 'KB/s', 'MB/s', 'GB/s']
        unit_index = 0
        speed_float = float(speed_bytes)
        
        while speed_float > 1024 and unit_index < len(units) - 1:
            speed_float /= 1024
            unit_index += 1
            
        return f"{speed_float:.2f} {units[unit_index]}"

    def _format_eta(self, seconds: float) -> str:
        """
        Format estimated time of arrival for display
        
        Args:
            seconds: Time in seconds
            
        Returns:
            str: Formatted ETA string
        """
        if seconds < 0 or seconds > 86400 * 7:  # More than a week
            return "∞"
            
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        
        if days > 0:
            return f"{days}d {hours}h"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    async def cleanup(self) -> None:
        """Clean up resources and cancel active downloads"""
        try:
            if self.aria2 and self.active_downloads:
                for gid, download in list(self.active_downloads.items()):
                    try:
                        logger.info(f"Canceling download: {gid}")
                        await asyncio.to_thread(self.aria2.remove, [download])
                    except Exception as e:
                        logger.error(f"Error canceling download {gid}: {str(e)}")
                        
            self.active_downloads = {}
            self.aria2 = None
            logger.info("Downloader cleanup complete")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with cleanup"""
        await self.cleanup()