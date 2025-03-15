"""
Video Encoding Service
Main application entry point with robust error handling and resource management.
"""

import asyncio
import os
import shutil
import signal
import sys
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Optional, Set, Tuple, Union

import psutil
from pyrogram.types import Message

from bot_manager import BotManager
from config import Config
from display import ProgressTracker
from downloaders import Downloader
from encode import VideoEncoder
from logger import BotLogger
from queue_manager import QueueItem
from startup import process_manager, start_aria2c
from uploaders import Uploader


# === Constants and Type Definitions ===
class TaskStatus(Enum):
    """Enumeration of possible task statuses for clear state tracking."""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    ENCODING = "encoding"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class EncodingResult:
    """Data class to store encoding results."""
    file_path: str
    quality: str
    original_size: float  # MB
    encoded_size: float  # MB
    reduction_percent: float
    target_exceeded: bool = False
    size_excess: float = 0.0  # Percentage


class ApplicationError(Exception):
    """Base exception class for application-specific errors."""
    pass


class DownloadError(ApplicationError):
    """Exception raised for errors during download phase."""
    pass


class EncodingError(ApplicationError):
    """Exception raised for errors during encoding phase."""
    pass


class UploadError(ApplicationError):
    """Exception raised for errors during upload phase."""
    pass


class EncodingTracker:
    """Tracks the state of multi-quality encoding tasks."""
    
    def __init__(self):
        self.completed_qualities: Set[str] = set()
        self.uploaded_files: Set[str] = set()
        self.failed_qualities: Dict[str, str] = {}  # quality -> error message
    
    def mark_quality_complete(self, quality: str) -> None:
        """Mark a specific quality as successfully completed."""
        self.completed_qualities.add(quality)
    
    def mark_file_uploaded(self, file_path: str) -> None:
        """Mark a file as successfully uploaded."""
        self.uploaded_files.add(file_path)
    
    def mark_quality_failed(self, quality: str, error: str) -> None:
        """Mark a quality as failed with error reason."""
        self.failed_qualities[quality] = error
    
    def is_complete(self, qualities: List[str]) -> bool:
        """Check if all requested qualities have been completed."""
        return all(q in self.completed_qualities for q in qualities)
    
    def get_completion_status(self, qualities: List[str]) -> Dict[str, str]:
        """Get comprehensive status report for all qualities."""
        result = {}
        for q in qualities:
            if q in self.completed_qualities:
                result[q] = "✅ Completed"
            elif q in self.failed_qualities:
                result[q] = f"❌ Failed: {self.failed_qualities[q]}"
            else:
                result[q] = "⏳ Pending"
        return result


# === Directory Management ===
class DirectoryManager:
    """Manages application directories with proper cleanup."""
    
    DOWNLOADS_DIR = "downloads"
    ENCODES_DIR = "encodes"
    
    @classmethod
    def setup_directories(cls) -> None:
        """Ensure all required directories exist."""
        for directory in [cls.DOWNLOADS_DIR, cls.ENCODES_DIR]:
            os.makedirs(directory, exist_ok=True)
    
    @classmethod
    def cleanup_directories(cls) -> None:
        """Clean up all temporary directories."""
        for directory in [cls.DOWNLOADS_DIR, cls.ENCODES_DIR]:
            if os.path.exists(directory):
                try:
                    shutil.rmtree(directory)
                    os.makedirs(directory)
                except (PermissionError, OSError) as e:
                    print(f"Warning: Failed to clean {directory}: {e}")
    
    @staticmethod
    def safe_remove(file_path: str) -> bool:
        """Safely remove a file with error handling."""
        if not file_path or not os.path.exists(file_path):
            return True
        
        try:
            os.remove(file_path)
            return True
        except (PermissionError, OSError) as e:
            print(f"Warning: Failed to remove {file_path}: {e}")
            return False


# === Task Processing ===
class TaskProcessor:
    """Handles processing of encoding tasks."""
    
    def __init__(self, logger: BotLogger, message: Message):
        self.logger = logger
        self.message = message
        self.status_message: Optional[Message] = None
        self.log_message: Optional[Message] = None
        self.encoder: Optional[VideoEncoder] = None
        self.tracker = EncodingTracker()
        self.downloader = Downloader(
            Config.ARIA2_HOST, 
            Config.ARIA2_PORT, 
            Config.ARIA2_SECRET
        )
    
    async def initialize(self, task_id: str) -> None:
        """Initialize the task with status messages."""
        self.log_message = await self.logger.log_message(
            f"⚡ New task started\n"
            f"👤 User: {self.message.from_user.mention}\n"
            f"🆔 Task: {task_id}"
        )
        
        self.status_message = await self.message.reply_text(
            f"⏳ Processing task {task_id}...\n"
            f"Use /cancel {task_id} to stop this task"
        )
        
        self.progress_tracker = ProgressTracker(
            lambda text: self.status_message.edit_text(text) 
            if self.status_message else None
        )
    
    async def update_status(self, text: str) -> None:
        """Update status message with proper error handling."""
        if not self.status_message:
            return
        
        try:
            await self.status_message.edit_text(text)
        except Exception as e:
            print(f"Failed to update status: {e}")
    
    async def download_file(self, file_path: str, is_url: bool) -> Tuple[str, float]:
        """Download a file with comprehensive error handling."""
        if not is_url:
            try:
                if not os.path.exists(file_path):
                    raise DownloadError(f"Local file not found: {file_path}")
                
                file_size = os.path.getsize(file_path) / (1024 * 1024)
                return file_path, file_size
            except (OSError, IOError) as e:
                raise DownloadError(f"Error accessing local file: {str(e)}")
        
        try:
            await self.update_status("⬇️ Starting download...")
            
            # Execute download
            downloaded_file, file_size = await self.downloader.download_aria2(
                file_path,
                self.progress_tracker.update_progress,
                DirectoryManager.DOWNLOADS_DIR
            )
            
            # Verify download
            if not os.path.exists(downloaded_file):
                raise DownloadError("Download verification failed - file not found")
            
            actual_size = os.path.getsize(downloaded_file) / (1024 * 1024)
            
            # File size validation
            if actual_size > Config.MAX_FILE_SIZE_MB:
                DirectoryManager.safe_remove(downloaded_file)
                raise DownloadError(
                    f"File too large: {actual_size:.1f}MB "
                    f"(max: {Config.MAX_FILE_SIZE_MB}MB)"
                )
            
            # Log download completion
            await self.update_status(
                f"✅ Download complete!\n"
                f"📁 File: {os.path.basename(downloaded_file)}\n"
                f"📦 Size: {actual_size:.1f}MB\n"
                "🎬 Preparing to encode..."
            )
            
            await self.logger.log_status(
                f"✅ Download complete\n"
                f"📁 File: {os.path.basename(downloaded_file)}\n"
                f"📦 Size: {actual_size:.1f}MB",
                self.log_message.id if self.log_message else None
            )
            
            return downloaded_file, actual_size
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if isinstance(e, DownloadError):
                raise
            raise DownloadError(f"Download failed: {str(e)}")
    
    async def encode_quality(
        self, 
        downloaded_file: str, 
        quality: str, 
        original_size: float,
        cancel_flag: asyncio.Event
    ) -> EncodingResult:
        """Encode a video at a specific quality level."""
        if not self.encoder:
            self.encoder = VideoEncoder()
        
        # Prepare output path
        base_name = os.path.splitext(os.path.basename(downloaded_file))[0]
        output_path = os.path.join(
            DirectoryManager.ENCODES_DIR,
            f"{base_name}_{quality}.mkv"
        )
        
        try:
            await self.update_status(f"🎬 Starting {quality} encode...")
            
            # Start encoding
            encoded_file, encode_info = await self.encoder.encode_video(
                downloaded_file, 
                output_path,
                Config.TARGET_SIZES[quality], 
                quality,
                progress_callback=self.progress_tracker.update_progress,
                cancel_event=cancel_flag
            )
            
            # Verify encoded file
            if not encoded_file or not os.path.exists(encoded_file):
                raise EncodingError(f"Encoding failed for {quality} - file not found")
            
            encoded_size = os.path.getsize(encoded_file) / (1024 * 1024)
            reduction = ((original_size - encoded_size) / original_size) * 100
            
            target_exceeded = encode_info.get('target_exceeded', False)
            size_excess = encode_info.get('size_excess', 0.0)
            
            return EncodingResult(
                file_path=encoded_file,
                quality=quality,
                original_size=original_size,
                encoded_size=encoded_size,
                reduction_percent=reduction,
                target_exceeded=target_exceeded,
                size_excess=size_excess
            )
            
        except asyncio.CancelledError:
            DirectoryManager.safe_remove(output_path)
            raise
        except Exception as e:
            DirectoryManager.safe_remove(output_path)
            if isinstance(e, EncodingError):
                raise
            raise EncodingError(f"Error encoding {quality}: {str(e)}")
    
    async def upload_encoded_file(self, encode_result: EncodingResult) -> bool:
        """Upload an encoded file with retry logic."""
        encoded_file = encode_result.file_path
        quality = encode_result.quality
        
        for upload_attempt in range(1, 4):  # 3 attempts
            try:
                await self.update_status(
                    f"📤 Uploading {quality} ({upload_attempt}/3)..."
                )
                
                # Prepare caption
                caption = (
                    f"🎥 {os.path.splitext(os.path.basename(encoded_file))[0]}\n"
                    f"📊 Quality: {quality}\n"
                    f"📦 Size: {encode_result.encoded_size:.1f}MB\n"
                    f"🔄 Reduced: {encode_result.reduction_percent:.1f}%"
                )
                
                if encode_result.target_exceeded:
                    caption += f"\n⚠️ Note: Size exceeded target by {encode_result.size_excess:.1f}%"
                
                # Upload the file
                await Uploader.upload_video(
                    self.message._client,
                    self.message.chat.id,
                    encoded_file,
                    caption,
                    progress_callback=self.progress_tracker.update_progress,
                    filename=os.path.basename(encoded_file)
                )
                
                await self.update_status(f"✅ {quality} completed and uploaded!")
                return True
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if upload_attempt < 3:
                    print(f"Upload attempt {upload_attempt} failed: {e}")
                    await asyncio.sleep(5)  # Wait before retry
                    continue
                raise UploadError(f"Failed to upload {quality} after 3 attempts: {str(e)}")
        
        return False  # Should never reach here due to exception in final attempt


# === Main Task Processing Function ===
async def process_queue_item(item: QueueItem) -> None:
    """Process a single queue item with comprehensive error handling."""
    downloaded_file = None
    encoded_files = []
    cancel_event = asyncio.Event()
    
    # Track item's cancellation flag
    if hasattr(item, 'cancel_flag'):
        asyncio.create_task(_monitor_cancel_flag(item.cancel_flag, cancel_event))
    
    try:
        # Initialize processing components
        logger = BotLogger(item.message._client)
        processor = TaskProcessor(logger, item.message)
        await processor.initialize(item.task_id)
        
        # Phase 1: Download
        try:
            downloaded_file, original_size = await processor.download_file(
                item.file_path, 
                item.is_url
            )
        except DownloadError as e:
            await processor.update_status(f"❌ Download failed: {str(e)}")
            await logger.log_status(
                f"❌ Download failed: {str(e)}",
                processor.log_message.id if processor.log_message else None
            )
            return
        
        # Phase 2: Process each quality
        success_count = 0
        for quality in Config.QUALITIES:
            if cancel_event.is_set():
                break
                
            try:
                # Encode the video
                encode_result = await processor.encode_quality(
                    downloaded_file, 
                    quality, 
                    original_size,
                    cancel_event
                )
                encoded_files.append(encode_result.file_path)
                
                # Upload the encoded video
                upload_success = await processor.upload_encoded_file(encode_result)
                
                if upload_success:
                    processor.tracker.mark_quality_complete(quality)
                    processor.tracker.mark_file_uploaded(encode_result.file_path)
                    success_count += 1
                
                # Clean up the encoded file
                DirectoryManager.safe_remove(encode_result.file_path)
                encoded_files = [f for f in encoded_files if os.path.exists(f)]
                
            except (EncodingError, UploadError) as e:
                processor.tracker.mark_quality_failed(quality, str(e))
                await processor.update_status(f"❌ Error with {quality}: {str(e)}")
                continue
            except asyncio.CancelledError:
                await processor.update_status(f"🛑 Task {item.task_id} was cancelled")
                return
            except Exception as e:
                processor.tracker.mark_quality_failed(quality, f"Unexpected error: {str(e)}")
                await processor.update_status(f"❌ Error with {quality}: {str(e)}")
                continue
        
        # Final status update
        if cancel_event.is_set():
            await processor.update_status(f"🛑 Task {item.task_id} was cancelled")
        elif success_count == len(Config.QUALITIES):
            await processor.update_status("✅ All qualities processed successfully!")
        else:
            status_lines = [f"⚠️ Task {item.task_id} completed with warnings:"]
            for quality, status in processor.tracker.get_completion_status(Config.QUALITIES).items():
                status_lines.append(f"• {quality}: {status}")
            await processor.update_status("\n".join(status_lines))
        
        # Log final status
        await logger.log_status(
            f"✅ Task {item.task_id} completed with {success_count}/{len(Config.QUALITIES)} qualities",
            processor.log_message.id if processor.log_message else None
        )
            
    except asyncio.CancelledError:
        await processor.update_status(f"🛑 Task {item.task_id} was cancelled")
    except Exception as e:
        await processor.update_status(f"❌ Fatal error: {str(e)}")
        print(f"Unhandled exception in task {item.task_id}: {str(e)}")
    finally:
        # Clean up all resources
        for file_path in [downloaded_file] + encoded_files:
            DirectoryManager.safe_remove(file_path)


async def _monitor_cancel_flag(cancel_flag: asyncio.Event, cancel_event: asyncio.Event) -> None:
    """Monitor a cancellation flag and propagate to the cancel event."""
    while True:
        if cancel_flag.is_set():
            cancel_event.set()
            break
        await asyncio.sleep(0.5)


# === Signal Handling and Cleanup ===
async def shutdown(sig=None) -> None:
    """Gracefully shut down all application components."""
    if sig:
        print(f"\n🛑 Received signal {sig.name}, shutting down gracefully...")
    else:
        print("\n🛑 Shutting down gracefully...")
    
    # Cancel all tasks except the current one
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        print(f"Error during task cancellation: {e}")
    
    # Kill any remaining ffmpeg processes
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if 'ffmpeg' in proc.name().lower():
                print(f"Terminating ffmpeg process {proc.pid}")
                proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            print(f"Process cleanup error: {e}")
    
    # Clean directories
    DirectoryManager.cleanup_directories()
    
    # Exit with success code
    if sys.version_info >= (3, 9):
        # For Python 3.9+, we can use the cleaner exit approach
        loop = asyncio.get_running_loop()
        loop.stop()
    else:
        sys.exit(0)


def handle_sigterm(signum, frame) -> None:
    """Handle SIGTERM and SIGINT signals."""
    print("\n🛑 Received shutdown signal, cleaning up...")
    # Close the event loop through a task to avoid sync/async issues
    if asyncio.get_event_loop().is_running():
        asyncio.create_task(shutdown())
    else:
        # For synchronous context
        DirectoryManager.cleanup_directories()
        # Force kill any running ffmpeg processes
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if 'ffmpeg' in proc.name().lower():
                    proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        sys.exit(0)


# === Main Application Entry Point ===
async def main() -> None:
    """Main application entry point with comprehensive error handling."""
    try:
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, handle_sigterm)
        signal.signal(signal.SIGINT, handle_sigterm)
        
        # Make signals work in asyncio
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig, 
                lambda s=sig: asyncio.create_task(shutdown(s))
            )
        
        # Initialize components
        await start_aria2c()
        DirectoryManager.setup_directories()
        
        # Start the bot
        bot = BotManager(process_queue_item)
        await bot.start()
        
        # Run indefinitely until shutdown is called
        shutdown_future = asyncio.create_task(wait_for_shutdown())
        await shutdown_future
        
    except Exception as e:
        print(f"Fatal application error: {e}")
    finally:
        await shutdown()


async def wait_for_shutdown() -> None:
    """Wait for shutdown event."""
    # This could be implemented with a proper shutdown event
    # For now, we just wait indefinitely
    shutdown_event = asyncio.Event()
    await shutdown_event.wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Received Ctrl+C")
    finally:
        # Ensure cleanup happens in any case
        if sys.version_info >= (3, 9):
            try:
                asyncio.run(shutdown())
            except RuntimeError:
                # The event loop might already be closed
                pass
        else:
            # For older Python versions
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(shutdown())
            loop.close()