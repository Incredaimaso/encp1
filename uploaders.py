from pyrogram import Client, errors
import os
import asyncio
import time
import logging
from typing import Optional, Callable, Dict, Any
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Uploader')

class UploadError(Exception):
    """Custom exception for upload-related errors"""
    pass

class NetworkError(UploadError):
    """Network-related upload errors"""
    pass

class FileError(UploadError):
    """File-related upload errors"""
    pass

class VerificationError(UploadError):
    """Upload verification errors"""
    pass

class Uploader:
    # Increased buffer size for faster uploads (10MB)
    BUFFER_SIZE = 10 * 1024 * 1024
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds
    
    # Verification timeout
    VERIFICATION_TIMEOUT = 30  # seconds

    @staticmethod
    @asynccontextmanager
    async def upload_session(client: Client):
        """Context manager for handling upload sessions and cleanup"""
        try:
            yield
        except asyncio.CancelledError:
            logger.warning("Upload task was cancelled. Performing cleanup...")
            # Allow pending operations to complete
            await asyncio.sleep(0.5)
            raise
        except errors.FloodWait as e:
            logger.warning(f"Rate limit hit. Need to wait {e.value} seconds")
            raise NetworkError(f"Rate limited: must wait {e.value} seconds")
        except (errors.BadRequest, errors.Unauthorized, errors.Forbidden) as e:
            logger.error(f"API error: {str(e)}")
            raise UploadError(f"Telegram API error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during upload: {str(e)}", exc_info=True)
            raise
        finally:
            # Ensure all pending tasks are properly cleaned up
            tasks = [t for t in asyncio.all_tasks() 
                    if t is not asyncio.current_task() and "upload_progress" in str(t)]
            for task in tasks:
                task.cancel()
            
            logger.info("Upload session cleanup completed")

    @staticmethod
    async def _progress_tracker(current: int, total: int, progress_callback: Callable, 
                              update_interval: float = 1.0) -> None:
        """Tracks and reports upload progress with rate limiting"""
        task_data = {}
        
        async def update_progress():
            while True:
                if 'current' in task_data and 'total' in task_data:
                    current, total = task_data['current'], task_data['total']
                    elapsed = time.time() - task_data.get('start_time', time.time())
                    speed = current / elapsed if elapsed > 0 else 0
                    eta = (total - current) / speed if speed > 0 else 0
                    
                    try:
                        await progress_callback(current, total,
                            f"📤 Uploading file...\n"
                            f"📊 Progress: {(current/total)*100:.1f}%\n"
                            f"📦 Size: {current/(1024*1024):.1f}MB / {total/(1024*1024):.1f}MB\n"
                            f"⚡ Speed: {speed/(1024*1024):.2f} MB/s\n"
                            f"⏱️ ETA: {int(eta/60)}m {int(eta%60)}s"
                        )
                    except Exception as e:
                        logger.warning(f"Progress callback error: {str(e)}")
                
                await asyncio.sleep(update_interval)
        
        # Start the progress tracker in a separate task
        progress_task = asyncio.create_task(update_progress(), name="upload_progress")
        task_data['start_time'] = time.time()
        
        try:
            def progress(current: int, total: int):
                task_data['current'] = current
                task_data['total'] = total
            
            return progress
        finally:
            # Cleanup when done
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass

    @staticmethod
    async def verify_upload(client: Client, chat_id: int, message_id: int, 
                          expected_size: int) -> bool:
        """Verifies that uploaded file exists and has correct size"""
        try:
            # Create a timeout for the verification
            async with asyncio.timeout(Uploader.VERIFICATION_TIMEOUT):
                # Get message to verify document exists
                message = await client.get_messages(chat_id, message_ids=message_id)
                
                if not message or not message.document:
                    logger.error("Verification failed: Message or document not found")
                    return False
                
                # Verify file size matches expected size
                actual_size = message.document.file_size
                if abs(actual_size - expected_size) > 1024:  # Allow 1KB difference
                    logger.error(f"Size mismatch: expected {expected_size}, got {actual_size}")
                    return False
                
                logger.info(f"Upload verified successfully: message_id={message_id}")
                return True
        except asyncio.TimeoutError:
            logger.error("Verification timed out")
            return False
        except Exception as e:
            logger.error(f"Verification error: {str(e)}")
            return False

    @staticmethod
    async def upload_video(client: Client, chat_id: int,
                          video_path: str, caption: str,
                          progress_callback: Callable, 
                          filename: str = None,
                          retry_count: int = 0) -> bool:
        """
        Uploads a video file to Telegram with robust error handling and retries
        
        Args:
            client: PyroFork client
            chat_id: Target chat ID
            video_path: Path to video file
            caption: Caption for the uploaded file
            progress_callback: Callback function for progress updates
            filename: Optional filename override
            retry_count: Current retry count (internal)
            
        Returns:
            bool: True if upload succeeded, False otherwise
            
        Raises:
            FileError: If file doesn't exist or is empty
            NetworkError: For network-related issues
            UploadError: For other upload failures
        """
        # Validate file
        if not os.path.exists(video_path):
            raise FileError(f"Upload file not found: {video_path}")

        file_size = os.path.getsize(video_path)
        if file_size == 0:
            raise FileError(f"Upload file is empty: {video_path}")

        logger.info(f"Starting upload: {video_path} ({file_size/(1024*1024):.2f} MB)")
        
        async with Uploader.upload_session(client):
            try:
                # Setup progress tracking in a separate task
                progress = await Uploader._progress_tracker(0, file_size, progress_callback)
                
                # Perform upload with automatic stream handling
                message = await client.send_document(
                    chat_id=chat_id,
                    document=video_path,
                    caption=caption,
                    file_name=filename or os.path.basename(video_path),
                    force_document=True,
                    progress=progress,
                    disable_notification=True,
                    file_size=file_size,
                    stream=True
                )

                # Verify upload was successful
                if not await Uploader.verify_upload(client, chat_id, message.id, file_size):
                    raise VerificationError("Upload verification failed")

                logger.info(f"Upload completed successfully: {video_path}")
                return True

            except (errors.FloodWait, errors.ServerError) as e:
                # Handle rate limiting and server errors with retries
                retry_count += 1
                wait_time = getattr(e, 'value', Uploader.RETRY_DELAY)
                
                if retry_count <= Uploader.MAX_RETRIES:
                    logger.warning(f"Upload failed, retrying ({retry_count}/{Uploader.MAX_RETRIES}) "
                                  f"after {wait_time}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                    return await Uploader.upload_video(client, chat_id, video_path, caption,
                                                    progress_callback, filename, retry_count)
                else:
                    logger.error(f"Max retries exceeded for upload: {str(e)}")
                    raise NetworkError(f"Upload failed after {Uploader.MAX_RETRIES} retries: {str(e)}")

            except asyncio.CancelledError:
                logger.warning("Upload cancelled by user")
                raise
            
            except Exception as e:
                logger.error(f"Upload error: {str(e)}", exc_info=True)
                raise UploadError(f"Upload failed: {str(e)}")