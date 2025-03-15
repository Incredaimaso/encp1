"""
Robust logging system for Telegram bots with comprehensive error handling and metrics.

This module provides a structured logging system that tracks task execution,
captures performance metrics, and gracefully handles communication failures.
"""

from pyrogram import Client
from pyrogram.errors import RPCError, FloodWait, ChatWriteForbidden, MessageNotModified
from pyrogram.types import Message
from dataclasses import dataclass
from typing import Optional, Union, Dict, Any
from datetime import datetime
import time
import asyncio
import logging
import traceback

# Configure standard Python logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)

logger = logging.getLogger("BotLogger")

@dataclass
class TaskProgress:
    """Data class for representing task progress metrics."""
    percent: float
    speed: float
    current: float
    total: float
    eta: str


class LoggingError(Exception):
    """Base exception class for logging-related errors."""
    pass


class ChannelWriteError(LoggingError):
    """Raised when writing to the log channel fails."""
    pass


class MessageUpdateError(LoggingError):
    """Raised when updating a message fails."""
    pass


class BotLogger:
    """
    Comprehensive logging system for Telegram bots with robust error handling.
    
    This class manages task logging to a designated channel, handles Telegram API
    errors gracefully, and provides detailed progress tracking with fallback mechanisms.
    """
    
    def __init__(self, client: Client, config: Any):
        """
        Initialize the logger with client and configuration.
        
        Args:
            client: The Pyrogram client instance
            config: Configuration object containing logging settings
        """
        self.client = client
        self.log_channel = getattr(config, "LOG_CHANNEL", None)
        self.enabled = getattr(config, "ENABLE_LOGS", False)
        self.fallback_enabled = getattr(config, "ENABLE_FALLBACK_LOGGING", True)
        self.retry_attempts = getattr(config, "LOG_RETRY_ATTEMPTS", 3)
        self.retry_delay = getattr(config, "LOG_RETRY_DELAY", 2)
        
        # Task tracking
        self.task_messages: Dict[str, int] = {}  # Maps task_id to message_id
        self.task_start_times: Dict[str, float] = {}  # Maps task_id to start timestamp
        
        # Verify configuration
        self._verify_configuration()
    
    def _verify_configuration(self) -> None:
        """Verify logger configuration and log warnings for potential issues."""
        if self.enabled and not self.log_channel:
            logger.warning("Logging enabled but no LOG_CHANNEL specified. Channel logging will be disabled.")
            self.enabled = False
            
        if not self.enabled:
            logger.info("Channel logging is disabled. Using fallback logging only.")
    
    async def log_task_start(self, task_id: str, user_info: dict) -> Optional[Message]:
        """
        Initialize task logging in the channel.
        
        Args:
            task_id: Unique identifier for the task
            user_info: Dictionary containing user details
            
        Returns:
            The sent message object if successful, None otherwise
            
        Raises:
            ChannelWriteError: If channel writing fails after retries
        """
        if not self._can_log():
            return None
            
        text = self._format_task_start_message(task_id, user_info)
        
        try:
            msg = await self._retry_operation(
                lambda: self.client.send_message(
                    chat_id=self.log_channel,
                    text=text,
                    disable_web_page_preview=True
                )
            )
            
            if msg:
                self.task_messages[task_id] = msg.id
                self.task_start_times[task_id] = time.time()
                logger.info(f"Started logging for task {task_id}")
            return msg
            
        except LoggingError as e:
            self._handle_logging_failure("task start", task_id, e)
            return None
    
    async def update_task_progress(self, task_id: str, status: str, progress: Optional[Dict[str, float]] = None) -> None:
        """
        Update task progress in the log channel.
        
        Args:
            task_id: Unique identifier for the task
            status: Current status message
            progress: Dictionary containing progress metrics
            
        Raises:
            MessageUpdateError: If message update fails after retries
        """
        if not self._can_log() or task_id not in self.task_messages:
            return
            
        try:
            # Create TaskProgress object if progress data is provided
            progress_obj = None
            if progress:
                try:
                    progress_obj = TaskProgress(
                        percent=progress.get('percent', 0),
                        speed=progress.get('speed', 0),
                        current=progress.get('current', 0),
                        total=progress.get('total', 0),
                        eta=progress.get('eta', 'calculating...')
                    )
                except (KeyError, TypeError) as e:
                    logger.warning(f"Invalid progress data format: {e}")
            
            duration = time.time() - self.task_start_times.get(task_id, time.time())
            text = self._format_progress_message(task_id, status, duration, progress_obj)
            
            await self._retry_operation(
                lambda: self.client.edit_message_text(
                    chat_id=self.log_channel,
                    message_id=self.task_messages[task_id],
                    text=text
                )
            )
            
            logger.debug(f"Updated progress for task {task_id}: {status}")
            
        except MessageNotModified:
            # This is normal if the content hasn't changed
            pass
        except LoggingError as e:
            self._handle_logging_failure("progress update", task_id, e)
    
    async def log_task_completion(self, task_id: str, success: bool, result: Optional[str] = None) -> None:
        """
        Log task completion with final status.
        
        Args:
            task_id: Unique identifier for the task
            success: Whether the task completed successfully
            result: Optional result details
        """
        if not self._can_log() or task_id not in self.task_messages:
            return
            
        try:
            duration = time.time() - self.task_start_times.get(task_id, time.time())
            status = "✅ Completed Successfully" if success else "❌ Failed"
            
            text = (
                f"🏁 Task Completed: `{task_id}`\n"
                f"⏱️ Total Duration: {self._format_duration(duration)}\n"
                f"📊 Final Status: {status}\n"
            )
            
            if result:
                text += f"\n📋 Result: {result}\n"
                
            text += "➖➖➖➖➖➖➖➖➖➖➖➖"
            
            await self._retry_operation(
                lambda: self.client.edit_message_text(
                    chat_id=self.log_channel,
                    message_id=self.task_messages[task_id],
                    text=text
                )
            )
            
            # Clean up tracking data
            self.task_messages.pop(task_id, None)
            self.task_start_times.pop(task_id, None)
            
            logger.info(f"Task {task_id} completed with status: {status}")
            
        except LoggingError as e:
            self._handle_logging_failure("completion", task_id, e)
    
    async def log_system_error(self, error: Exception, context: str = "system") -> None:
        """
        Log system-level errors to both channel and fallback logger.
        
        Args:
            error: The exception that occurred
            context: Context description where the error happened
        """
        error_traceback = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        
        # Always log to file/console
        logger.error(f"Error in {context}: {error}\n{error_traceback}")
        
        if not self._can_log():
            return
            
        try:
            text = (
                f"⚠️ System Error Detected\n\n"
                f"🔍 Context: `{context}`\n"
                f"⏱️ Time: {self._get_current_time()}\n"
                f"❌ Error: `{type(error).__name__}: {str(error)}`\n\n"
                f"```\n{error_traceback[:3800]}```" # Trim to avoid message length limits
            )
            
            await self._retry_operation(
                lambda: self.client.send_message(
                    chat_id=self.log_channel,
                    text=text,
                    disable_web_page_preview=True
                )
            )
            
        except LoggingError as e:
            logger.error(f"Failed to log system error to channel: {e}")
    
    async def forward_message(self, message: Message) -> Optional[Message]:
        """
        Forward message to log channel with retry logic.
        
        Args:
            message: The message to forward
            
        Returns:
            The forwarded message if successful, None otherwise
        """
        if not self._can_log():
            return None
            
        try:
            return await self._retry_operation(lambda: message.forward(self.log_channel))
        except LoggingError as e:
            logger.error(f"Failed to forward message: {e}")
            return None
    
    async def log_file(self, file_path: str, caption: str) -> Optional[Message]:
        """
        Send file to log channel with retry logic.
        
        Args:
            file_path: Path to the file to be sent
            caption: Caption for the file
            
        Returns:
            The sent message if successful, None otherwise
        """
        if not self._can_log():
            return None
            
        try:
            return await self._retry_operation(
                lambda: self.client.send_document(
                    chat_id=self.log_channel,
                    document=file_path,
                    caption=caption
                )
            )
        except LoggingError as e:
            logger.error(f"Failed to log file {file_path}: {e}")
            return None
    
    # Private helper methods
    
    def _can_log(self) -> bool:
        """Check if channel logging is available."""
        return self.enabled and self.log_channel is not None
    
    def _format_task_start_message(self, task_id: str, user_info: dict) -> str:
        """Format the initial task message."""
        return (
            "🆕 New Task Added to Queue\n\n"
            f"🆔 Task ID: `{task_id}`\n"
            f"👤 User: {user_info.get('mention', 'Unknown')}\n"
            f"💬 Chat: {user_info.get('chat_title', 'Private')}\n"
            f"📁 File: `{user_info.get('filename', 'N/A')}`\n"
            f"⏱️ Added: {self._get_current_time()}\n"
            "➖➖➖➖➖➖➖➖➖➖➖➖\n"
            "⏳ Status: Queued"
        )
    
    def _format_progress_message(self, task_id: str, status: str, duration: float, 
                               progress: Optional[TaskProgress] = None) -> str:
        """Format the progress update message."""
        progress_text = self._format_progress_details(progress) if progress else ""
        
        return (
            f"⚡ Task: `{task_id}`\n"
            f"⏱️ Duration: {self._format_duration(duration)}\n"
            f"📊 Status: {status}\n"
            f"{progress_text}\n"
            "➖➖➖➖➖➖➖➖➖➖➖➖"
        )
    
    def _format_progress_details(self, progress: TaskProgress) -> str:
        """Format progress details with styled metrics."""
        try:
            # Ensure percent is between 0-100 for bar calculation
            percent = max(0, min(100, progress.percent))
            bars_count = int(percent / 10)
            bars = "▰" * bars_count + "▱" * (10 - bars_count)
            
            return (
                f"\n📈 Progress Details:\n"
                f"├─⚡ Speed: {progress.speed:.2f} MB/s\n"
                f"├─📊 Progress: [{bars}] {percent:.1f}%\n"
                f"├─📦 Size: {progress.current:.1f}/{progress.total:.1f} MB\n"
                f"└─⏳ ETA: {progress.eta}"
            )
        except (AttributeError, TypeError, ValueError) as e:
            logger.warning(f"Error formatting progress details: {e}")
            return "\n⚠️ Progress data formatting error"
    
    def _get_current_time(self) -> str:
        """Get formatted current time."""
        return datetime.now().strftime("%I:%M:%S %p")
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable form."""
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    async def _retry_operation(self, operation, attempts: int = None) -> Any:
        """
        Retry an async operation with exponential backoff.
        
        Args:
            operation: Callable that returns a coroutine
            attempts: Number of retry attempts (defaults to self.retry_attempts)
            
        Returns:
            The result of the operation if successful
            
        Raises:
            LoggingError: If all retry attempts fail
        """
        if attempts is None:
            attempts = self.retry_attempts
            
        last_error = None
        
        for attempt in range(1, attempts + 1):
            try:
                return await operation()
                
            except FloodWait as e:
                logger.warning(f"FloodWait error, waiting for {e.value} seconds")
                await asyncio.sleep(e.value)
                last_error = e
                
            except ChatWriteForbidden as e:
                # Can't write to this chat, disable channel logging
                logger.error(f"No permission to write to log channel: {e}")
                self.enabled = False
                raise ChannelWriteError(f"No permission to write to log channel: {e}")
                
            except RPCError as e:
                logger.warning(f"Telegram API error on attempt {attempt}/{attempts}: {e}")
                await asyncio.sleep(self.retry_delay * attempt)  # Exponential backoff
                last_error = e
                
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt}/{attempts}: {e}")
                await asyncio.sleep(self.retry_delay)
                last_error = e
        
        # All attempts failed
        error_msg = f"Operation failed after {attempts} attempts: {last_error}"
        logger.error(error_msg)
        raise LoggingError(error_msg)
    
    def _handle_logging_failure(self, operation: str, task_id: str, error: Exception) -> None:
        """Log failures to fallback logger when channel logging fails."""
        if self.fallback_enabled:
            logger.error(f"Failed to log {operation} for task {task_id}: {error}")