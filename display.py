"""
Progress Tracker Module

A robust utility for tracking and displaying progress information for file operations
or other long-running processes with proper error handling and async support.
"""

import time
import os
import logging
from typing import Callable, Optional, Union, TypeVar, Awaitable
import asyncio
from math import floor
from enum import Enum, auto
from dataclasses import dataclass
from contextlib import contextmanager, asynccontextmanager

# Configure module logger
logger = logging.getLogger(__name__)

# Type definitions
MessageUpdaterT = TypeVar('MessageUpdaterT')
SyncUpdater = Callable[[str], None]
AsyncUpdater = Callable[[str], Awaitable[None]]
UpdaterCallable = Union[SyncUpdater, AsyncUpdater]


class ProgressError(Exception):
    """Base exception class for progress tracking errors."""
    pass


class UpdaterError(ProgressError):
    """Raised when the message updater fails."""
    pass


class StatusUpdateError(ProgressError):
    """Raised when a status update operation fails."""
    pass


class ProcessingState(Enum):
    """Enum representing the current state of the progress tracker."""
    INITIALIZING = auto()
    RUNNING = auto()
    PAUSED = auto()
    COMPLETED = auto()
    ERROR = auto()


@dataclass
class ProgressStats:
    """Container for progress statistics."""
    current_bytes: int = 0
    total_bytes: int = 0
    start_time: float = 0
    last_update_time: float = 0
    last_processed_mb: float = 0
    
    @property
    def current_mb(self) -> float:
        """Convert current bytes to megabytes."""
        return self.current_bytes / (1024 * 1024)
    
    @property
    def total_mb(self) -> float:
        """Convert total bytes to megabytes."""
        return self.total_bytes / (1024 * 1024)
    
    @property
    def percent_complete(self) -> float:
        """Calculate the percentage of completion."""
        if self.total_bytes <= 0:
            return 0
        return (self.current_bytes / self.total_bytes) * 100
    
    @property
    def elapsed_time(self) -> float:
        """Get the elapsed time since start."""
        return time.time() - self.start_time


class ProgressTracker:
    """
    Tracks and displays progress for long-running operations.
    
    This class provides robust progress tracking with customizable display options,
    error handling, and both synchronous and asynchronous update capabilities.
    """
    
    def __init__(
        self, 
        message_updater: UpdaterCallable,
        update_interval: float = 2.0,
        error_retry_count: int = 3,
        error_retry_delay: float = 1.0,
        log_errors: bool = True
    ):
        """
        Initialize the progress tracker.
        
        Args:
            message_updater: Callable that updates the progress message (sync or async)
            update_interval: Minimum time between updates in seconds
            error_retry_count: Number of retries on update failure
            error_retry_delay: Delay between retries in seconds
            log_errors: Whether to log errors to the module logger
        
        Raises:
            TypeError: If message_updater is not callable
        """
        if not callable(message_updater):
            raise TypeError("message_updater must be callable")
            
        self.message_updater = message_updater
        self.update_interval = max(0.5, update_interval)  # Enforce reasonable minimum
        self.error_retry_count = error_retry_count
        self.error_retry_delay = error_retry_delay
        self.log_errors = log_errors
        
        # Internal state
        self.stats = ProgressStats(start_time=time.time(), last_update_time=time.time())
        self.state = ProcessingState.INITIALIZING
        self._is_async_updater = asyncio.iscoroutinefunction(message_updater)
        self._update_lock = asyncio.Lock() if self._is_async_updater else None
    
    async def update_progress(
        self, 
        current: int, 
        total: int, 
        action: Optional[str] = None,
        filename: Optional[str] = None
    ) -> bool:
        """
        Update the progress display with the current status.
        
        Args:
            current: Current position in bytes
            total: Total size in bytes
            action: Optional action description (e.g., "Downloading", "Processing")
            filename: Optional filename being processed
        
        Returns:
            bool: True if the update was successful, False otherwise
        
        Raises:
            UpdaterError: If the updater fails after all retries
        """
        try:
            # Skip if we're updating too frequently
            current_time = time.time()
            if current_time - self.stats.last_update_time < self.update_interval:
                return True
                
            # Acquire lock if using async to prevent concurrent updates
            if self._update_lock:
                async with self._update_lock:
                    return await self._perform_update(current, total, action, filename)
            else:
                return await self._perform_update(current, total, action, filename)
                
        except Exception as e:
            if self.log_errors:
                logger.error(f"Progress update failed: {str(e)}", exc_info=True)
            if isinstance(e, (UpdaterError, StatusUpdateError)):
                raise
            raise StatusUpdateError(f"Failed to update progress: {str(e)}") from e
    
    async def _perform_update(
        self, 
        current: int, 
        total: int, 
        action: Optional[str] = None,
        filename: Optional[str] = None
    ) -> bool:
        """
        Internal method to perform the actual update operation.
        
        Args:
            current: Current position in bytes
            total: Total size in bytes
            action: Action description (e.g., "Downloading")
            filename: Filename being processed
        
        Returns:
            bool: True if update was successful
        
        Raises:
            UpdaterError: If the updater fails after all retries
        """
        # Update stats
        self.stats.current_bytes = current
        self.stats.total_bytes = total
        current_time = time.time()
        
        # Generate status text
        if isinstance(action, str) and '\n' in action:
            # Use custom status if provided with newlines
            status_text = action
        else:
            status_text = self._format_progress(
                action or "Processing",
                filename or "File",
                self.stats.current_mb,
                self.stats.total_mb,
                self.stats.last_processed_mb,
                self.stats.percent_complete
            )
        
        # Try to update the message with retries
        for attempt in range(self.error_retry_count + 1):
            try:
                if self._is_async_updater:
                    await self.message_updater(status_text)
                else:
                    self.message_updater(status_text)
                    
                # Update last processed stats
                self.stats.last_update_time = current_time
                self.stats.last_processed_mb = self.stats.current_mb
                return True
                
            except Exception as e:
                # Skip retrying for "not modified" messages from some APIs
                if "MESSAGE_NOT_MODIFIED" in str(e):
                    return True
                    
                if attempt < self.error_retry_count:
                    if self.log_errors:
                        logger.warning(
                            f"Update attempt {attempt+1} failed: {str(e)}. Retrying..."
                        )
                    await asyncio.sleep(self.error_retry_delay)
                else:
                    if self.log_errors:
                        logger.error(
                            f"Failed to update progress after {self.error_retry_count} attempts",
                            exc_info=True
                        )
                    raise UpdaterError(
                        f"Failed to update progress after {self.error_retry_count} attempts"
                    ) from e
        
        return False  # Should never reach here due to exception raising
    
    def _format_progress(
        self, 
        status: str, 
        filename: str, 
        current: float, 
        total: float, 
        last_value: float, 
        percent: float
    ) -> str:
        """
        Format a progress message with various metrics.
        
        Args:
            status: Status message (e.g., "Downloading")
            filename: Name of the file being processed
            current: Current progress in MB
            total: Total size in MB
            last_value: Previous progress value for speed calculation
            percent: Percentage complete
            
        Returns:
            str: Formatted progress message
        """
        # Format progress bar
        bar_length = 20
        filled = min(bar_length, floor(percent / 5))  # 20 segments for 100%, capped
        bar = "▪️" * filled + "▫️" * (bar_length - filled)

        # Calculate speed over the update interval
        time_diff = self.stats.elapsed_time if last_value == 0 else time.time() - self.stats.last_update_time
        speed = self._calculate_speed(current, last_value, time_diff)
        
        # Format display elements
        current_size = self._format_size(current)
        total_size = self._format_size(total)
        speed_text = self._format_size(speed) + "/s"
        eta = self._calculate_eta(total - current, speed)

        return (
            f"Name: {filename}\n"
            f"{status}: {percent:.1f}%\n"
            f"⟨⟨{bar}⟩⟩\n"
            f"{current_size} of {total_size}\n"
            f"Speed: {speed_text}\n"
            f"ETA: {eta}"
        )

    def _calculate_speed(self, current: float, last: float, time_diff: float) -> float:
        """
        Calculate the processing speed.
        
        Args:
            current: Current progress in MB
            last: Last progress value in MB
            time_diff: Time difference in seconds
            
        Returns:
            float: Speed in MB/s
        """
        if time_diff <= 0 or current < last:
            return 0
        return (current - last) / time_diff

    def _format_size(self, size_mb: float) -> str:
        """
        Format a size in MB to a human-readable string.
        
        Args:
            size_mb: Size in megabytes
            
        Returns:
            str: Formatted size string
        """
        if size_mb >= 1024:
            return f"{size_mb/1024:.2f} GB"
        return f"{size_mb:.2f} MB"

    def _calculate_eta(self, remaining_mb: float, speed_mbs: float) -> str:
        """
        Calculate estimated time remaining.
        
        Args:
            remaining_mb: Remaining size in MB
            speed_mbs: Current speed in MB/s
            
        Returns:
            str: Formatted ETA string
        """
        if speed_mbs <= 0.001:  # Avoid division by zero and unrealistic ETAs
            return "∞"
            
        seconds = remaining_mb / speed_mbs
        
        if seconds > 86400:  # More than a day
            return f"{seconds/86400:.1f} days"
        elif seconds > 3600:
            return f"{seconds/3600:.1f} hours"
        elif seconds > 60:
            return f"{seconds/60:.0f} minutes"
        return f"{seconds:.0f} seconds"
    
    @classmethod
    @asynccontextmanager
    async def track_operation(
        cls,
        message_updater: UpdaterCallable,
        total_size: int,
        action: str = "Processing",
        filename: str = "File",
        **kwargs
    ):
        """
        Context manager for tracking a file operation.
        
        Args:
            message_updater: Callable that updates the progress message
            total_size: Total size in bytes
            action: Action description
            filename: Name of the file being processed
            **kwargs: Additional arguments to pass to ProgressTracker constructor
            
        Yields:
            ProgressTracker: The progress tracker instance
            
        Example:
            ```python
            async with ProgressTracker.track_operation(
                update_message, 
                total_size=file_size, 
                action="Downloading",
                filename="example.zip"
            ) as tracker:
                # Process file in chunks
                bytes_read = 0
                async for chunk in download_file():
                    bytes_read += len(chunk)
                    await tracker.update_progress(bytes_read, total_size)
            ```
        """
        tracker = cls(message_updater, **kwargs)
        tracker.state = ProcessingState.RUNNING
        try:
            yield tracker
            tracker.state = ProcessingState.COMPLETED
            # Final update to show 100%
            await tracker.update_progress(total_size, total_size, action, filename)
        except Exception as e:
            tracker.state = ProcessingState.ERROR
            # Update with error state
            error_message = f"{action} failed: {str(e)}"
            try:
                await tracker.update_progress(
                    tracker.stats.current_bytes, 
                    total_size,
                    f"{action}: ERROR\n{error_message}",
                    filename
                )
            except Exception:
                if tracker.log_errors:
                    logger.exception("Failed to update final error status")
            raise


# Simplified function for quick progress tracking
async def track_progress(
    updater: UpdaterCallable,
    current: int,
    total: int,
    action: str = "Processing",
    filename: str = "File"
) -> None:
    """
    Simple utility function for one-off progress updates.
    
    Args:
        updater: Message update function
        current: Current progress in bytes
        total: Total size in bytes
        action: Action description
        filename: Name of the file
    """
    tracker = ProgressTracker(updater)
    await tracker.update_progress(current, total, action, filename)