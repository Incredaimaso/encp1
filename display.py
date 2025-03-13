import time
import os
from typing import Callable
import asyncio
from math import floor

class ProgressTracker:
    def __init__(self, message_updater: Callable):
        self.message_updater = message_updater
        self.start_time = time.time()
        self.last_update = 0
        self.last_processed = 0
        self.update_interval = 2  # Update every 2 seconds

    async def update_progress(self, current: int, total: int, action: str = None):
        try:
            current_time = time.time()
            if current_time - self.last_update < self.update_interval:
                return

            # Calculate progress metrics
            current_mb = current / (1024 * 1024)  # Convert to MB
            total_mb = total / (1024 * 1024)
            speed = self._calculate_speed(current_mb, self.last_processed, current_time)
            progress = (current / total) * 100 if total > 0 else 0
            
            # Generate status text
            if isinstance(action, str) and '\n' in action:
                # Use custom status if provided
                status_text = action
            else:
                status_text = self._format_progress(
                    action or "Processing",
                    "File",
                    current_mb,
                    total_mb,
                    self.last_processed,
                    progress
                )

            await self.message_updater(status_text)
            self.last_update = current_time
            self.last_processed = current_mb

        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                print(f"Progress update error: {e}")

    def _format_progress(self, status: str, filename: str, current: float, 
                        total: float, last_value: float, percent: float) -> str:
        # Format progress bar
        bar_length = 20
        filled = floor(percent / 5)  # 20 segments for 100%
        bar = "▪️" * filled + "▫️" * (bar_length - filled)

        # Format sizes
        current_size = self._format_size(current)
        total_size = self._format_size(total)

        # Calculate speed
        speed = self._calculate_speed(current, last_value, time.time() - self.last_update)
        speed_text = self._format_size(speed) + "/s"

        # Calculate ETA
        eta = self._format_eta(total - current, speed)

        return (
            f"Name: {filename}\n"
            f"{status}: {percent:.1f}%\n"
            f"⟨⟨{bar}⟩⟩\n"
            f"{current_size} of {total_size}\n"
            f"Speed: {speed_text}\n"
            f"ETA: {eta}"
        )

    def _calculate_speed(self, current: float, last: float, time_diff: float) -> float:
        if time_diff == 0:
            return 0
        return (current - last) / time_diff

    def _format_size(self, size_mb: float) -> str:
        if size_mb > 1024:
            return f"{size_mb/1024:.2f} GB"
        return f"{size_mb:.2f} MB"

    def _format_eta(self, remaining_mb: float, speed_mbs: float) -> str:
        if speed_mbs == 0:
            return "∞"
        seconds = remaining_mb / speed_mbs
        if seconds > 3600:
            return f"{seconds/3600:.2f} hours"
        elif seconds > 60:
            return f"{seconds/60:.0f} minutes"
        return f"{seconds:.0f} seconds"
