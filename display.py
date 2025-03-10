import time
from typing import Callable
import asyncio

class ProgressTracker:
    def __init__(self, message_updater: Callable):
        self.message_updater = message_updater
        self.start_time = time.time()
        self.last_update = 0
        self.last_message = ""
        self.min_update_interval = 2
        self.max_update_interval = 10
        self.update_count = 0

    async def update_progress(self, current: int, total: int, action: str = "Processing"):
        current_time = time.time()
        
        # Check update interval
        if self.update_count < 10:
            update_interval = 0.5
        else:
            update_interval = min(self.max_update_interval, 
                                self.min_update_interval + (self.update_count // 10))

        if current_time - self.last_update < update_interval:
            return

        # Use custom status text if provided with newlines
        if isinstance(action, str) and '\n' in action:
            status_text = action
        else:
            percentage = (current * 100) / total
            speed = current / (time.time() - self.start_time)
            eta = (total - current) / speed if speed > 0 else 0

            status_text = (
                f"{action}...\n"
                f"{self._create_progress_bar(percentage)}\n"
                f"📊 Progress: {percentage:.1f}%\n"
                f"🚀 Speed: {self._format_size(speed)}/s\n"
                f"⏱ ETA: {self._format_time(eta)}"
            )

        # Only update if content changed
        if status_text != self.last_message:
            try:
                await self.message_updater(status_text)
                self.last_message = status_text
                self.last_update = current_time
                self.update_count += 1
            except Exception as e:
                if "MESSAGE_NOT_MODIFIED" not in str(e):
                    raise

    def _create_progress_bar(self, percentage: float) -> str:
        filled = int(percentage / 5)
        return f"[{'█' * filled}{'░' * (20-filled)}]"

    def _format_size(self, size: float) -> str:
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f}{unit}"
            size /= 1024
        return f"{size:.1f}TB"

    def _format_time(self, seconds: float) -> str:
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
