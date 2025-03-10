from collections import deque
from dataclasses import dataclass
from typing import Any, Deque
import asyncio
import os

@dataclass
class QueueItem:
    user_id: int
    file_path: str
    quality: str
    message: Any
    is_url: bool = False

class QueueManager:
    def __init__(self):
        self.queue: Deque[QueueItem] = deque()
        self.processing = False

    def add_item(self, item: QueueItem):
        self.queue.append(item)

    def get_next(self) -> QueueItem:
        return self.queue.popleft() if self.queue else None

    @property
    def is_empty(self) -> bool:
        return len(self.queue) == 0

    async def process_queue(self, process_func):
        if self.processing:
            return
        
        self.processing = True
        while not self.is_empty:
            item = self.get_next()
            if item:
                try:
                    # Get just the filename from magnet URL
                    if item.is_url and item.file_path.startswith('magnet:'):
                        file_name = item.file_path.split('&dn=')[1].split('&')[0]
                        file_name = file_name.replace('%20', ' ').replace('%5B', '[').replace('%5D', ']')
                    else:
                        file_name = os.path.basename(item.file_path)

                    status_msg = (
                        "⏳ Processing item in queue...\n"
                        f"📁 File: {file_name}\n"
                        "🔄 Status: Starting"
                    )
                    await item.message.reply_text(status_msg)
                    
                    await process_func(item)
                    await asyncio.sleep(5)
                except Exception as e:
                    print(f"Error processing queue item: {e}")
            await asyncio.sleep(1)
        self.processing = False
