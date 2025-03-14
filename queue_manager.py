from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque
import asyncio
import os
import backoff
from time import sleep
import uuid
import time
from logger import BotLogger

@dataclass
class QueueItem:
    user_id: int
    file_path: str
    quality: str
    message: Any
    is_url: bool = False
    task_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    status: str = "queued"
    cancel_flag: bool = False

class QueueManager:
    def __init__(self):
        self.queue: Deque[QueueItem] = deque()
        self.processing = False
        self.max_retries = 5
        self.retry_delay = 5
        self.connection_retries = 3
        self.connection_backoff = 5
        self.max_consecutive_failures = 3
        self.failure_count = 0
        self.backoff_time = 30
        self.connection_timeout = 30
        self.max_retry_backoff = 300  # 5 minutes
        self.operation_timeout = 7200  # Increase timeout to 2 hours
        self.max_item_retries = 3
        self.active_tasks = {}  # task_id -> QueueItem
        self.progress_check_interval = 30  # Check progress every 30 seconds

    async def process_queue(self, process_func):
        if self.processing:
            return

        self.processing = True
        while not self.is_empty:
            item = self.get_next()
            if not item:
                continue

            try:
                logger = BotLogger(item.message._client)
            
                # Log initial queue status
                log_msg = await logger.log_task_start(
                    item.task_id,
                    {
                        'mention': item.message.from_user.mention,
                        'chat_title': getattr(item.message.chat, 'title', None),
                        'filename': os.path.basename(item.file_path)
                    }
                )

                # Process with enhanced progress tracking
                async def progress_wrapper(current, total, status_text):
                    progress = {
                        'current': current,
                        'total': total,
                        'percent': (current/total)*100,
                        'speed': self._calculate_speed(current, item.task_id),
                        'eta': self._estimate_eta(current, total, item.task_id)
                    }
                    
                    await logger.update_task_progress(
                        item.task_id,
                        status_text,
                        progress
                    )
            except Exception as e:
                print(f"Error initializing logger: {e}")
                raise
    
                    # Process with logging
                try:
                    result = await process_func(item, progress_callback=progress_wrapper)
                    
                    # Forward encoded file to logs if enabled
                    if Config.FORWARD_ENCODED and hasattr(result, 'message_id'):
                        await logger.forward_message(result)
                        
                    await logger.update_task_progress(item.task_id, "✅ Completed")
                    
                except Exception as e:
                    await logger.update_task_progress(
                        item.task_id,
                        f"❌ Failed: {str(e)}"
                    )
                    raise


    def add_item(self, item: QueueItem):
        self.queue.append(item)
        self.active_tasks[item.task_id] = item
        return item.task_id

    def get_next(self) -> QueueItem:
        return self.queue.popleft() if self.queue else None

    @property
    def is_empty(self) -> bool:
        return len(self.queue) == 0

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, ConnectionResetError),
        max_tries=5,
        jitter=None
    )
    async def _safe_process(self, item, process_func):
        try:
            await process_func(item)
        except (ConnectionError, ConnectionResetError) as e:
            print(f"Connection error in queue processing: {e}")
            await asyncio.sleep(self.retry_delay)
            raise
        except Exception as e:
            print(f"Error processing item: {e}")
            raise

    async def _process_with_recovery(self, item, process_func):
        for retry in range(self.max_retries):
            try:
                if retry > 0:
                    await asyncio.sleep(self.connection_backoff * retry)
                    await item.message.reply_text(
                        f"🔄 Retry attempt {retry + 1}/{self.max_retries}..."
                    )
                return await process_func(item)
            except ConnectionError as e:
                print(f"Connection error (attempt {retry + 1}): {e}")
                if retry == self.max_retries - 1:
                    raise
            except Exception as e:
                raise

    async def _handle_connection_error(self, e: Exception):
        self.failure_count += 1
        if self.failure_count >= self.max_consecutive_failures:
            await asyncio.sleep(self.backoff_time)
            self.failure_count = 0
        else:
            await asyncio.sleep(self.retry_delay)

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a running task by its ID"""
        if task_id in self.active_tasks:
            item = self.active_tasks[task_id]
            item.cancel_flag = True
            # Update status message if available
            try:
                if hasattr(item, 'status_message'):
                    await item.status_message.edit_text(
                        f"🛑 Cancelling task {task_id}...\n"
                        f"📁 File: {self._get_display_name(item)}"
                    )
            except Exception as e:
                print(f"Error updating cancel message: {e}")
            return True
        return False

    async def process_queue(self, process_func):
        if self.processing:
            return
        
        self.processing = True
        while not self.is_empty:
            item = self.get_next()
            if not item:
                continue

            try:
                # Initial status message
                status_msg = await item.message.reply_text(
                    f"⏳ Processing task `{item.task_id}`...\n"
                    f"📁 File: `{self._get_display_name(item)}`\n"
                    f"💡 Use `/cancel {item.task_id}` to stop this task"
                )
                
                item.status_message = status_msg  # Store for updating

                # Process with timeout and cancellation check
                try:
                    async with asyncio.timeout(self.operation_timeout):
                        last_progress_check = time.time()
                        last_progress_size = 0

                        while not item.cancel_flag:
                            try:
                                # Start processing
                                process_task = asyncio.create_task(process_func(item))
                                
                                # Monitor progress
                                while not process_task.done():
                                    await asyncio.sleep(self.progress_check_interval)
                                    current_time = time.time()
                                    
                                    # Check for progress
                                    if current_time - last_progress_check >= self.progress_check_interval:
                                        if hasattr(item, 'current_size'):
                                            if item.current_size == last_progress_size:
                                                print("Warning: No progress detected")
                                            last_progress_size = item.current_size
                                        last_progress_check = current_time

                                await process_task
                                break

                            except asyncio.TimeoutError:
                                print(f"⚠️ Operation timed out, but encoding might still be progressing...")
                                continue
                            except Exception as e:
                                if "Connection" in str(e):
                                    await self._handle_connection_error(e)
                                    continue
                                raise

                except asyncio.TimeoutError:
                    await status_msg.edit_text(
                        f"⚠️ Task {item.task_id} timed out after {self.operation_timeout}s"
                    )

                if item.cancel_flag:
                    await status_msg.edit_text(
                        f"❌ Task {item.task_id} cancelled!\n"
                        f"📁 File: {self._get_display_name(item)}"
                    )

            except Exception as e:
                print(f"Queue item error: {e}")
                continue
            finally:
                if item.task_id in self.active_tasks:
                    del self.active_tasks[item.task_id]

        self.processing = False

    def _get_display_name(self, item: QueueItem) -> str:
        if item.is_url and item.file_path.startswith('magnet:'):
            try:
                name = item.file_path.split('&dn=')[1].split('&')[0]
                return name.replace('%20', ' ').replace('%5B', '[').replace('%5D', ']')
            except:
                return "Magnet link"
        return os.path.basename(item.file_path)
