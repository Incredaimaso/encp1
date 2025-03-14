from pyrogram import Client
from config import Config
import asyncio
import time
from typing import Optional, Union
from pyrogram.types import Message
from datetime import datetime

class BotLogger:
    def __init__(self, client: Client):
        self.client = client
        self.log_channel = Config.LOG_CHANNEL
        self.enabled = Config.ENABLE_LOGS
        self.task_messages = {}  # Track message IDs per task
        self.task_start_times = {}  # Track task durations

    async def log_task_start(self, task_id: str, user_info: dict) -> Optional[Message]:
        """Initialize task log in channel"""
        try:
            text = (
                "🆕 New Task Added to Queue\n\n"
                f"🆔 Task ID: `{task_id}`\n"
                f"👤 User: {user_info['mention']}\n"
                f"💬 Chat: {user_info.get('chat_title', 'Private')}\n"
                f"📁 File: `{user_info.get('filename', 'N/A')}`\n"
                f"⏱️ Added: {self._get_current_time()}\n"
                "➖➖➖➖➖➖➖➖➖➖➖➖\n"
                "⏳ Status: Queued"
            )
            msg = await self.log_message(text)
            if msg:
                self.task_messages[task_id] = msg.id
                self.task_start_times[task_id] = time.time()
            return msg
        except Exception as e:
            print(f"Log start error: {e}")
            return None
        
    async def update_task_progress(self, task_id: str, status: str, progress: dict = None):
        """Update task progress in log channel"""
        if task_id not in self.task_messages:
            return

        try:
            duration = time.time() - self.task_start_times[task_id]
            progress_text = self._format_progress(progress) if progress else ""
            
            text = (
                f"⚡ Task: `{task_id}`\n"
                f"⏱️ Duration: {self._format_duration(duration)}\n"
                f"📊 Status: {status}\n"
                f"{progress_text}\n"
                "➖➖➖➖➖➖➖➖➖➖➖➖"
            )
            
            await self.edit_message(self.task_messages[task_id], text)
        except Exception as e:
            print(f"Progress update error: {e}")

    def _format_progress(self, progress: dict) -> str:
        """Format progress details with better styling"""
        bars = "▰" * int(progress['percent']/10) + "▱" * (10-int(progress['percent']/10))
        return (
            f"\n📈 Progress Details:\n"
            f"├─⚡ Speed: {progress['speed']:.2f} MB/s\n"
            f"├─📊 Progress: [{bars}] {progress['percent']:.1f}%\n"
            f"├─📦 Size: {progress['current']:.1f}/{progress['total']:.1f} MB\n"
            f"└─⏳ ETA: {progress['eta']}"
        )
    
    def _get_current_time(self) -> str:
        """Get formatted current time"""
        return datetime.now().strftime("%I:%M:%S %p")


    async def log_message(self, text: str, reply_to: Optional[int] = None) -> Optional[Message]:
        """Send log message to channel"""
        if not self.enabled or not self.log_channel:
            return None
        
        try:
            return await self.client.send_message(
                chat_id=self.log_channel,
                text=text,
                reply_to_message_id=reply_to,
                disable_web_page_preview=True
            )
        except Exception as e:
            print(f"Logging error: {e}")
            return None

    async def forward_message(self, message: Message) -> Optional[Message]:
        """Forward message to log channel"""
        if not self.enabled or not self.log_channel:
            return None
            
        try:
            return await message.forward(self.log_channel)
        except Exception as e:
            print(f"Forward error: {e}")
            return None

    async def log_file(self, file_path: str, caption: str) -> Optional[Message]:
        """Send file to log channel"""
        if not self.enabled or not self.log_channel:
            return None
            
        try:
            return await self.client.send_document(
                chat_id=self.log_channel,
                document=file_path,
                caption=caption
            )
        except Exception as e:
            print(f"File log error: {e}")
            return None

    async def log_status(self, text: str, edit_message_id: Optional[int] = None) -> Optional[Message]:
        """Send or edit status message in log channel"""
        if not self.enabled or not self.log_channel:
            return None
            
        try:
            if edit_message_id:
                return await self.client.edit_message_text(
                    chat_id=self.log_channel,
                    message_id=edit_message_id,
                    text=text
                )
            return await self.log_message(text)
        except Exception as e:
            print(f"Status log error: {e}")
            return None
