from pyrogram import Client
from config import Config
import asyncio
from typing import Optional, Union
from pyrogram.types import Message

class BotLogger:
    def __init__(self, client: Client):
        self.client = client
        self.log_channel = Config.LOG_CHANNEL
        self.enabled = Config.ENABLE_LOGS

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
