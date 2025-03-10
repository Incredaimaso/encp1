try:
    from pyrogram import Client, filters
except ImportError:
    # Alternative import path
    from pyrogram.client import Client
    from pyrogram.filters import filters

import asyncio
from config import Config
from handlers import Handlers
from queue_manager import QueueManager
from users import UserManager
from downloaders import Downloader
from encode import VideoEncoder

class BotManager:
    def __init__(self, process_func):
        self.queue_manager = QueueManager()
        self.user_manager = UserManager()
        self.handlers = Handlers(self.queue_manager, self.user_manager, process_func)
        
        self.app = Client(
            "video_encoder_bot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.BOT_TOKEN
        )
        self.setup_handlers()
        self.max_retries = 3
        self.retry_delay = 5
    
    def setup_handlers(self):
        # Command handlers
        self.app.on_message(filters.command("start"))(self.handlers.start_handler)
        self.app.on_message(filters.command("help"))(self.handlers.help_handler)
        self.app.on_message(filters.command("add"))(self.handlers.add_user_handler)
        self.app.on_message(filters.command("l"))(self.handlers.download_handler)
    
    async def start(self):
        for attempt in range(self.max_retries):
            try:
                async with self.app:
                    print("Bot is starting...")
                    await self.app.send_message(
                        Config.OWNER_ID,
                        "🤖 Bot is Online!\n"
                        f"Owner ID: {Config.OWNER_ID}\n"
                        "Send /help for available commands"
                    )
                    while True:
                        try:
                            await asyncio.sleep(1)
                        except ConnectionResetError:
                            print("Connection reset, reconnecting...")
                            await asyncio.sleep(self.retry_delay)
                            break
            except Exception as e:
                print(f"Bot connection error (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                    continue
                raise
