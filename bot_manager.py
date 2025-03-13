try:
    from pyrogram import Client, filters, enums  # Add enums import
except ImportError:
    # Alternative import path
    from pyrogram.client import Client
    from pyrogram.filters import filters

import asyncio
import backoff
from config import Config
from handlers import Handlers
from queue_manager import QueueManager
from users import UserManager
from downloaders import Downloader
from encode import VideoEncoder
import time
from pyrogram.errors import FloodWait
import os

class BotManager:
    def __init__(self, process_func):
        self.queue_manager = QueueManager()
        self.user_manager = UserManager()
        self.handlers = Handlers(self.queue_manager, self.user_manager, process_func)
        
        self.app = None
        self.session_active = False
        self.max_retries = 5
        self.retry_delay = 5
        self.retry_count = 0
        self.check_interval = 60  # Check connection every minute
        self.reconnect_delay = 5
        self.max_reconnect_attempts = 5
        self.session_timeout = 60 * 15  # 15 minutes
        self.max_session_retries = 5
        self.session_count = 0
        self.session_lock = asyncio.Lock()
        self.db_timeout = 30  # Database timeout in seconds
        self.session_file = "video_encoder_bot.session"
        self.parse_mode = "markdown2"  # Updated parse mode for newer Pyrogram
    
    def setup_handlers(self):
        # Command handlers
        self.app.on_message(filters.command("start"))(self.handlers.start_handler)
        self.app.on_message(filters.command("help"))(self.handlers.help_handler)
        self.app.on_message(filters.command("add"))(self.handlers.add_user_handler)
        self.app.on_message(filters.command("l"))(self.handlers.download_handler)
        self.app.on_message(filters.command("cancel"))(self.handlers.cancel_handler)
    
    def setup_app(self):
        if not self.app:
            self.app = Client(
                "video_encoder_bot",
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                bot_token=Config.BOT_TOKEN,
                parse_mode=self.parse_mode  # Use markdown2
            )
            self.setup_handlers()

    @backoff.on_exception(
        backoff.expo,
        ConnectionError,
        max_tries=5
    )
    async def _safe_bot_call(self, func):
        try:
            return await func()
        except Exception as e:
            print(f"Bot call error: {e}")
            raise

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, ConnectionResetError),
        max_tries=3,
        jitter=None,
    )
    async def _maintain_connection(self):
        try:
            await self.app.get_me()
            return True
        except Exception as e:
            print(f"Connection check failed: {e}")
            return False

    async def _attempt_reconnect(self):
        print("Attempting to reconnect...")
        for i in range(self.max_reconnect_attempts):
            try:
                await self.app.start()
                return True
            except Exception as e:
                print(f"Reconnection attempt {i+1} failed: {e}")
                await asyncio.sleep(self.reconnect_delay)
        return False

    async def _init_session(self):
        try:
            async with self.session_lock:
                if self.session_active:
                    await self.app.stop()
                    self.session_active = False
                    # Clear session file
                    if os.path.exists(self.session_file):
                        os.remove(self.session_file)
                    await asyncio.sleep(2)
                
                self.setup_app()
                await self.app.start()
                self.session_active = True
                print("📡 Bot session initialized")
                return True
        except Exception as e:
            print(f"❌ Session init error: {e}")
            self.session_active = False
            self.app = None
            # Cleanup session on error
            if os.path.exists(self.session_file):
                os.remove(self.session_file)
            return False

    async def start(self):
        while True:
            try:
                if not await self._init_session():
                    await asyncio.sleep(5)
                    continue

                while self.session_active:
                    try:
                        await asyncio.sleep(self.check_interval)
                        async with self.session_lock:
                            me = await self.app.get_me()
                            if not me:
                                raise ConnectionError("Bot session invalid")
                    except FloodWait as e:
                        print(f"⚠️ Rate limit, waiting {e.value} seconds")
                        await asyncio.sleep(e.value)
                    except Exception as e:
                        print(f"❌ Connection error: {e}")
                        self.session_active = False
                        break

            except Exception as e:
                print(f"❌ Bot error: {str(e)}")
                # Cleanup session file on error
                if os.path.exists(self.session_file):
                    os.remove(self.session_file)
                await asyncio.sleep(5)

            finally:
                if self.app and self.session_active:
                    async with self.session_lock:
                        await self.app.stop()
                        self.session_active = False
                print("🔄 Restarting bot connection...")
                await asyncio.sleep(5)
