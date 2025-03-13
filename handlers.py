from pyrogram import Client, filters
from config import Config
from queue_manager import QueueItem, QueueManager
from users import UserManager
from display import ProgressTracker
import os

class Handlers:
    def __init__(self, queue_manager: QueueManager, user_manager: UserManager, process_func):
        self.queue_manager = queue_manager
        self.user_manager = user_manager
        self.process_func = process_func

    async def start_handler(self, client, message):
        await message.reply_text(
            "**Welcome to Video Encoder Bot\\!**\n"  # Escaped markdown2
            "Send me a video or use `/l` to download and encode\\.\n"
            "Use `/help` for more information\\."
        )

    async def help_handler(self, client, message):
        await message.reply_text(
            "**Available Commands:**\n"
            "`/l <url>` \\- Download and encode video\n"
            "`/add <user_id>` \\- Add approved user \\(owner only\\)\n"
            "Send video file to encode directly"
        )

    async def add_user_handler(self, client, message):
        if message.from_user.id != Config.OWNER_ID:
            await message.reply_text("⚠️ Only owner can add users\\!")
            return

        try:
            user_id = int(message.text.split()[1])
            if self.user_manager.add_user(user_id):
                await message.reply_text(f"✅ User `{user_id}` added successfully\\!")
            else:
                await message.reply_text("⚠️ User already approved\\!")
        except:
            await message.reply_text("❌ Invalid user ID\\!")

    async def download_handler(self, client, message):
        if not self.user_manager.is_approved(message.from_user.id, Config.OWNER_ID):
            await message.reply_text("⚠️ You are not approved to use this bot\\!")
            return

        try:
            if len(message.text.split()) < 2:
                await message.reply_text("❌ Please provide a URL\\!")
                return
                
            url = message.text.split(None, 1)[1].strip()
            if not url.startswith(('http://', 'https://', 'magnet:')):
                await message.reply_text("❌ Invalid URL format\\!")
                return

            self.queue_manager.add_item(QueueItem(
                message.from_user.id,
                url,
                None,
                message,
                True
            ))
            await message.reply_text("✅ Added to queue\\!")
            await self.queue_manager.process_queue(self.process_func)
            
        except Exception as e:
            await message.reply_text(f"❌ Error: `{str(e)}`")

    async def cancel_handler(self, client, message):
        if not self.user_manager.is_approved(message.from_user.id, Config.OWNER_ID):
            await message.reply_text("⚠️ You are not approved to use this bot\\!")
            return

        try:
            task_id = message.text.split(None, 1)[1].strip()
            if await self.queue_manager.cancel_task(task_id):
                await message.reply_text(
                    f"✅ Task `{task_id}` cancellation initiated\n"
                    "Please wait for current operation to complete\\..."
                )
            else:
                await message.reply_text(
                    f"❌ Task `{task_id}` not found or already completed\\!"
                )
        except IndexError:
            await message.reply_text(
                "❌ Please provide a task ID\\!\n"
                "Usage: `/cancel task_id`"
            )
        except Exception as e:
            await message.reply_text(f"❌ Error: `{str(e)}`")
