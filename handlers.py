from pyrogram import Client, filters
from pyrogram.errors import (
    FloodWait, MessageNotModified, UserIsBlocked, 
    ChatWriteForbidden, MediaEmpty, WebpageCurlFailed
)
from config import Config
from queue_manager import QueueItem, QueueManager
from users import UserManager
from display import ProgressTracker
import os
import asyncio
import re
from typing import Optional, Tuple, Dict, Any
from functools import wraps
from logger import BotLogger
from urllib.parse import urlparse
import time
import traceback

# Custom exception classes for better error handling
class BotPermissionError(Exception):
    """Raised when a user attempts an unauthorized operation."""
    pass

class InvalidInputError(Exception):
    """Raised when user provides invalid input to a command."""
    pass

class ProcessingError(Exception):
    """Raised when an error occurs during media processing."""
    pass

class NetworkError(Exception):
    """Raised when network-related issues occur."""
    pass

class Handlers:
    """
    Manages all command and message handlers for the Telegram bot.
    Implements comprehensive error handling and request validation.
    """
    
    def __init__(self, queue_manager: QueueManager, user_manager: UserManager, process_func, logger: BotLogger):
        self.queue_manager = queue_manager
        self.user_manager = user_manager
        self.process_func = process_func
        self.logger = logger
        self.command_cooldowns: Dict[int, Dict[str, float]] = {}  # User ID -> {command: last_used_timestamp}
        self.COOLDOWN_SECONDS = 3  # Prevent command spam
        
        # Initialize queue processing task
        self._queue_task = None
    
    def _check_cooldown(self, user_id: int, command: str) -> bool:
        """
        Check if a user is on cooldown for a specific command.
        Returns True if user can proceed, False if on cooldown.
        """
        current_time = time.time()
        if user_id not in self.command_cooldowns:
            self.command_cooldowns[user_id] = {}
        
        if command in self.command_cooldowns[user_id]:
            last_used = self.command_cooldowns[user_id][command]
            if current_time - last_used < self.COOLDOWN_SECONDS:
                return False
        
        # Update timestamp
        self.command_cooldowns[user_id][command] = current_time
        return True
    
    def start_queue_processing(self):
        """Start background queue processing if not already running."""
        if self._queue_task is None or self._queue_task.done():
            self._queue_task = asyncio.create_task(
                self.queue_manager.process_queue(self.process_func)
            )
            self.logger.info("Queue processing task started")
    
    async def handle_pyrogram_errors(self, func):
        """Decorator to handle common Pyrogram errors."""
        @wraps(func)
        async def wrapper(client, message, *args, **kwargs):
            try:
                return await func(client, message, *args, **kwargs)
            except FloodWait as e:
                self.logger.warning(f"Rate limit hit. Waiting {e.x} seconds")
                await asyncio.sleep(e.x)
                return await func(client, message, *args, **kwargs)
            except MessageNotModified:
                self.logger.debug("Message not modified - ignoring error")
                return None
            except UserIsBlocked:
                self.logger.info(f"User {message.from_user.id} has blocked the bot")
                return None
            except ChatWriteForbidden:
                self.logger.error(f"Bot doesn't have permission to write in chat with {message.from_user.id}")
                return None
            except Exception as e:
                error_msg = f"Unexpected error in {func.__name__}: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                try:
                    await message.reply_text(f"❌ Internal error occurred. Please try again later.")
                except Exception:
                    pass
                return None
        return wrapper
    
    async def validate_user(self, message, admin_only=False):
        """
        Validate if a user is authorized to use the bot or specific admin commands.
        Raises BotPermissionError if unauthorized.
        """
        user_id = message.from_user.id
        
        if admin_only and user_id != Config.OWNER_ID:
            self.logger.warning(f"Unauthorized admin access attempt by user {user_id}")
            raise BotPermissionError("Only the bot owner can perform this action")
            
        if not self.user_manager.is_approved(user_id, Config.OWNER_ID):
            self.logger.info(f"Unauthorized access attempt by user {user_id}")
            raise BotPermissionError("You are not approved to use this bot")
    
    async def start_handler(self, client, message):
        """Handle the /start command."""
        try:
            if not self._check_cooldown(message.from_user.id, "start"):
                return
                
            user_info = f"{message.from_user.first_name} (ID: {message.from_user.id})"
            self.logger.info(f"New user interaction: {user_info}")
            
            await message.reply_text(
                f"Welcome to Video Encoder Bot, {message.from_user.first_name}!\n\n"
                "📤 Send me a video file to encode it\n"
                "🔗 Use /l to download and encode from a URL\n"
                "ℹ️ Use /help for detailed instructions"
            )
        except Exception as e:
            self.logger.error(f"Error in start_handler: {str(e)}", exc_info=True)
            await message.reply_text("❌ An error occurred. Please try again later.")

    async def help_handler(self, client, message):
        """Handle the /help command with detailed instructions."""
        try:
            if not self._check_cooldown(message.from_user.id, "help"):
                return
                
            help_text = (
                "📋 **Available Commands:**\n\n"
                "🎬 **Video Processing:**\n"
                "• Send any video file to encode it\n"
                "• `/l <url>` - Download and encode from URL\n\n"
                "⚙️ **Management:**\n"
                "• `/status` - Check queue status\n"
                "• `/cancel <task_id>` - Cancel a queued task\n\n"
                "👥 **User Management:**\n"
                "• `/add <user_id>` - Add approved user (owner only)\n\n"
                "📊 **Supported URL Types:**\n"
                "• Direct HTTP/HTTPS links\n"
                "• Magnet links\n\n"
                "❓ For more assistance, contact @{}\n"
                "⚠️ Note: Large files may take time to process"
            ).format(Config.SUPPORT_USERNAME if hasattr(Config, 'SUPPORT_USERNAME') else "BotOwner")
            
            await message.reply_text(help_text)
        except Exception as e:
            self.logger.error(f"Error in help_handler: {str(e)}", exc_info=True)
            await message.reply_text("❌ An error occurred. Please try again later.")

    async def add_user_handler(self, client, message):
        """Handle the /add command to add authorized users."""
        try:
            if not self._check_cooldown(message.from_user.id, "add"):
                return
                
            await self.validate_user(message, admin_only=True)
            
            args = message.text.split()
            if len(args) != 2:
                raise InvalidInputError("Please provide a valid user ID")
                
            try:
                user_id = int(args[1])
            except ValueError:
                raise InvalidInputError("User ID must be a number")
                
            if user_id == Config.OWNER_ID:
                await message.reply_text("⚠️ Owner is already approved!")
                return
                
            if self.user_manager.add_user(user_id):
                self.logger.info(f"User {user_id} added by owner {message.from_user.id}")
                await message.reply_text(f"✅ User {user_id} added successfully!")
            else:
                await message.reply_text("⚠️ User already approved!")
                
        except BotPermissionError as e:
            await message.reply_text(f"⚠️ {str(e)}")
        except InvalidInputError as e:
            await message.reply_text(f"❌ {str(e)}\nUsage: /add <user_id>")
        except Exception as e:
            self.logger.error(f"Error in add_user_handler: {str(e)}", exc_info=True)
            await message.reply_text("❌ An internal error occurred")

    def _validate_url(self, url: str) -> Tuple[bool, str, Optional[str]]:
        """
        Validate URL format and type.
        Returns (is_valid, url_type, error_message)
        """
        url = url.strip()
        
        if url.startswith('magnet:?'):
            return True, 'magnet', None
            
        if any(url.startswith(p) for p in ['http://', 'https://']):
            try:
                parsed = urlparse(url)
                if not parsed.netloc:
                    return False, '', "Invalid URL format"
                return True, 'direct', None
            except Exception:
                return False, '', "Could not parse URL"
                
        return False, '', "URL must be a direct link or magnet"

    async def download_handler(self, client, message):
        """Handle the /l command to download and encode from URL."""
        status_message = None
        
        try:
            if not self._check_cooldown(message.from_user.id, "download"):
                return
                
            await self.validate_user(message)
            
            args = message.text.split(maxsplit=1)
            if len(args) < 2:
                raise InvalidInputError("Please provide a URL")
                
            url = args[1].strip()
            is_valid, url_type, error = self._validate_url(url)
            
            if not is_valid:
                raise InvalidInputError(error or "Invalid URL format")
                
            status_message = await message.reply_text("⏳ Validating URL and preparing download...")
            
            # Create queue item
            item = QueueItem(
                user_id=message.from_user.id,
                file_path=url,
                quality=None,
                message=message,
                status_message=status_message,
                is_url=True,
                url_type=url_type
            )
            
            task_id = self.queue_manager.add_item(item)
            self.logger.info(f"Added download task {task_id} for user {message.from_user.id}: {url_type} link")
            
            # Start queue processing in background
            self.start_queue_processing()
            
            # Update message with queue info
            position = self.queue_manager.get_position(task_id)
            queue_info = f"(#{position} in queue)" if position > 1 else "(processing next)"
            
            await status_message.edit_text(
                f"✅ Download queued! {queue_info}\n"
                f"🆔 Task ID: `{task_id}`\n"
                f"💡 Type: {url_type.title()} Link\n"
                f"📝 Use `/cancel {task_id}` to cancel"
            )
            
        except BotPermissionError as e:
            await message.reply_text(f"⚠️ {str(e)}")
        except InvalidInputError as e:
            await message.reply_text(f"❌ {str(e)}\nUsage: /l <url>")
        except Exception as e:
            error_details = traceback.format_exc()
            self.logger.error(f"Error in download_handler: {error_details}")
            
            error_message = f"❌ Download error: {str(e)}"
            if status_message:
                await status_message.edit_text(error_message)
            else:
                await message.reply_text(error_message)

    async def cancel_handler(self, client, message):
        """Handle the /cancel command to cancel a queued task."""
        try:
            if not self._check_cooldown(message.from_user.id, "cancel"):
                return
                
            await self.validate_user(message)
            
            args = message.text.split(None, 1)
            if len(args) < 2:
                raise InvalidInputError("Please provide a task ID")
                
            task_id = args[1].strip()
            
            # Verify the task belongs to this user or is admin
            task_info = self.queue_manager.get_task_info(task_id)
            if task_info and task_info.user_id != message.from_user.id and message.from_user.id != Config.OWNER_ID:
                raise BotPermissionError("You can only cancel your own tasks")
            
            result = await self.queue_manager.cancel_task(task_id)
            if result:
                self.logger.info(f"Task {task_id} cancellation initiated by user {message.from_user.id}")
                await message.reply_text(
                    f"✅ Task {task_id} cancellation initiated\n"
                    "⏳ Please wait for current operation to complete..."
                )
            else:
                await message.reply_text(
                    f"❌ Task {task_id} not found or already completed!"
                )
                
        except BotPermissionError as e:
            await message.reply_text(f"⚠️ {str(e)}")
        except InvalidInputError as e:
            await message.reply_text(f"❌ {str(e)}\nUsage: /cancel <task_id>")
        except Exception as e:
            self.logger.error(f"Error in cancel_handler: {str(e)}", exc_info=True)
            await message.reply_text(f"❌ Error: `{str(e)}`")
            
    async def status_handler(self, client, message):
        """Handle the /status command to show queue status."""
        try:
            if not self._check_cooldown(message.from_user.id, "status"):
                return
                
            await self.validate_user(message)
            
            stats = self.queue_manager.get_queue_stats()
            user_tasks = self.queue_manager.get_user_tasks(message.from_user.id)
            
            active_task = "None" if not stats['active_task'] else f"`{stats['active_task']}`"
            
            status_text = (
                "📊 **Queue Status:**\n\n"
                f"🔄 Active Task: {active_task}\n"
                f"📑 Queue Length: {stats['queue_length']}\n"
                f"✅ Completed Today: {stats['completed_today']}\n\n"
            )
            
            if user_tasks:
                status_text += "**Your Tasks:**\n"
                for task_id, position in user_tasks.items():
                    status = "🔄 Processing" if position == 0 else f"⏳ #{position} in queue"
                    status_text += f"• `{task_id}`: {status}\n"
            else:
                status_text += "You have no active tasks in the queue."
                
            await message.reply_text(status_text)
            
        except BotPermissionError as e:
            await message.reply_text(f"⚠️ {str(e)}")
        except Exception as e:
            self.logger.error(f"Error in status_handler: {str(e)}", exc_info=True)
            await message.reply_text("❌ An error occurred while fetching queue status")
            
    async def handle_media(self, client, message):
        """Process media files sent directly to the bot."""
        try:
            await self.validate_user(message)
            
            # Check if message contains a video
            if not message.video and not message.document:
                await message.reply_text(
                    "⚠️ Please send a video file or use /l command for URL downloads."
                )
                return
                
            # Get file details
            media = message.video or message.document
            file_name = media.file_name if hasattr(media, 'file_name') else f"video_{message.from_user.id}_{int(time.time())}"
            file_size = media.file_size
            
            # Check file size limit if configured
            if hasattr(Config, 'MAX_FILE_SIZE') and file_size > Config.MAX_FILE_SIZE:
                max_size_mb = Config.MAX_FILE_SIZE / (1024 * 1024)
                actual_size_mb = file_size / (1024 * 1024)
                await message.reply_text(
                    f"❌ File too large: {actual_size_mb:.1f}MB\n"
                    f"Maximum allowed: {max_size_mb:.1f}MB"
                )
                return
                
            # Reply with initial status
            status_message = await message.reply_text(
                f"📝 File: `{file_name}`\n"
                f"💾 Size: {file_size / (1024 * 1024):.1f}MB\n"
                "⏳ Adding to processing queue..."
            )
            
            # Create queue item
            item = QueueItem(
                user_id=message.from_user.id,
                message=message,
                status_message=status_message,
                file_path=None,  # Will be set during download
                quality=None,
                is_url=False
            )
            
            task_id = self.queue_manager.add_item(item)
            self.logger.info(f"Added media task {task_id} for user {message.from_user.id}: {file_name}")
            
            # Start queue processing
            self.start_queue_processing()
            
            # Update message with queue info
            position = self.queue_manager.get_position(task_id)
            queue_info = f"(#{position} in queue)" if position > 1 else "(processing next)"
            
            await status_message.edit_text(
                f"✅ File queued! {queue_info}\n"
                f"📝 File: `{file_name}`\n"
                f"💾 Size: {file_size / (1024 * 1024):.1f}MB\n"
                f"🆔 Task ID: `{task_id}`\n"
                f"📝 Use `/cancel {task_id}` to cancel"
            )
            
        except BotPermissionError as e:
            await message.reply_text(f"⚠️ {str(e)}")
        except Exception as e:
            self.logger.error(f"Error in handle_media: {str(e)}", exc_info=True)
            await message.reply_text(f"❌ Error processing media: {str(e)}")
            
    def register_handlers(self, app: Client):
        """Register all handlers with the Pyrogram client."""
        app.add_handler(filters.command(["start"]), self.start_handler)
        app.add_handler(filters.command(["help"]), self.help_handler)
        app.add_handler(filters.command(["add"]), self.add_user_handler)
        app.add_handler(filters.command(["l"]), self.download_handler)
        app.add_handler(filters.command(["cancel"]), self.cancel_handler)
        app.add_handler(filters.command(["status"]), self.status_handler)
        app.add_handler(filters.media, self.handle_media)
        
        self.logger.info("All handlers registered successfully")