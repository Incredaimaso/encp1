from pyrogram import Client
import os
import asyncio
from typing import Optional, Callable
from anilist import AniListAPI
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import time

class Uploader:
    # Upload buffer size (2MB)
    BUFFER_SIZE = 2 * 1024 * 1024

    @staticmethod
    async def _retry_upload(func, *args, max_retries=3, **kwargs):
        last_error = None
        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(5)
        raise last_error

    @staticmethod
    async def upload_video(client: Client, chat_id: int, 
                          video_path: str, caption: str, 
                          progress_callback, filename: str = None) -> bool:
        if not os.path.exists(video_path):
            raise Exception("Upload file not found")

        file_size = os.path.getsize(video_path)
        if file_size == 0:
            raise Exception("Upload file is empty")

        last_progress_update = time.time()
        upload_start_time = time.time()

        async def progress(current: int, total: int):
            nonlocal last_progress_update
            now = time.time()
            
            if now - last_progress_update >= 1:
                elapsed = now - upload_start_time
                speed = current / elapsed if elapsed > 0 else 0
                eta = (total - current) / speed if speed > 0 else 0
                
                await progress_callback(current, total,
                    f"📤 Uploading file...\n"
                    f"📊 Progress: {(current/total)*100:.1f}%\n"
                    f"📦 Size: {current/(1024*1024):.1f}MB / {total/(1024*1024):.1f}MB\n"
                    f"⚡ Speed: {speed/(1024*1024):.2f} MB/s\n"
                    f"⏱️ ETA: {int(eta/60)}m {int(eta%60)}s"
                )
                last_progress_update = now

        try:
            await client.send_document(
                chat_id=chat_id,
                document=video_path,
                caption=caption,
                file_name=filename or os.path.basename(video_path),
                force_document=True,
                progress=progress,
                disable_notification=True
            )
            return True
        except Exception as e:
            print(f"Upload error: {str(e)}")
            raise Exception(f"Upload failed: {str(e)}")

    @staticmethod
    async def _upload_single(client: Client, chat_id: int, 
                          video_path: str, caption: str, 
                          progress_callback, filename: str = None) -> bool:
        try:
            file_size = os.path.getsize(video_path)
            last_update_time = [0]

            async def progress(current: int, total: int):
                now = asyncio.get_event_loop().time()
                if now - last_update_time[0] < 0.5:
                    return
                last_update_time[0] = now
                
                speed = current / (now - progress.start_time)
                eta = (total - current) / speed if speed > 0 else 0
                
                await progress_callback(current, total,
                    f"📤 Uploading: {os.path.basename(video_path)}\n"
                    f"📊 Progress: {(current/total)*100:.1f}%\n"
                    f"📦 Size: {current/(1024*1024):.1f}MB / {file_size/(1024*1024):.1f}MB\n"
                    f"🚀 Speed: {speed/(1024*1024):.2f} MB/s\n"
                    f"⏱️ ETA: {int(eta/60)}m {int(eta%60)}s"
                )

            progress.start_time = asyncio.get_event_loop().time()

            # Upload with basic parameters
            await client.send_document(
                chat_id=chat_id,
                document=video_path,
                caption=caption,
                file_name=filename,
                force_document=True,
                progress=progress,
                disable_notification=True
            )

            return True
            
        except Exception as e:
            print(f"Upload error: {str(e)}")
            raise Exception(f"Upload failed: {str(e)}")

    @staticmethod
    def generate_caption(original_name: str, quality: str, 
                        original_size: float, new_size: float) -> str:
        quality_info = {
            '480p': '480p (SD)',
            '720p': '720p (HD)',
            '1080p': '1080p (FHD)'
        }
        
        return (
            f"✅ Encoded: {original_name}\n"
            f"📊 Quality: {quality_info.get(quality, quality)}\n"
            f"📦 Size: {original_size:.1f}MB ➡️ {new_size:.1f}MB\n"
            f"🎯 Reduction: {((original_size-new_size)/original_size)*100:.1f}%"
        )
