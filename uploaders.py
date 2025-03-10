from pyrogram import Client
import os
import asyncio
from typing import Optional, Callable
from anilist import AniListAPI
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

class Uploader:
    # Upload buffer size (2MB)
    BUFFER_SIZE = 2 * 1024 * 1024

    @staticmethod
    async def upload_video(client: Client, chat_id: int, 
                          video_path: str, caption: str, 
                          progress_callback, filename: str = None,
                          telegraph_url: str = None) -> bool:
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

            # Create inline keyboard if Telegraph URL exists
            reply_markup = None
            if telegraph_url:
                reply_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton(
                        "📋 Detailed MediaInfo",
                        url=telegraph_url
                    )]
                ])

            # Get correct name from AniList
            anilist = AniListAPI()
            current_title = os.path.splitext(os.path.basename(filename))[0]
            anime_data = await anilist.search_anime(os.path.basename(video_path))
            if anime_data:
                proper_title = anime_data.get('title', {}).get('english') or \
                             anime_data.get('title', {}).get('romaji')
                if proper_title:
                    filename = filename.replace(
                        current_title,
                        proper_title.strip()
                    )

            # Get thumbnail
            thumb_path = await anilist.get_thumbnail(
                proper_title or filename,
                os.path.dirname(video_path)
            )

            # Upload with all parameters
            await client.send_document(
                chat_id=chat_id,
                document=video_path,
                caption=caption,
                thumb=thumb_path,
                file_name=filename,
                force_document=True,
                reply_markup=reply_markup,
                progress=progress,
                disable_notification=True
            )

            # Cleanup thumbnail
            if thumb_path and os.path.exists(thumb_path):
                os.remove(thumb_path)

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
