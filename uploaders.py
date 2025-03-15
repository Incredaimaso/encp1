from pyrogram import Client
import os
import asyncio
import time
from typing import Optional, Callable

class Uploader:
    # Increased buffer size for faster uploads (10MB)
    BUFFER_SIZE = 10 * 1024 * 1024

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

            if now - last_progress_update >= 1:  # Reduced frequency for better performance
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
            message = await client.send_document(
                chat_id=chat_id,
                document=video_path,
                caption=caption,
                file_name=filename or os.path.basename(video_path),
                force_document=True,
                progress=progress,
                disable_notification=True,
                file_size=file_size,
                stream=True
            )

            # Verify upload
            if not await Uploader.verify_upload(client, chat_id, message.id, file_size):
                raise Exception("Upload verification failed")

            return True

        except asyncio.CancelledError:
            print("Upload task was cancelled. Performing cleanup...")
            return False

        except Exception as e:
            print(f"Upload error: {str(e)}")
            raise Exception(f"Upload failed: {str(e)}")

        finally:
            print("Cleaning up resources...")
            await asyncio.sleep(1)  # Ensures any pending I/O operations are completed
