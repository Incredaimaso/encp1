from startup import start_aria2c
from bot_manager import BotManager
from queue_manager import QueueItem  # Fix the import
import asyncio
import atexit
import signal
import os
import shutil
from encode import VideoEncoder
from downloaders import Downloader
from uploaders import Uploader
from display import ProgressTracker
from config import Config
from pathlib import Path
from renamer import VideoRenamer

DOWNLOADS_DIR = "downloads"
ENCODES_DIR = "encodes"

class EncodingTracker:
    def __init__(self):
        self.completed_qualities = set()
        self.uploaded_files = set()

    def mark_quality_complete(self, quality: str):
        self.completed_qualities.add(quality)

    def mark_file_uploaded(self, file_path: str):
        self.uploaded_files.add(file_path)

    def is_complete(self, qualities):
        return all(q in self.completed_qualities for q in qualities)

def setup_directories():
    for directory in [DOWNLOADS_DIR, ENCODES_DIR]:
        os.makedirs(directory, exist_ok=True)

def cleanup_directories():
    for directory in [DOWNLOADS_DIR, ENCODES_DIR]:
        if os.path.exists(directory):
            shutil.rmtree(directory)
            os.makedirs(directory)

async def process_queue_item(item: QueueItem):
    retries = 3
    status_message = None
    encoder = None
    
    try:
        for attempt in range(retries):
            try:
                if not status_message:
                    status_message = await item.message.reply_text(
                        f"⏳ Processing task {item.task_id}...\n"
                        f"Use /cancel {item.task_id} to stop this task"
                    )
                
                progress_tracker = ProgressTracker(lambda text: status_message.edit_text(text))
                downloader = Downloader(Config.ARIA2_HOST, Config.ARIA2_PORT, Config.ARIA2_SECRET)

                # Download phase
                downloaded_file = None
                try:
                    if item.is_url:
                        try:
                            await status_message.edit_text("⬇️ Starting download...")
                            downloaded_file, file_size = await downloader.download_aria2(
                                item.file_path,
                                progress_tracker.update_progress,
                                DOWNLOADS_DIR
                            )
                            
                            # Explicit verification
                            if not os.path.exists(downloaded_file):
                                raise Exception("Download verification failed")
                            
                            actual_size = os.path.getsize(downloaded_file) / (1024 * 1024)
                            await status_message.edit_text(
                                f"✅ Download complete!\n"
                                f"📁 File: {os.path.basename(downloaded_file)}\n"
                                f"📦 Size: {actual_size:.1f}MB\n"
                                "🎬 Starting encode..."
                            )
                            
                            if actual_size > 1900:
                                raise Exception("File too large (max: 1.9GB)")

                        except Exception as e:
                            if downloaded_file and os.path.exists(downloaded_file):
                                os.remove(downloaded_file)
                            raise Exception(f"Download failed: {str(e)}")
                    else:
                        downloaded_file = item.file_path
                        actual_size = os.path.getsize(downloaded_file) / (1024 * 1024)

                    # Initialize encoder once
                    if not encoder:
                        encoder = VideoEncoder()
        
                    for quality in Config.QUALITIES:
                        if item.cancel_flag:
                            break

                        try:
                            output_path = os.path.join(
                                ENCODES_DIR,
                                f"{os.path.splitext(os.path.basename(downloaded_file))[0]}_{quality}.mkv"
                            )

                            # Encode video
                            await status_message.edit_text(f"🎬 Starting {quality} encode...")
                            encoded_file, _ = await encoder.encode_video(
                                downloaded_file, 
                                output_path,
                                Config.TARGET_SIZES[quality], 
                                quality,
                                progress_callback=progress_tracker.update_progress
                            )

                            # Verify encoded file
                            if not os.path.exists(encoded_file):
                                raise Exception(f"Encoding failed - file not found: {encoded_file}")

                            encoded_size = os.path.getsize(encoded_file)/(1024*1024)
                            if encoded_size > Config.TARGET_SIZES[quality]:
                                raise Exception(f"Encoded size {encoded_size:.1f}MB exceeds limit for {quality}")

                            # Upload with retries
                            await status_message.edit_text(f"📤 Uploading {quality}...")
                            upload_success = False
                            
                            for upload_attempt in range(3):
                                try:
                                    caption = (
                                        f"🎥 {os.path.splitext(os.path.basename(downloaded_file))[0]}\n"
                                        f"📊 Quality: {quality}\n"
                                        f"📦 Size: {encoded_size:.1f}MB\n"
                                        f"🔄 Reduced: {((actual_size-encoded_size)/actual_size)*100:.1f}%"
                                    )

                                    await Uploader.upload_video(
                                        item.message._client,
                                        item.message.chat.id,
                                        encoded_file,
                                        caption,
                                        progress_callback=progress_tracker.update_progress,
                                        filename=os.path.basename(encoded_file)
                                    )
                                    upload_success = True
                                    await status_message.edit_text(f"✅ {quality} uploaded successfully!")
                                    break

                                except Exception as e:
                                    print(f"Upload attempt {upload_attempt + 1} failed: {e}")
                                    if upload_attempt < 2:
                                        await status_message.edit_text(
                                            f"⚠️ Upload failed, retrying {quality} ({upload_attempt + 2}/3)..."
                                        )
                                        await asyncio.sleep(5)
                                        continue
                                    raise

                            if not upload_success:
                                raise Exception(f"Failed to upload {quality} after 3 attempts")

                        except Exception as e:
                            print(f"Error processing {quality}: {e}")
                            await status_message.edit_text(f"❌ Error with {quality}: {str(e)}")
                            continue
                        finally:
                            # Cleanup encoded file after upload attempt
                            try:
                                if os.path.exists(encoded_file):
                                    os.remove(encoded_file)
                            except Exception as e:
                                print(f"Cleanup error: {e}")

                    await status_message.edit_text("✅ All qualities processed!")
                    return  # Success - exit retry loop

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    raise Exception(f"Processing error: {str(e)}")

            except (ConnectionError, ConnectionResetError) as e:
                print(f"Connection error (attempt {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(5)
                    continue
                raise
            except Exception as e:
                print(f"Process error: {e}")
                if status_message:
                    await status_message.edit_text(
                        f"❌ Error in task {item.task_id}: {str(e)}\n"
                        "Task has been cancelled."
                    )
                raise
            finally:
                # Cleanup
                try:
                    if downloaded_file and os.path.exists(downloaded_file):
                        os.remove(downloaded_file)
                    if 'output_path' in locals() and os.path.exists(output_path):
                        os.remove(output_path)
                except Exception as e:
                    print(f"Cleanup error: {e}")

    except Exception as e:
        print(f"Process error: {e}")
        if status_message:
            await status_message.edit_text(f"❌ Error: {str(e)}")
    finally:
        # Clean up source file
        try:
            if downloaded_file and os.path.exists(downloaded_file):
                os.remove(downloaded_file)
        except Exception as e:
            print(f"Source cleanup error: {e}")

async def main():
    try:
        await start_aria2c()
        setup_directories()
        
        atexit.register(cleanup_directories)
        signal.signal(signal.SIGINT, lambda sig, frame: cleanup_directories())
        
        bot = BotManager(process_queue_item)
        await bot.start()
    finally:
        cleanup_directories()

if __name__ == "__main__":
    asyncio.run(main())
