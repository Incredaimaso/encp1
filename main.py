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
import sys
import psutil

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
                            # Clear previous encoded file if exists
                            if 'encoded_file' in locals() and os.path.exists(encoded_file):
                                os.remove(encoded_file)

                            output_path = os.path.join(
                                ENCODES_DIR,
                                f"{os.path.splitext(os.path.basename(downloaded_file))[0]}_{quality}.mkv"
                            )

                            # Encode single quality
                            await status_message.edit_text(f"🎬 Starting {quality} encode...")
                            encoded_file, encode_info = await encoder.encode_video(
                                downloaded_file, 
                                output_path,
                                Config.TARGET_SIZES[quality], 
                                quality,
                                progress_callback=progress_tracker.update_progress
                            )

                            # Verify encoded file
                            if not encoded_file or not os.path.exists(encoded_file):
                                raise Exception(f"Encoding failed for {quality} - file not found")

                            encoded_size = os.path.getsize(encoded_file)/(1024*1024)

                            # Upload with retries
                            upload_success = False
                            for upload_attempt in range(3):
                                try:
                                    await status_message.edit_text(
                                        f"📤 Uploading {quality} "
                                        f"({upload_attempt + 1}/3)..."
                                    )

                                    reduction = ((actual_size-encoded_size)/actual_size)*100
                                    caption = (
                                        f"🎥 {os.path.splitext(os.path.basename(downloaded_file))[0]}\n"
                                        f"📊 Quality: {quality}\n"
                                        f"📦 Size: {encoded_size:.1f}MB\n"
                                        f"🔄 Reduced: {reduction:.1f}%"
                                    )

                                    if encode_info and encode_info.get('target_exceeded'):
                                        caption += f"\n⚠️ Note: Size exceeded target by {encode_info['size_excess']:.1f}%"

                                    await Uploader.upload_video(
                                        item.message._client,
                                        item.message.chat.id,
                                        encoded_file,
                                        caption,
                                        progress_callback=progress_tracker.update_progress,
                                        filename=os.path.basename(encoded_file)
                                    )

                                    upload_success = True
                                    await status_message.edit_text(
                                        f"✅ {quality} completed and uploaded!"
                                    )
                                    break

                                except Exception as e:
                                    print(f"Upload attempt {upload_attempt + 1} failed: {e}")
                                    if upload_attempt < 2:
                                        await asyncio.sleep(5)
                                        continue
                                    raise

                            if not upload_success:
                                raise Exception(f"Failed to upload {quality} after 3 attempts")

                            # Clean up this quality's encoded file before moving to next
                            try:
                                if os.path.exists(encoded_file):
                                    os.remove(encoded_file)
                            except Exception as e:
                                print(f"Cleanup error for {quality}: {e}")

                        except Exception as e:
                            print(f"Error processing {quality}: {e}")
                            await status_message.edit_text(f"❌ Error with {quality}: {str(e)}")
                            continue

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

def handle_sigterm(signum, frame):
    print("\n🛑 Received shutdown signal, cleaning up...")
    cleanup_directories()
    # Force kill any running ffmpeg processes
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if 'ffmpeg' in proc.name().lower():
                proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    sys.exit(0)

async def main():
    try:
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, handle_sigterm)
        signal.signal(signal.SIGINT, handle_sigterm)
        
        # Make signals work in asyncio
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))

        await start_aria2c()
        setup_directories()
        
        bot = BotManager(process_queue_item)
        await bot.start()
    except Exception as e:
        print(f"Main error: {e}")
    finally:
        await cleanup()

async def shutdown(sig):
    print(f"\n🛑 Received signal {sig.name}, shutting down gracefully...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    cleanup_directories()
    sys.exit(0)

async def cleanup():
    print("🧹 Cleaning up resources...")
    cleanup_directories()
    # Kill any remaining ffmpeg processes
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if 'ffmpeg' in proc.name().lower():
                proc.kill()
        except:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Received Ctrl+C")
    finally:
        asyncio.run(cleanup())
