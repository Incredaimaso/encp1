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
from mediainfo import MediaInfoGenerator

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
    status_message = await item.message.reply_text("⏳ Processing...")
    progress_tracker = ProgressTracker(lambda text: status_message.edit_text(text))
    encoder = VideoEncoder()
    downloader = Downloader(Config.ARIA2_HOST, Config.ARIA2_PORT, Config.ARIA2_SECRET)
    renamer = VideoRenamer()
    media_info = MediaInfoGenerator()
    downloaded_file = None
    encoded_files = {}
    encoding_status = {q: "⏳ Pending" for q in Config.QUALITIES}

    try:
        # Download phase
        if item.is_url:
            await status_message.edit_text("⬇️ Starting download...")
            downloaded_file, file_size = await downloader.download_aria2(
                item.file_path,
                progress_tracker.update_progress,
                DOWNLOADS_DIR
            )
            print(f"Downloaded file: {downloaded_file} ({file_size:.1f}MB)")
            
            if file_size > 1500:
                raise Exception("File too large (max: 1.5GB)")
                
            await status_message.edit_text(
                "🎬 Encoding Queue:\n" + 
                "\n".join(f"{q}: {encoding_status[q]}" for q in Config.QUALITIES)
            )
        else:
            downloaded_file = item.file_path
            file_size = os.path.getsize(downloaded_file) / (1024 * 1024)

        # Process each quality
        for quality in Config.QUALITIES:
            try:
                encoding_status[quality] = "🔄 Processing"
                await status_message.edit_text(
                    "🎬 Encoding Status:\n" + 
                    "\n".join(f"{q}: {encoding_status[q]}" for q in Config.QUALITIES)
                )

                # Use cached source for higher qualities
                input_file = downloaded_file
                if quality in ['720p', '1080p'] and '480p' in encoded_files:
                    input_file = downloaded_file  # Use original for better quality
                
                output_path = os.path.join(
                    ENCODES_DIR,
                    f"encoded_{quality}_{os.path.basename(downloaded_file)}"
                )

                # Encode
                print(f"Starting {quality} encode...")
                _, process = await encoder.encode_video(
                    input_file, output_path,
                    Config.TARGET_SIZES[quality], quality,
                    progress_tracker.update_progress
                )

                if process.returncode != 0:
                    raise Exception(f"Encoding failed for {quality}")

                encoded_files[quality] = output_path
                print(f"Successfully encoded {quality}")

                # Get media info and upload to Telegraph
                info = media_info.get_media_info(output_path)
                telegraph_url = await media_info.upload_to_telegraph(
                    info, input_file, output_path
                )
                
                # Generate filename with correct details
                new_name = renamer.generate_filename(os.path.basename(downloaded_file), quality)
                
                # Update caption format
                caption = (
                    f"✅ {new_name}\n\n"
                    f"📊 Media Info\n\n"
                    f"<b>General</b>\n"
                    f"Format: {info['general'].get('format', 'N/A')}\n"
                    f"Quality: {quality}\n"
                    f"Duration: {info['general'].get('duration', 'N/A')}\n\n"
                    f"<b>Video</b>\n"
                    f"Codec: {info['video'].get('codec', 'N/A')}\n"
                    f"Resolution: {info['video'].get('resolution', 'N/A')}\n"
                    f"FPS: {info['video'].get('fps', 'N/A')}\n"
                    f"Bitrate: {info['video'].get('bitrate', 'N/A')}\n\n"
                    f"<b>Size</b>\n"
                    f"Before: {file_size:.2f} MB\n"
                    f"After: {os.path.getsize(output_path)/(1024*1024):.2f} MB\n"
                    f"Saved: {((file_size-os.path.getsize(output_path)/(1024*1024))/file_size)*100:.1f}%"
                )

                # Upload with new parameters
                await Uploader.upload_video(
                    item.message._client,
                    item.message.chat.id,
                    output_path,
                    caption=caption,
                    filename=new_name,
                    telegraph_url=telegraph_url,
                    progress_callback=progress_tracker.update_progress
                )
                
                encoding_status[quality] = "✅ Done"
                await status_message.edit_text(
                    "🎬 Encoding Status:\n" + 
                    "\n".join(f"{q}: {encoding_status[q]}" for q in Config.QUALITIES)
                )

            except Exception as e:
                print(f"Error processing {quality}: {e}")
                encoding_status[quality] = "❌ Failed"
                await status_message.edit_text(f"Error in {quality}: {str(e)}")
                continue

        # Cleanup only after all qualities are done
        cleanup_files = list(encoded_files.values())
        if downloaded_file:
            cleanup_files.append(downloaded_file)
            
        for file_path in cleanup_files:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"Cleaned up: {file_path}")
            except Exception as e:
                print(f"Cleanup error: {e}")

        await status_message.edit_text("✅ All processing complete!")

    except Exception as e:
        print(f"Process error: {e}")
        await status_message.edit_text(f"❌ Error: {str(e)}")

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
