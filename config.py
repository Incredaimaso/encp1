import os
from dotenv import load_dotenv
import psutil

load_dotenv()

class Config:
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    API_ID = int(os.getenv('API_ID'))
    API_HASH = os.getenv('API_HASH')
    OWNER_ID = int(os.getenv('OWNER_ID'))
    ARIA2_HOST = os.getenv('ARIA2_HOST')
    ARIA2_PORT = int(os.getenv('ARIA2_PORT'))
    ARIA2_SECRET = os.getenv('ARIA2_SECRET')

    SUPPORTED_FORMATS = ['.mkv', '.mp4', '.avi', '.webm']
    QUALITIES = ['480p', '720p', '1080p']
    
    # Enhanced performance settings
    MAX_CONCURRENT_ENCODES = max(1, os.cpu_count() // 2)  # Half of CPU cores
    RAM_USAGE_LIMIT = int(psutil.virtual_memory().total * 0.9 / (1024 * 1024))  # 90% of total RAM
    CPU_USAGE_LIMIT = 100  # Use all available CPU
    IO_NICE = -10  # Higher I/O priority (Linux only)
    PROCESS_NICE = -10  # Higher process priority (Linux only)
    TEMP_BUFFER_SIZE = 256 * 1024  # 256MB buffer for I/O

    # FFmpeg specific settings
    FFMPEG_THREAD_QUEUE_SIZE = 1024  # Larger thread queue
    FFMPEG_HWACCEL = 'auto'  # Auto hardware acceleration
    FFMPEG_CUSTOM_OPTS = {
        'thread_queue_size': '1024',
        'probesize': '100M',
        'analyzeduration': '100M'
    }
    
    TARGET_SIZES = {
        '480p': 90,   # Slightly reduced targets for CPU
        '720p': 185,  
        '1080p': 280  
    }
    
    # Bot settings
    DEFAULT_PARSE_MODE = "markdown"
    MESSAGE_TEMPLATES = {
        'welcome': "*Welcome to Video Encoder Bot!*\n"
                  "Send me a video or use `/l` to download and encode.",
        'help': "*Available Commands:*\n"
               "`/l <url>` - Download and encode video\n"
               "`/add <user_id>` - Add approved user (owner only)",
        'error': "❌ *Error:* `{}`"
    }
