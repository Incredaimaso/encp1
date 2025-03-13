import os
from dotenv import load_dotenv

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
    
    # Performance settings
    MAX_CONCURRENT_ENCODES = 2  # Number of simultaneous encodes
    RAM_USAGE_LIMIT = 14 * 1024  # 14GB in MB
    CPU_USAGE_LIMIT = 90  # Max CPU usage percentage
    TEMP_BUFFER_SIZE = 64 * 1024  # 64MB buffer for I/O
    
    TARGET_SIZES = {
        '480p': 90,   # Slightly reduced targets for CPU
        '720p': 185,  
        '1080p': 280  
    }
