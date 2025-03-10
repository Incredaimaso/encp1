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
    TARGET_SIZES = {
        '480p': 95,   # Target slightly below 100MB limit
        '720p': 190,  # Target slightly below 200MB limit
        '1080p': 290  # Target slightly below 300MB limit
    }
