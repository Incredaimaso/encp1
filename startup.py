import asyncio
import subprocess
import sys
import os
from config import Config

async def start_aria2c():
    try:
        downloads_dir = os.path.abspath("downloads")
        os.makedirs(downloads_dir, exist_ok=True)
        
        if sys.platform == 'win32':
            subprocess.run('taskkill /F /IM aria2c.exe', shell=True, stderr=subprocess.DEVNULL)
        else:
            subprocess.run('killall aria2c', shell=True, stderr=subprocess.DEVNULL)
        
        cmd = [
            'aria2c',
            '--enable-rpc',
            f'--rpc-listen-port={Config.ARIA2_PORT}',
            '--rpc-allow-origin-all',
            '--daemon=false',
            '--continue=true',
            '--max-concurrent-downloads=3',
            '--max-connection-per-server=16',
            '--split=16',
            '--rpc-listen-all=true',
            '--quiet=true',
            f'--dir={downloads_dir}',
            '--bt-max-peers=0',
            '--seed-time=0',
            '--file-allocation=none'
        ]
        
        process = subprocess.Popen(cmd)
        await asyncio.sleep(3)
        print(f"Aria2c daemon started successfully in {downloads_dir}")
        return process
    except Exception as e:
        print(f"Failed to start aria2c daemon: {e}")
        sys.exit(1)
