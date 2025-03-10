import asyncio
import subprocess
import sys
import os
import socket
from config import Config

def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def kill_aria2c():
    try:
        if sys.platform == 'win32':
            subprocess.run(['taskkill', '/F', '/IM', 'aria2c.exe'], 
                         stdout=subprocess.DEVNULL, 
                         stderr=subprocess.DEVNULL)
        else:
            subprocess.run(['pkill', '-9', 'aria2c'], 
                         stdout=subprocess.DEVNULL, 
                         stderr=subprocess.DEVNULL)
    except Exception as e:
        print(f"Error killing aria2c: {e}")

async def start_aria2c():
    try:
        downloads_dir = os.path.abspath("downloads")
        os.makedirs(downloads_dir, exist_ok=True)

        # Kill any existing aria2c process
        kill_aria2c()
        await asyncio.sleep(2)

        # Check if port is still in use
        if is_port_in_use(Config.ARIA2_PORT):
            print(f"Port {Config.ARIA2_PORT} is still in use, waiting...")
            for _ in range(5):
                await asyncio.sleep(1)
                if not is_port_in_use(Config.ARIA2_PORT):
                    break
            else:
                raise Exception(f"Port {Config.ARIA2_PORT} is still in use after waiting")

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
            '--min-split-size=10M',
            '--rpc-listen-all=true',
            '--quiet=true',
            '--rpc-max-request-size=10M',
            '--connect-timeout=60',
            '--timeout=600',
            '--max-tries=5',
            '--retry-wait=10',
            '--rpc-save-upload-metadata=true',
            '--allow-overwrite=true',
            '--auto-file-renaming=false',
            f'--dir={downloads_dir}',
            '--bt-max-peers=0',
            '--seed-time=0',
            '--file-allocation=none'
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        )

        # Wait for aria2c to start
        for _ in range(10):
            if process.poll() is not None:
                raise Exception("aria2c failed to start")
            if is_port_in_use(Config.ARIA2_PORT):
                print(f"Aria2c daemon started successfully in {downloads_dir}")
                return process
            await asyncio.sleep(1)

        raise Exception("Aria2c failed to start after waiting")

    except Exception as e:
        print(f"Failed to start aria2c daemon: {e}")
        sys.exit(1)
