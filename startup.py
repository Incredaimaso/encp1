import asyncio
import subprocess
import sys
import os
import socket
from config import Config
import psutil

def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def get_aria2c_processes():
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if 'aria2c' in proc.info['name'].lower():
                return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return None

async def start_aria2c():
    try:
        downloads_dir = os.path.abspath("downloads")
        os.makedirs(downloads_dir, exist_ok=True)
        
        # Kill existing processes
        if sys.platform == 'win32':
            subprocess.run('taskkill /F /IM aria2c.exe', shell=True, stderr=subprocess.DEVNULL)
            await asyncio.sleep(1)
        else:
            subprocess.run('pkill -9 aria2c', shell=True, stderr=subprocess.DEVNULL)
        
        # Simplified aria2c command
        cmd = [
            'aria2c',
            '--enable-rpc',
            f'--rpc-listen-port={Config.ARIA2_PORT}',
            '--rpc-listen-all=true',
            '--rpc-allow-origin-all=true',
            f'--dir={downloads_dir}'  # Keep download directory setting
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        )

        # Wait for process to start
        for i in range(5):
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise Exception(f"aria2c failed to start: {stderr.decode()}")
            
            if is_port_in_use(Config.ARIA2_PORT):
                print(f"Aria2c daemon started successfully in {downloads_dir}")
                return process
                
            await asyncio.sleep(1)

        raise Exception("Aria2c failed to start after timeout")

    except Exception as e:
        print(f"Failed to start aria2c daemon: {e}")
        sys.exit(1)
