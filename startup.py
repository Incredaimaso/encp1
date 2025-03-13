import asyncio
import subprocess
import sys
import os
import socket
import signal
import psutil
import resource
from config import Config
from typing import Optional

class ProcessManager:
    def __init__(self):
        self.processes = []
        self.aria2_process = None
        self.max_memory_percent = 90
        self.cpu_affinity = list(range(os.cpu_count()))  # Use all cores
        self.monitor_interval = 60  # Check every minute

    async def setup_processes(self):
        """Initialize all system processes"""
        try:
            # Set process limits
            resource.setrlimit(resource.RLIMIT_NOFILE, (131072, 131072))
            
            # Set CPU affinity
            if hasattr(os, 'sched_setaffinity'):
                os.sched_setaffinity(0, self.cpu_affinity)
            
            # Start process monitoring
            asyncio.create_task(self._monitor_resources())
            
            return True
        except Exception as e:
            print(f"Process setup error: {e}")
            return False

    async def _monitor_resources(self):
        """Monitor system resources"""
        while True:
            try:
                # Check memory usage
                memory_percent = psutil.virtual_memory().percent
                if memory_percent > self.max_memory_percent:
                    print(f"⚠️ High memory usage: {memory_percent}%")
                
                # Check CPU usage
                cpu_percent = psutil.cpu_percent(interval=1)
                if cpu_percent > 90:
                    print(f"⚠️ High CPU usage: {cpu_percent}%")
                
                # Monitor aria2c process
                if not self._check_aria2c():
                    print("♻️ Restarting aria2c...")
                    await self.start_aria2()
                    
            except Exception as e:
                print(f"Monitor error: {e}")
            
            await asyncio.sleep(self.monitor_interval)

    def _check_aria2c(self) -> bool:
        """Check if aria2c is running"""
        for proc in psutil.process_iter(['name']):
            try:
                if 'aria2c' in proc.info['name']:
                    return True
            except:
                continue
        return False

    async def start_aria2(self):
        """Start aria2c daemon"""
        try:
            downloads_dir = os.path.abspath("downloads")
            os.makedirs(downloads_dir, exist_ok=True)
            
            cmd = [
                'aria2c',
                '--enable-rpc',
                f'--rpc-listen-port={Config.ARIA2_PORT}',
                '--rpc-listen-all=true',
                '--daemon=false',
                '--max-connection-per-server=10',
                '--rpc-max-request-size=1024M',
                '--seed-time=0.01',
                '--min-split-size=10M',
                '--follow-torrent=mem',
                '--split=10',
                f'--dir={downloads_dir}'
            ]

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid if os.name != 'nt' else None
            )
            
            self.processes.append(process)
            self.aria2_process = process
            print(f"✅ Aria2c daemon started - PID: {process.pid}")
            return process

        except Exception as e:
            print(f"❌ Failed to start aria2c: {e}")
            return None

    def cleanup(self):
        """Clean up all managed processes"""
        print("\n🧹 Cleaning up processes...")
        for proc in self.processes:
            try:
                if proc and proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass

# Create global process manager
process_manager = ProcessManager()

# Make cleanup function available
cleanup = process_manager.cleanup
