import asyncio
import subprocess
import sys
import os
import socket
import signal
import psutil
import resource
import logging
import time
import traceback
import json
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path
from contextlib import suppress
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("process_manager.log")
    ]
)
logger = logging.getLogger("ProcessManager")

class ConfigurationError(Exception):
    """Exception raised for errors in the configuration."""
    pass

class ProcessError(Exception):
    """Exception raised for errors in process management."""
    pass

class ResourceError(Exception):
    """Exception raised for resource constraint violations."""
    pass

class Aria2Error(Exception):
    """Exception raised for aria2c-specific errors."""
    pass

def retry(max_attempts=3, delay=2, backoff=2, exceptions=(Exception,)):
    """Retry decorator with exponential backoff for functions."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay
            
            while attempts < max_attempts:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    attempts += 1
                    if attempts >= max_attempts:
                        logger.error(f"Function {func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    
                    logger.warning(f"Attempt {attempts} failed for {func.__name__}: {e}. Retrying in {current_delay}s...")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
                    
        return wrapper
    return decorator

class ProcessManager:
    """Manages system processes with comprehensive error handling and monitoring."""
    
    def __init__(self, config=None):
        """Initialize the process manager with configuration parameters."""
        self.config = config or {}
        self.processes = []
        self.aria2_process = None
        self.shutdown_requested = False
        self.shutting_down = False
        
        # Default configuration with sensible values
        self.max_memory_percent = self.config.get('max_memory_percent', 90)
        self.max_cpu_percent = self.config.get('max_cpu_percent', 90)
        self.cpu_affinity = self.config.get('cpu_affinity', list(range(os.cpu_count())))
        self.monitor_interval = self.config.get('monitor_interval', 60)
        self.aria2_port = self.config.get('aria2_port', 6800)
        self.downloads_dir = Path(self.config.get('downloads_dir', 'downloads')).absolute()
        
        # Validate configuration
        self._validate_config()
        
        # Register signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._signal_handler)

    def _validate_config(self) -> None:
        """Validate configuration parameters to ensure they're within acceptable ranges."""
        try:
            if not 0 <= self.max_memory_percent <= 100:
                raise ConfigurationError(f"Invalid max_memory_percent: {self.max_memory_percent}. Must be between 0-100.")
            
            if not 0 <= self.max_cpu_percent <= 100:
                raise ConfigurationError(f"Invalid max_cpu_percent: {self.max_cpu_percent}. Must be between 0-100.")
            
            if not self.cpu_affinity:
                raise ConfigurationError("CPU affinity list cannot be empty.")
            
            if max(self.cpu_affinity) >= os.cpu_count():
                raise ConfigurationError(f"Invalid CPU affinity. Maximum value should be less than {os.cpu_count()}.")
            
            if self.monitor_interval < 1:
                raise ConfigurationError(f"Invalid monitor_interval: {self.monitor_interval}. Must be at least 1 second.")
            
            if not 1024 <= self.aria2_port <= 65535:
                raise ConfigurationError(f"Invalid aria2_port: {self.aria2_port}. Must be between 1024-65535.")
            
            # Create downloads directory if it doesn't exist
            self.downloads_dir.mkdir(parents=True, exist_ok=True)
            
        except Exception as e:
            logger.critical(f"Configuration validation failed: {e}")
            raise ConfigurationError(f"Configuration validation failed: {e}") from e

    def _signal_handler(self, sig, frame) -> None:
        """Handle system signals for graceful shutdown."""
        if self.shutting_down:
            logger.warning("Forced exit requested. Terminating immediately.")
            sys.exit(1)
            
        logger.info(f"Signal {sig} received. Initiating graceful shutdown...")
        self.shutdown_requested = True
        
        # Schedule cleanup to run in the event loop
        if asyncio.get_event_loop().is_running():
            asyncio.create_task(self.cleanup())
        else:
            self.cleanup_sync()

    async def setup_processes(self) -> bool:
        """Initialize all system processes with proper error handling."""
        try:
            logger.info("Setting up system processes...")
            
            # Set process limits
            try:
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                target_limit = min(131072, hard)
                resource.setrlimit(resource.RLIMIT_NOFILE, (target_limit, hard))
                logger.info(f"Set file descriptor limit to {target_limit} (was {soft})")
            except (resource.error, ValueError) as e:
                logger.warning(f"Could not set file descriptor limit: {e}")
            
            # Set CPU affinity
            if hasattr(os, 'sched_setaffinity'):
                try:
                    os.sched_setaffinity(0, self.cpu_affinity)
                    logger.info(f"Set CPU affinity to cores: {self.cpu_affinity}")
                except (OSError, ValueError) as e:
                    logger.warning(f"Could not set CPU affinity: {e}")
            
            # Start resource monitoring
            self._monitor_task = asyncio.create_task(self._monitor_resources())
            self._monitor_task.add_done_callback(self._handle_task_exception)
            
            return True
            
        except Exception as e:
            logger.critical(f"Process setup error: {e}\n{traceback.format_exc()}")
            await self.cleanup()
            raise ProcessError(f"Failed to set up processes: {e}") from e

    def _handle_task_exception(self, task):
        """Handle exceptions in background tasks."""
        if task.cancelled():
            return
            
        exception = task.exception()
        if exception:
            logger.error(f"Task {task.get_name()} failed with error: {exception}\n{traceback.format_exc()}")
            
            # Restart monitoring if it fails
            if task == self._monitor_task and not self.shutdown_requested:
                logger.info("Restarting resource monitoring task...")
                self._monitor_task = asyncio.create_task(self._monitor_resources())
                self._monitor_task.add_done_callback(self._handle_task_exception)

    async def _monitor_resources(self) -> None:
        """Monitor system resources and manage processes accordingly."""
        logger.info(f"Starting resource monitoring (interval: {self.monitor_interval}s)")
        
        while not self.shutdown_requested:
            try:
                # Check memory usage
                memory_info = psutil.virtual_memory()
                memory_percent = memory_info.percent
                if memory_percent > self.max_memory_percent:
                    logger.warning(f"High memory usage: {memory_percent:.1f}% (threshold: {self.max_memory_percent}%)")
                    
                    if memory_percent > 95:  # Critical threshold
                        logger.critical(f"Critical memory usage ({memory_percent:.1f}%). Taking emergency action.")
                        # Emergency memory recovery could be implemented here
                
                # Check CPU usage
                cpu_percent = psutil.cpu_percent(interval=1)
                if cpu_percent > self.max_cpu_percent:
                    logger.warning(f"High CPU usage: {cpu_percent:.1f}% (threshold: {self.max_cpu_percent}%)")
                
                # Monitor aria2c process
                if self.aria2_process and not await self._check_aria2c():
                    logger.warning("Aria2c process not detected. Attempting restart...")
                    await self.start_aria2()
                    
            except Exception as e:
                logger.error(f"Resource monitoring error: {e}\n{traceback.format_exc()}")
            
            await asyncio.sleep(self.monitor_interval)
        
        logger.info("Resource monitoring stopped")

    async def _check_aria2c(self) -> bool:
        """Check if aria2c is running and responding."""
        # First check if the process is running
        if self.aria2_process and self.aria2_process.poll() is not None:
            return False
            
        # Then check if any aria2c process exists
        for proc in psutil.process_iter(['name', 'pid']):
            try:
                if 'aria2c' in proc.info['name']:
                    # Verify it's responding to RPC calls
                    try:
                        import urllib.request
                        import json
                        import urllib.error
                        
                        data = json.dumps({
                            "jsonrpc": "2.0",
                            "id": "healthcheck",
                            "method": "aria2.getGlobalStat",
                            "params": []
                        }).encode('utf-8')
                        
                        req = urllib.request.Request(
                            f"http://localhost:{self.aria2_port}/jsonrpc",
                            data=data,
                            headers={'Content-Type': 'application/json'}
                        )
                        
                        with urllib.request.urlopen(req, timeout=2) as response:
                            if response.getcode() == 200:
                                return True
                    except (urllib.error.URLError, json.JSONDecodeError, ConnectionRefusedError) as e:
                        logger.debug(f"Aria2c RPC check failed: {e}")
                        return False
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                logger.debug(f"Process check error: {e}")
                continue
                
        return False

    @retry(max_attempts=3, delay=2, backoff=2, exceptions=(Aria2Error, subprocess.SubprocessError))
    async def start_aria2c(self) -> Optional[subprocess.Popen]:
        """Start aria2c daemon with improved settings and robust error handling."""
        process = None
        
        try:
            logger.info("Starting aria2c daemon...")
            
            # Ensure downloads directory exists
            self.downloads_dir.mkdir(parents=True, exist_ok=True)
            
            # Kill existing aria2c processes
            await self._terminate_existing_aria2c()
            
            # Clean up temporary files
            await self._cleanup_aria2_temp_files()
            
            # Check if port is available
            if not await self._check_port_available(self.aria2_port):
                new_port = await self._find_available_port(self.aria2_port + 1, 7000)
                if new_port:
                    logger.warning(f"Port {self.aria2_port} is busy, using port {new_port} instead")
                    self.aria2_port = new_port
                else:
                    raise Aria2Error(f"Port {self.aria2_port} is busy and no alternative ports are available")
            
            # Build aria2c command
            cmd = self._build_aria2_command()
            
            logger.info(f"Executing: {' '.join(cmd)}")
            
            # Start process with appropriate error handling
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1  # Line-buffered
            )
            
            # Verify process started successfully
            if process.poll() is not None:
                stderr = process.stderr.read() if process.stderr else "No error output available"
                raise Aria2Error(f"aria2c failed to start: {stderr}")
                
            # Give process some time to initialize before checking
            await asyncio.sleep(2)
            
            # Wait for RPC to become available
            rpc_available, error_message = await self._wait_for_aria2_rpc()
            if not rpc_available:
                if process.poll() is not None:
                    stderr = process.stderr.read() if process.stderr else "No error output available"
                    raise Aria2Error(f"aria2c process died: {stderr}")
                else:
                    process.terminate()
                    raise Aria2Error(f"aria2c RPC failed to respond: {error_message}")
            
            logger.info(f"✅ Aria2c started successfully (PID: {process.pid})")
            self.processes.append(process)
            self.aria2_process = process
            
            # Start log readers
            asyncio.create_task(self._read_process_output(process.stdout, logging.INFO))
            asyncio.create_task(self._read_process_output(process.stderr, logging.ERROR))
            
            return process
            
        except Aria2Error as e:
            logger.error(f"aria2c error: {e}")
            if process and process.poll() is None:
                process.terminate()
            raise
            
        except Exception as e:
            logger.error(f"Failed to start aria2c: {e}\n{traceback.format_exc()}")
            if process and process.poll() is None:
                process.terminate()
            raise Aria2Error(f"Unexpected error starting aria2c: {e}") from e

    async def _check_port_available(self, port: int) -> bool:
        """Check if a port is available for use."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('127.0.0.1', port))
            sock.close()
            return result != 0  # If result is 0, connection succeeded, so port is in use
        except Exception as e:
            logger.warning(f"Port check error: {e}")
            return False  # Assume port is not available on error

    async def _find_available_port(self, start_port: int, end_port: int) -> Optional[int]:
        """Find an available port in the specified range."""
        for port in range(start_port, end_port + 1):
            if await self._check_port_available(port):
                return port
        return None

    async def _terminate_existing_aria2c(self) -> None:
        """Safely terminate any existing aria2c processes."""
        try:
            if os.name == 'nt':
                subprocess.run("taskkill /f /im aria2c.exe", shell=True, check=False)
            else:
                subprocess.run("pkill aria2c", shell=True, check=False)
            await asyncio.sleep(2)  # Allow processes to terminate
            
            # Double-check and forcefully terminate if needed
            for proc in psutil.process_iter(['name', 'pid']):
                try:
                    if 'aria2c' in proc.info['name']:
                        logger.warning(f"Forcefully terminating aria2c process (PID: {proc.info['pid']})")
                        os.kill(proc.info['pid'], 9)  # SIGKILL
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                    logger.debug(f"Process termination error: {e}")
                    
        except Exception as e:
            logger.warning(f"Error while terminating existing aria2c processes: {e}")

    async def _cleanup_aria2_temp_files(self) -> None:
        """Clean up aria2c temporary files."""
        try:
            cleanup_count = 0
            for f in self.downloads_dir.glob('*.aria2'):
                f.unlink()
                cleanup_count += 1
                
            if cleanup_count > 0:
                logger.info(f"Cleaned up {cleanup_count} aria2 temporary files")
                
        except Exception as e:
            logger.warning(f"Error cleaning up aria2 temporary files: {e}")

    def _build_aria2_command(self) -> List[str]:
        """Build the aria2c command with appropriate settings."""
        return [
            'aria2c',
            '--enable-rpc',
            f'--rpc-listen-port={self.aria2_port}',
            '--rpc-listen-all=true',
            '--daemon=false',
            '--max-connection-per-server=16',
            '--split=16',
            '--min-split-size=10M',
            '--max-concurrent-downloads=10',
            '--seed-time=0',
            '--check-certificate=false',
            '--max-overall-upload-limit=1K',
            '--file-allocation=none',
            f'--dir={self.downloads_dir}',
            '--allow-overwrite=true',
            '--continue=true',
            '--auto-file-renaming=false',
            '--log-level=warn',
            '--keep-unfinished-download-result=true',
            '--retry-wait=5',
            '--max-tries=5',
            '--connect-timeout=10',
            '--timeout=10'
        ]

    async def _wait_for_aria2_rpc(self) -> Tuple[bool, str]:
        """Wait for aria2c RPC to become available. Returns (success, error_message)."""
        import urllib.request
        import urllib.error
        
        max_attempts = 10
        last_error = ""
        
        for attempt in range(max_attempts):
            try:
                # Try a simple connection first to check if the port is accessible
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1)
                result = s.connect_ex(('localhost', self.aria2_port))
                s.close()
                
                if result != 0:
                    last_error = f"Port {self.aria2_port} is not open yet"
                    logger.debug(f"Waiting for port to open ({attempt + 1}/{max_attempts}): {last_error}")
                    await asyncio.sleep(1)
                    continue
                
                # Now try an actual RPC call
                data = json.dumps({
                    "jsonrpc": "2.0",
                    "id": "healthcheck",
                    "method": "aria2.getVersion",
                    "params": []
                }).encode('utf-8')
                
                req = urllib.request.Request(
                    f"http://localhost:{self.aria2_port}/jsonrpc",
                    data=data,
                    headers={'Content-Type': 'application/json'}
                )
                
                with urllib.request.urlopen(req, timeout=2) as response:
                    if response.getcode() == 200:
                        result = json.loads(response.read().decode('utf-8'))
                        logger.info(f"Aria2c RPC responding on port {self.aria2_port} (attempt {attempt + 1})")
                        logger.info(f"Aria2c version: {result.get('result', {}).get('version', 'unknown')}")
                        return True, ""
            except json.JSONDecodeError as e:
                last_error = f"Invalid JSON response: {e}"
                logger.debug(f"Waiting for RPC ({attempt + 1}/{max_attempts}): {last_error}")
            except urllib.error.URLError as e:
                last_error = f"URL error: {e}"
                logger.debug(f"Waiting for RPC ({attempt + 1}/{max_attempts}): {last_error}")
            except ConnectionRefusedError as e:
                last_error = f"Connection refused: {e}"
                logger.debug(f"Waiting for RPC ({attempt + 1}/{max_attempts}): {last_error}")
            except socket.timeout as e:
                last_error = f"Socket timeout: {e}"
                logger.debug(f"Waiting for RPC ({attempt + 1}/{max_attempts}): {last_error}")
            except Exception as e:
                last_error = f"Unexpected error: {e}"
                logger.debug(f"Waiting for RPC ({attempt + 1}/{max_attempts}): {last_error}")
                
            await asyncio.sleep(1)
                
        logger.error(f"Aria2c RPC failed to respond after {max_attempts} attempts. Last error: {last_error}")
        return False, last_error

    async def _read_process_output(self, pipe, log_level):
        """Read and log process output."""
        if pipe is None:
            return
            
        while True:
            line = pipe.readline()
            if not line:
                break
            line = line.strip()
            if line:
                logger.log(log_level, f"aria2c: {line}")

    async def cleanup(self) -> None:
        """Clean up all managed processes asynchronously."""
        if self.shutting_down:
            return
            
        self.shutting_down = True
        logger.info("Cleaning up processes...")
        
        # Cancel monitoring task
        if hasattr(self, '_monitor_task') and self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._monitor_task
        
        # Terminate all managed processes
        terminate_tasks = []
        for proc in self.processes:
            if proc and proc.poll() is None:
                terminate_tasks.append(self._terminate_process(proc))
                
        if terminate_tasks:
            await asyncio.gather(*terminate_tasks, return_exceptions=True)
            
        logger.info("Cleanup completed")

    async def _terminate_process(self, proc):
        """Safely terminate a process with timeout handling."""
        try:
            proc_info = f"Process (PID: {proc.pid})"
            logger.info(f"Terminating {proc_info}")
            
            proc.terminate()
            
            # Wait for process to terminate
            for _ in range(5):  # 5 second timeout
                if proc.poll() is not None:
                    logger.info(f"{proc_info} terminated successfully")
                    return
                await asyncio.sleep(1)
                
            # Force kill if still running
            logger.warning(f"{proc_info} did not terminate gracefully, sending SIGKILL")
            try:
                if os.name == 'nt':
                    subprocess.run(f"taskkill /F /PID {proc.pid}", shell=True, check=False)
                else:
                    os.kill(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass  # Process already gone
                
        except Exception as e:
            logger.error(f"Error terminating process: {e}")

    def cleanup_sync(self) -> None:
        """Synchronous version of cleanup for signal handlers."""
        if self.shutting_down:
            return
            
        self.shutting_down = True
        logger.info("Performing synchronous cleanup...")
        
        for proc in self.processes:
            try:
                if proc and proc.poll() is None:
                    logger.info(f"Terminating process (PID: {proc.pid})")
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        logger.warning(f"Force killing process (PID: {proc.pid})")
                        if os.name == 'nt':
                            subprocess.run(f"taskkill /F /PID {proc.pid}", shell=True, check=False)
                        else:
                            os.kill(proc.pid, signal.SIGKILL)
            except Exception as e:
                logger.error(f"Error in synchronous cleanup: {e}")
                
        logger.info("Synchronous cleanup completed")


class Application:
    """Main application class to manage the application lifecycle."""
    
    def __init__(self, config=None):
        """Initialize the application with configuration."""
        self.config = config or {}
        self.process_manager = ProcessManager(config)
        
    async def start(self):
        """Start the application and all required processes."""
        try:
            logger.info("Starting application...")
            
            # Set up process limits and monitoring
            if not await self.process_manager.setup_processes():
                raise ProcessError("Failed to set up process management")
                
            # Start aria2c
            aria2_process = await self.process_manager.start_aria2()
            if not aria2_process:
                raise Aria2Error("Failed to start aria2c")
                
            logger.info("Application started successfully")
            return True
            
        except Exception as e:
            logger.critical(f"Application start failed: {e}\n{traceback.format_exc()}")
            await self.process_manager.cleanup()
            return False
            
    async def stop(self):
        """Stop the application gracefully."""
        logger.info("Stopping application...")
        await self.process_manager.cleanup()
        logger.info("Application stopped")


# Create a factory function to instantiate and configure the application
def create_application(config=None) -> Application:
    """Create and configure the application instance."""
    if config is None:
        # Try to import configuration from a local module
        try:
            from config import Config
            config = {
                'aria2_port': getattr(Config, 'ARIA2_PORT', 6800),
                'downloads_dir': getattr(Config, 'DOWNLOADS_DIR', 'downloads'),
                'max_memory_percent': getattr(Config, 'MAX_MEMORY_PERCENT', 90),
                'max_cpu_percent': getattr(Config, 'MAX_CPU_PERCENT', 90),
                'monitor_interval': getattr(Config, 'MONITOR_INTERVAL', 60),
            }
        except ImportError:
            logger.warning("Config module not found, using default values")
            config = {}
            
    return Application(config)


# Functional API for backward compatibility
async def start_aria2c():
    """Start aria2c daemon and return when ready (compatibility function)."""
    app = create_application()
    try:
        await app.start()
        return app.process_manager.aria2_process
    except Exception as e:
        logger.error(f"Failed to start aria2c: {e}")
        await app.stop()
        return None


# Create and export application instance for global use
app = create_application()
process_manager = app.process_manager


# Main entry point when run directly
async def main():
    """Main application entry point."""
    try:
        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()
        
        # Start the application
        if await app.start():
            logger.info("Application running. Press Ctrl+C to stop.")
            
            # Keep running until shutdown is requested
            while not process_manager.shutdown_requested:
                await asyncio.sleep(1)
                
        else:
            logger.error("Application failed to start")
            
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        
    finally:
        await app.stop()


# Export the required functions and objects
__all__ = ['start_aria2c', 'process_manager', 'app', 'main', 'Application', 'ProcessManager']
cleanup = process_manager.cleanup


# Run the application when executed directly
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical(f"Fatal error: {e}\n{traceback.format_exc()}")
        sys.exit(1)