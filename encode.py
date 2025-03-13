import ffmpeg
import os
import time
import asyncio
from typing import Dict, Tuple
from cpu_encoder import CPUEncoder
import subprocess
import re
import pathlib
import logging
import sys

class VideoEncoder:
    def __init__(self):
        self.quality_params = {
            '480p': {
                'height': 480,
                'target_size': 95,
                'audio_bitrate': '64k'
            },
            '720p': {
                'height': 720,
                'target_size': 190,
                'audio_bitrate': '96k'
            },
            '1080p': {
                'height': 1080,
                'target_size': 285,
                'audio_bitrate': '128k'
            }
        }
        self._gpu_check_done = False
        self.gpu_available = None
        self.cpu_encoder = CPUEncoder()
        self.min_progress_interval = 0.5  # Minimum time between progress updates
        self.logger = logging.getLogger('encoder')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)
        self.process_timeout = 7200  # 2 hours max encoding time
        self.progress_interval = 1  # Check progress every second
        self.MAX_SIZES = {
            '480p': 95,  # ~100MB limit
            '720p': 190, # ~200MB limit
            '1080p': 285 # ~300MB limit
        }
        self.last_line_length = 0  # For single-line updates
    
    def _check_gpu(self) -> bool:
        if self._gpu_check_done:
            return self.gpu_available
            
        try:
            # First check if nvidia-smi is available
            try:
                subprocess.check_output(['nvidia-smi'])
                print("NVIDIA GPU detected via nvidia-smi")
            except (subprocess.SubprocessError, FileNotFoundError):
                print("nvidia-smi check failed")
                self._gpu_check_done = True
                self.gpu_available = False
                return False

            # Then try a test encode
            test_input = ffmpeg.input('testsrc=duration=1:size=64x64', f='lavfi')
            test_output = ffmpeg.output(
                test_input, 
                '-', 
                vcodec='h264_nvenc',
                f='null'
            )
            ffmpeg.run(test_output, capture_stdout=True, capture_stderr=True, overwrite_output=True)
            print("NVIDIA encoder test successful")
            self._gpu_check_done = True
            self.gpu_available = True
            return True
        except Exception as e:
            print(f"GPU check error: {e}")
            self._gpu_check_done = True
            self.gpu_available = False
            return False

    def _calculate_encoding_params(self, target_size: int, duration: float) -> dict:
        # Calculate bitrate in kbps
        target_bits = target_size * 8 * 1024 * 1024
        bitrate = int(target_bits / duration)
        
        # Ensure buffer size is within limits (2GB)
        maxrate = min(int(bitrate * 1.5), 2_000_000)  # 2Mbps max
        bufsize = min(int(bitrate * 2), 2_000_000)    # 2Mbps max

        return {
            'b:v': f'{bitrate}k',
            'maxrate': f'{maxrate}k',
            'bufsize': f'{bufsize}k'
        }

    def _calculate_target_size(self, input_size: float, resolution: str) -> float:
        """Calculate target size based on input size and quality"""
        max_sizes = {'480p': 95, '720p': 190, '1080p': 290}  # Slightly below limits
        
        if resolution == '480p':
            target = min(input_size * 0.35, max_sizes['480p'])  # 35% of original
        elif resolution == '720p':
            target = min(input_size * 0.55, max_sizes['720p'])  # 55% of original
        else:  # 1080p
            target = min(input_size * 0.75, max_sizes['1080p'])  # 75% of original
            
        print(f"Target size for {resolution}: {target:.1f}MB (from {input_size:.1f}MB)")
        return target

    async def _verify_file(self, file_path: str, max_retries: int = 5) -> bool:
        for i in range(max_retries):
            try:
                # Check if file exists and is not being written
                if os.path.exists(file_path):
                    initial_size = os.path.getsize(file_path)
                    await asyncio.sleep(2)
                    if os.path.getsize(file_path) == initial_size:
                        # Try to probe the file
                        probe = ffmpeg.probe(file_path, v='error')
                        if 'streams' in probe and probe['streams']:
                            return True
                print(f"Verification attempt {i+1}/{max_retries}")
                await asyncio.sleep(2)
            except ffmpeg.Error as e:
                print(f"Probe error: {e.stderr.decode() if e.stderr else str(e)}")
            except Exception as e:
                print(f"Verification error: {e}")
        return False

    def _update_progress_line(self, text: str):
        # Clear previous line
        sys.stdout.write('\r' + ' ' * self.last_line_length)
        sys.stdout.write('\r' + text)
        sys.stdout.flush()
        self.last_line_length = len(text)

    async def encode_video(self, input_file: str, output_file: str, 
                          target_size: int, resolution: str,
                          progress_callback=None) -> Tuple[str, Dict]:
        process = None
        try:    
            # Single log message for encode start
            print(f"\n🎬 Starting {resolution} encode...")
            
            # Create logger instance for this encode only
            encode_logger = logging.getLogger(f'encoder_{resolution}')
            if not encode_logger.handlers:
                handler = logging.StreamHandler()
                handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
                encode_logger.addHandler(handler)
            
            # Verify size limit before starting
            if target_size > self.MAX_SIZES[resolution]:
                raise Exception(f"Target size {target_size}MB exceeds {resolution} limit of {self.MAX_SIZES[resolution]}MB")

            # Calculate guaranteed safe bitrate
            probe = ffmpeg.probe(input_file)
            duration = float(probe['format']['duration'])
            safe_bitrate = int((((self.MAX_SIZES[resolution] * 0.95) * 8 * 1024 * 1024) / duration))

            # Command with strict bitrate control
            cmd = [
                'ffmpeg', '-y',
                '-i', input_file,
                '-c:v', 'h264_nvenc',
                '-preset', 'p7',
                '-rc', 'vbr',
                '-b:v', f'{safe_bitrate}',
                '-maxrate', f'{safe_bitrate}',
                '-bufsize', f'{safe_bitrate*2}',
                '-vf', f'scale=-2:{self.quality_params[resolution]["height"]}',
                '-c:a', 'aac',
                '-b:a', self.quality_params[resolution]['audio_bitrate'],
                '-movflags', '+faststart',
                output_file
            ]

            self.logger.info(f"Running command: {' '.join(cmd)}")

            # Start process with better pipe handling
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'win32' else 0
            )

            start_time = time.time()
            last_progress_time = start_time
            last_size = 0
            stall_count = 0

            while process.poll() is None:
                try:
                    # Check timeout
                    if time.time() - start_time > self.process_timeout:
                        process.kill()
                        raise Exception("Encoding timed out after 2 hours")

                    # Read stderr for ffmpeg progress
                    stderr_line = process.stderr.readline().strip()
                    if stderr_line and 'frame=' in stderr_line:
                        # Update single line for FFmpeg progress
                        self._update_progress_line(f"FFmpeg: {stderr_line}")

                    if os.path.exists(output_file):
                        current_size = os.path.getsize(output_file)/(1024*1024)
                        current_time = time.time()
                        elapsed = current_time - start_time

                        # Check for stalled encoding
                        if current_size == last_size:
                            stall_count += 1
                            if stall_count > 30:  # 30 seconds stall
                                process.kill()
                                raise Exception("Encoding stalled - no progress for 30 seconds")
                        else:
                            stall_count = 0
                            last_size = current_size

                        # Update progress
                        if current_time - last_progress_time >= self.progress_interval:
                            speed = current_size / elapsed if elapsed > 0 else 0
                            eta = (target_size - current_size) / speed if speed > 0 else 0

                            status = (
                                f"🎬 Encoding {resolution}\n"
                                f"⚡ Speed: {speed:.2f} MB/s\n"
                                f"📊 Size: {current_size:.1f}MB / {target_size}MB\n"
                                f"⏱️ Time: {int(elapsed)}s\n"
                                f"⌛ ETA: {int(eta)}s"
                            )
                            
                            await progress_callback(current_size, target_size, status)
                            last_progress_time = current_time

                    await asyncio.sleep(0.1)  # Shorter sleep for more responsive updates

                except asyncio.CancelledError:
                    process.kill()
                    raise
                except Exception as e:
                    self.logger.error(f"Progress update error: {e}")

            print()  # New line after encoding completes

            # Check process completion
            if process.returncode != 0:
                stderr = process.stderr.read()
                raise Exception(f"FFmpeg failed with code {process.returncode}: {stderr}")

            # Verify final size
            final_size = os.path.getsize(output_file)/(1024*1024)
            if final_size > self.MAX_SIZES[resolution]:
                raise Exception(f"Encoded file size {final_size:.1f}MB exceeds {resolution} limit")

            # Wait for process to finish and release file handles
            if process:
                process.communicate()
                process.terminate()
                process = None

            return output_file, None

        except Exception as e:
            encode_logger.error(f"Encoding error: {str(e)}")
            if process:
                process.terminate()
            if os.path.exists(output_file):
                os.remove(output_file)
            raise

    def _calculate_bitrate(self, target_size: int, duration: float) -> int:
        """Calculate video bitrate in kbps"""
        # Convert target size from MB to bits (minus 5% for audio)
        target_bits = (target_size * 0.95) * 8 * 1024 * 1024
        # Calculate bitrate (bits per second)
        return int(target_bits / duration)

    def _format_eta(self, seconds: float) -> str:
        if seconds < 0 or seconds > 18000:  # 5 hours max
            return "∞"
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"
