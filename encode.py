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
            '480p': 105,  # ~100MB + 10% tolerance
            '720p': 210,  # ~200MB + 10% tolerance
            '1080p': 315  # ~300MB + 10% tolerance
        }
        self.SIZE_TOLERANCE = 1.1  # Allow 10% over target
        self.last_line_length = 0  # For single-line updates

        # Optimized encoding parameters for better speed
        self.x264_params = {
            'preset': 'ultrafast',     # Fastest encoding preset
            'tune': 'fastdecode',      # Optimize for decoding speed
            'profile': 'high',
            'level': '4.1',
            'threads': 0,              # Auto thread detection
            'thread-type': 3,          # Frame + Slice threading
            'thread-input': 1,         # Enable threaded input
            'thread-output': 1,        # Enable threaded output
            'asm': 'auto',            # Enable all SIMD optimizations
            'stats': 1,               # Enable encoding stats
            'fast-pskip': 1,          # Enable fast pskip
            'rc-lookahead': 20,       # Reduce lookahead for speed
            'direct-pred': 1,         # Fast direct prediction
            'weightb': 0,             # Disable weighted prediction
            'mixed-refs': 0,          # Disable mixed references
            'me_method': 'dia',       # Fast motion estimation
            'subq': 1,                # Fast subpixel refinement
            'trellis': 0,             # Disable trellis optimization
            'no-mbtree': None,        # Disable macroblock tree
            'sync-lookahead': 0       # Disable lookahead sync
        }
        
        # Process monitoring settings
        self.stall_timeout = 10  # Reduce stall detection time
        self.min_progress = 0.1  # Minimum progress per check (MB)
        self.progress_check_interval = 2  # Check progress every 2 seconds
        self.TARGET_MARGIN = 1.2  # Allow 20% over target size
        self.MIN_SIZE_FACTOR = 0.6  # Minimum 60% of target size

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

    def _calculate_dynamic_target(self, input_size: float, progress: float, 
                                current_size: float, base_target: int) -> float:
        """Calculate dynamic target size based on encoding progress"""
        if progress < 10:
            # Too early to estimate
            return base_target
            
        # Project final size based on current progress
        projected_size = (current_size / (progress / 100))
        
        # Adjust target if projection is reasonable
        if projected_size < base_target * self.TARGET_MARGIN:
            return projected_size
        return base_target * self.TARGET_MARGIN

    async def encode_video(self, input_file: str, output_file: str, 
                          target_size: int, resolution: str,
                          progress_callback=None) -> Tuple[str, Dict]:
        try:
            # Calculate target bitrate
            probe = ffmpeg.probe(input_file)
            duration = float(probe['format']['duration'])
            total_bitrate = int((target_size * 8 * 1024 * 1024) / duration)
            audio_bitrate = int(self.quality_params[resolution]['audio_bitrate'].replace('k', '000'))
            video_bitrate = total_bitrate - audio_bitrate

            # Enhanced FFmpeg command with optimized parameters
            cmd = [
                'ffmpeg', '-y',
                '-hwaccel', 'auto',    # Enable hardware acceleration if available
                '-i', input_file,
                '-c:v', 'libx264',
                '-preset', self.x264_params['preset'],
                '-tune', self.x264_params['tune'],
                '-profile:v', self.x264_params['profile'],
                '-level', self.x264_params['level'],
                '-b:v', f'{video_bitrate}',
                '-maxrate', f'{int(video_bitrate * 2)}',
                '-bufsize', f'{int(video_bitrate * 4)}',
                '-refs', '2',          # Reduce reference frames
                '-bf', '3',           # Maximum B-frames
                '-flags', '+cgop',     # Closed GOP
                '-vf', f'scale=-2:{self.quality_params[resolution]["height"]}:flags=fast_bilinear',
                '-c:a', 'copy',
                '-b:a', self.quality_params[resolution]['audio_bitrate'],
                '-ac', '2',
                '-ar', '48000',
                '-max_muxing_queue_size', '4096',
                '-movflags', '+faststart+frag_keyframe+empty_moov',
                '-y',
                output_file
            ]

            process = None
            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )

                last_progress_time = time.time()
                last_size = 0
                encoded_file = None
                start_time = time.time()

                while process.poll() is None:
                    if os.path.exists(output_file):
                        current_size = os.path.getsize(output_file)/(1024*1024)
                        current_time = time.time()
                        elapsed = current_time - start_time

                        # Calculate progress percentage
                        progress = self._estimate_progress(process.stderr)
                        
                        # Get dynamic target size
                        dynamic_target = self._calculate_dynamic_target(
                            current_size, progress, current_size, target_size
                        )

                        if current_time - last_progress_time >= self.progress_check_interval:
                            speed = current_size / elapsed if elapsed > 0 else 0
                            eta = self._estimate_eta(progress, elapsed)

                            status = (
                                f"🎬 Encoding {resolution}\n"
                                f"⚡ Speed: {speed:.2f} MB/s\n"
                                f"📊 Size: {current_size:.1f}MB\n"
                                f"📈 Progress: {progress:.1f}%\n"
                                f"⏱️ ETA: {self._format_eta(eta)}"
                            )
                            
                            if progress > 10:  # Show projection after 10%
                                status += f"\n🎯 Projected: {dynamic_target:.1f}MB"
                                
                            if progress_callback:
                                await progress_callback(current_size, dynamic_target, status)
                            
                            last_progress_time = current_time
                            last_size = current_size

                        await asyncio.sleep(0.1)

                # Check final result
                if process.returncode != 0:
                    stderr = process.stderr.read()
                    raise Exception(f"FFmpeg error: {stderr}")

                if not os.path.exists(output_file):
                    raise Exception("Output file not found")

                final_size = os.path.getsize(output_file)/(1024*1024)
                if final_size > target_size:
                    size_excess = ((final_size - target_size) / target_size) * 100
                    print(f"⚠️ Warning: Encoded size {final_size:.1f}MB exceeds target {target_size}MB by {size_excess:.1f}%")
                    
                    # Only raise error if exceeds maximum tolerance
                    if final_size > target_size * self.SIZE_TOLERANCE:
                        raise Exception(f"Encoded file size {final_size:.1f}MB exceeds maximum limit")

                return output_file, {
                    'target_exceeded': final_size > target_size,
                    'final_size': final_size,
                    'size_excess': ((final_size - target_size) / target_size) * 100 if final_size > target_size else 0
                }

            finally:
                if process and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()

        except Exception as e:
            self.logger.error(f"Encoding error: {str(e)}")
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

    def _estimate_progress(self, stderr) -> float:
        """Estimate encode progress from ffmpeg output"""
        try:
            for line in stderr:
                if 'time=' in line:
                    # Extract time and duration info
                    time_match = re.search(r'time=(\d+:\d+:\d+.\d+)', line)
                    if time_match:
                        current = self._time_to_seconds(time_match.group(1))
                        return (current / self.total_duration) * 100
        except:
            pass
        return 0

    def _estimate_eta(self, progress: float, elapsed: float) -> float:
        """Estimate remaining time based on progress"""
        if progress <= 0:
            return 0
        return (elapsed / progress) * (100 - progress)

    def _time_to_seconds(self, time_str: str) -> float:
        """Convert FFmpeg time string to seconds"""
        h, m, s = time_str.split(':')
        return float(h) * 3600 + float(m) * 60 + float(s)
