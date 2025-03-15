import ffmpeg
import os
import time
import asyncio
import psutil
import subprocess
import logging
from typing import Dict, Tuple, Optional, Any, Union
from config import Config

class CPUEncoder:
    """
    CPU-based video encoder using FFmpeg with optimized parameters for different resolutions.
    Implements robust error handling and progress reporting.
    """
    
    def __init__(self, logger=None):
        """
        Initialize the CPU encoder with configuration parameters.
        
        Args:
            logger: Optional logger instance. If None, a new logger will be created.
        """
        self.logger = logger or self._setup_logger()
        self.quality_params = Config.QUALITY_PARAMS
        self.ram_mb = self._calculate_available_ram()
        self.x264_params = self._configure_x264_params()
        self.process_priority = 10  # Nice value for Linux (lower means higher priority)
        self.encoder_params = self._configure_encoder_params()
        self.process = None
        self.cancelled = False

    def _setup_logger(self) -> logging.Logger:
        """Setup a dedicated logger for the CPU encoder."""
        logger = logging.getLogger('cpu_encoder')
        logger.setLevel(logging.DEBUG)
        
        # Avoid adding handlers if they already exist
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def _calculate_available_ram(self) -> int:
        """Calculate available RAM for encoding processes (60% of total)."""
        try:
            return int((psutil.virtual_memory().total / (1024 * 1024)) * 0.6)
        except Exception as e:
            self.logger.warning(f"Failed to calculate RAM, using default: {e}")
            return 2048  # Default to 2GB if calculation fails

    def _configure_x264_params(self) -> Dict[str, Any]:
        """Configure x264 encoding parameters optimized for CPU encoding."""
        return {
            'preset': 'medium',
            'tune': 'film',
            'movflags': '+faststart',
            'threads': max(1, os.cpu_count() - 1),  # Leave one core free
            'thread-input': '1',  # Enable threaded input
            'thread-output': '1',  # Enable threaded output
            'asm': 'auto',  # Enable all CPU optimizations
            'prefetch-factor': '2',  # Increase prefetch for better RAM usage
            'cache-size': f'{int(self.ram_mb/2)}M',  # Use half of allocated RAM for cache
        }

    def _configure_encoder_params(self) -> Dict[str, Dict[str, str]]:
        """Configure codec-specific encoding parameters."""
        return {
            'h264': {
                'codec': 'libx264',
                'preset': 'medium',
                'tune': 'film',
                'crf': '23',
                'x264opts': 'me=hex:subme=7:no-fast-pskip=1:deblock=1,1'
            },
            'hevc': {
                'codec': 'libx265',
                'preset': 'medium',
                'tune': 'animation',  # Better for general content
                'x265-params': (
                    'crf=28:qcomp=0.65:psy-rd=1.0:psy-rdoq=1.0'
                    ':aq-mode=1:no-sao=1:frame-threads=4'
                    ':no-open-gop=1:repeat-headers=1:hrd=1'
                    ':pools=none'  # Disable thread pools
                )
            }
        }

    def _detect_input_codec(self, input_file: str) -> str:
        """
        Detect input file codec with proper error handling.
        
        Args:
            input_file: Path to the input video file
            
        Returns:
            String representing the codec ('hevc' or 'h264')
        """
        try:
            probe = ffmpeg.probe(input_file)
            for stream in probe['streams']:
                if stream['codec_type'] == 'video':
                    codec_name = stream.get('codec_name', '').lower()
                    if any(name in codec_name for name in ['hevc', '265', 'h265']):
                        self.logger.info(f"Detected HEVC codec in {input_file}")
                        return 'hevc'
            
            self.logger.info(f"Using default H.264 codec for {input_file}")
            return 'h264'  # Default to H.264
        except ffmpeg.Error as e:
            self.logger.error(f"FFmpeg probe error during codec detection: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")
            return 'h264'
        except Exception as e:
            self.logger.error(f"Unexpected error during codec detection: {e}")
            return 'h264'

    async def encode_video(self, 
                         input_file: str, 
                         output_file: str, 
                         target_size: int, 
                         resolution: str,
                         progress_callback: Optional[Any] = None) -> Tuple[str, Dict]:
        """
        Encode a video file using FFmpeg with CPU encoding.
        
        Args:
            input_file: Path to the input video file
            output_file: Path where the output video should be saved
            target_size: Target file size in MB
            resolution: Desired resolution ('480p', '720p', or '1080p')
            progress_callback: Optional callback for progress updates
            
        Returns:
            Tuple containing the output file path and process information
            
        Raises:
            FileNotFoundError: If the input file doesn't exist
            ValueError: If parameters are invalid
            Exception: For FFmpeg or encoding errors
        """
        self.cancelled = False
        
        # Validate inputs
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
            
        if resolution not in self.quality_params:
            raise ValueError(f"Invalid resolution: {resolution}. Must be one of {list(self.quality_params.keys())}")
            
        if target_size <= 0:
            raise ValueError(f"Target size must be positive, got {target_size}")
            
        try:
            params = self.quality_params[resolution]
            self.logger.info(f"Starting CPU encoding: {input_file} -> {output_file} ({resolution}, target: {target_size}MB)")
            
            # Probe input file
            try:
                probe = ffmpeg.probe(input_file)
                duration = float(probe['format']['duration'])
                self.logger.debug(f"Video duration: {duration:.2f} seconds")
            except ffmpeg.Error as e:
                raise ValueError(f"Failed to probe input file: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")
            
            # Calculate bitrates
            audio_bitrate = int(params['audio_bitrate'].replace('k', '000'))
            target_total_bits = target_size * 8 * 1024 * 1024
            video_bitrate = int((target_total_bits / duration) - audio_bitrate)
            
            if video_bitrate <= 0:
                raise ValueError(f"Calculated video bitrate is too low: {video_bitrate}. Please check target size and duration.")
            
            self.logger.debug(f"Calculated video bitrate: {video_bitrate} bits/s")

            # Base FFmpeg command with input
            cmd = [
                'ffmpeg', '-y', '-hide_banner',
                '-i', input_file,
                '-map', '0:v:0', '-map', '0:a:0?',
            ]

            # Add video encoding parameters
            input_codec = self._detect_input_codec(input_file)
            encoder = self.encoder_params['hevc' if input_codec == 'hevc' else 'h264']
            
            # Video codec settings
            cmd.extend([
                '-c:v', encoder['codec'],
                '-preset', encoder['preset'],
                '-tune', encoder['tune']
            ])

            # Add codec-specific parameters
            if encoder['codec'] == 'libx265':
                cmd.extend(['-x265-params', encoder['x265-params']])
            else:
                cmd.extend(['-x264opts', encoder['x264opts']])

            # Add common parameters
            cmd.extend([
                '-b:v', f'{video_bitrate}',
                '-maxrate', f'{int(video_bitrate * 1.2)}',
                '-bufsize', f'{int(video_bitrate * 1.5)}',
                '-vf', f'scale=-2:{params["height"]}',
                '-c:a', 'aac',
                '-b:a', params['audio_bitrate'],
                '-ac', '2',
                '-ar', '48000',
                '-max_muxing_queue_size', '1024',
                '-movflags', '+faststart',
                output_file
            ])

            self.logger.debug(f"FFmpeg command: {' '.join(cmd)}")

            # Start encoding process
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # Set CPU priority
            try:
                psutil.Process(self.process.pid).nice(self.process_priority)
                self.logger.debug(f"Set process priority to {self.process_priority}")
            except Exception as e:
                self.logger.warning(f"Failed to set process priority: {e}")

            start_time = time.time()
            last_update = 0
            last_size = 0
            failed_updates = 0
            stall_count = 0

            # Monitor process and provide progress updates
            while self.process.poll() is None:
                try:
                    if self.cancelled:
                        self.logger.info("Encoding cancelled by user")
                        self.process.terminate()
                        if os.path.exists(output_file):
                            os.remove(output_file)
                        raise asyncio.CancelledError("Encoding cancelled by user")
                    
                    await asyncio.sleep(0.5)
                    
                    if os.path.exists(output_file):
                        current_size = os.path.getsize(output_file)/(1024*1024)
                        elapsed = time.time() - start_time
                        
                        # Check for stalled encoding
                        if current_size == last_size:
                            stall_count += 1
                            if stall_count > 60:  # 30 seconds with no progress
                                self.logger.warning(f"Encoding appears stalled at {current_size:.2f}MB for 30 seconds")
                                # Don't raise an error, just log a warning
                        else:
                            stall_count = 0
                        
                        speed = current_size / elapsed if elapsed > 0 else 0

                        if current_size != last_size and time.time() - last_update >= 1:
                            try:
                                progress = min(99.9, (current_size / params['target_size']) * 100)
                                
                                # Estimate completion time
                                if progress > 0:
                                    eta_seconds = (elapsed / progress) * (100 - progress)
                                    eta_str = self._format_eta(eta_seconds)
                                else:
                                    eta_str = "calculating..."
                                
                                cpu_percent = psutil.cpu_percent(interval=None)
                                status = (
                                    f"🎬 Encoding {resolution} (CPU)\n"
                                    f"⚡ Speed: {speed:.2f} MB/s\n"
                                    f"📊 Size: {current_size:.1f}MB / {params['target_size']}MB\n"
                                    f"📈 Progress: {progress:.1f}%\n"
                                    f"⏱️ ETA: {eta_str}\n"
                                    f"💻 CPU Usage: {cpu_percent}%\n"
                                    f"🎯 Preset: {params['preset']}"
                                )
                                
                                if progress_callback:
                                    await progress_callback(current_size, params['target_size'], status)
                                
                                last_update = time.time()
                                last_size = current_size
                                failed_updates = 0
                                
                                self.logger.debug(f"Progress: {progress:.1f}%, Size: {current_size:.1f}MB")
                            except Exception as e:
                                failed_updates += 1
                                if failed_updates >= 5:
                                    self.logger.error(f"Too many failed progress updates: {e}")
                                    raise Exception(f"Too many failed progress updates: {e}")
                                self.logger.warning(f"Progress update failed: {e}")

                except asyncio.CancelledError:
                    self.logger.info("Encoding task cancelled")
                    if self.process and self.process.poll() is None:
                        self.process.terminate()
                        await asyncio.sleep(0.5)
                        if self.process.poll() is None:
                            self.process.kill()
                    raise
                except Exception as e:
                    if "Connection" in str(e):
                        self.logger.warning(f"Connection error in encoder, retrying: {e}")
                        await asyncio.sleep(1)
                        continue
                    raise

            # Process completed - check return code
            if self.process.returncode != 0:
                stderr_output = self.process.stderr.read() if hasattr(self.process, 'stderr') and self.process.stderr else "Unknown error"
                self.logger.error(f"Encoding failed with code {self.process.returncode}: {stderr_output}")
                raise Exception(f"Encoding failed with code {self.process.returncode}: {stderr_output}")

            # Verify output file
            if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
                raise Exception("Encoding completed but output file is missing or empty")
                
            final_size = os.path.getsize(output_file) / (1024 * 1024)
            self.logger.info(f"Encoding completed: {output_file} ({final_size:.2f}MB)")
            
            return output_file, self.process

        except asyncio.CancelledError:
            self.logger.info("Encoding cancelled")
            raise
        except Exception as e:
            self.logger.error(f"CPU encoding failed: {str(e)}", exc_info=True)
            if os.path.exists(output_file):
                try:
                    os.remove(output_file)
                    self.logger.info(f"Removed incomplete output file: {output_file}")
                except Exception as remove_error:
                    self.logger.warning(f"Failed to remove incomplete output file: {remove_error}")
            raise Exception(f"CPU encoding failed: {str(e)}")

    def cancel(self):
        """Cancel the current encoding process."""
        self.cancelled = True
        if self.process and self.process.poll() is None:
            self.process.terminate()
            
    def _format_eta(self, seconds: float) -> str:
        """Format estimated time remaining in a human-readable format."""
        if seconds < 0 or seconds > 18000:  # 5 hours max
            return "unknown"
            
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"