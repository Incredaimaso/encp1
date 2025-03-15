import ffmpeg
import os
import time
import asyncio
import logging
import sys
import subprocess
import re
import pathlib
from typing import Dict, Tuple, Optional, Any, Union, Callable
from cpu_encoder import CPUEncoder
from config import Config

class VideoEncoder:
    """
    High-level video encoding controller that manages the encoding process.
    Supports CPU encoding with robust error handling and progress reporting.
    """
    
    def __init__(self):
        """Initialize the video encoder with configuration and logging setup."""
        # Initialize logger
        self.logger = self._setup_logger()
        
        # Load quality parameters from Config
        self.quality_params = Config.QUALITY_PARAMS
        
        # Initialize CPU encoder
        self.cpu_encoder = CPUEncoder(logger=self.logger)
        
        # GPU detection state
        self._gpu_check_done = False
        self.gpu_available = None
        
        # Process monitoring settings
        self.process_timeout = 7200  # 2 hours max encoding time
        self.progress_interval = 1  # Check progress every second
        self.min_progress_interval = 0.5  # Minimum time between progress updates
        self.stall_timeout = 10  # Detect stalls after 10 seconds of no progress
        self.min_progress = 0.1  # Minimum progress per check (MB)
        self.progress_check_interval = 2  # Check progress every 2 seconds
        
        # Size constraints
        self.MAX_SIZES = {
            '480p': 105,  # ~100MB + 5% tolerance
            '720p': 210,  # ~200MB + 5% tolerance
            '1080p': 315   # ~300MB + 5% tolerance
        }
        self.SIZE_TOLERANCE = 1.1  # Allow 10% over target
        self.TARGET_MARGIN = 1.2  # Allow 20% over target size for dynamic targeting
        self.MIN_SIZE_FACTOR = 0.6  # Minimum 60% of target size
        
        # Terminal output handling
        self.last_line_length = 0  # For single-line updates
        
        # H.264 specific encoding parameters (as a fallback)
        self.h264_encode_params = {
            'preset': 'veryfast',
            'tune': 'film',
            'profile': 'high',
            'level': '4.1',
            'crf': 23,
            'x264opts': (
                'me=hex:subme=7:rc-lookahead=60'
                ':deblock=-1,-1:trellis=2:psy-rd=1.0,0.15'
            ),
            'movflags': '+faststart',
            'bf': 3,  # B-frames
            'g': 60,  # GOP size
            'keyint_min': 25  # Minimum GOP size
        }

    def _setup_logger(self) -> logging.Logger:
        """Set up a dedicated logger for the video encoder."""
        logger = logging.getLogger('video_encoder')
        logger.setLevel(logging.DEBUG)
        
        # Avoid adding handlers if they already exist
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    async def check_gpu(self) -> bool:
        """
        Check if GPU encoding is available.
        
        Returns:
            bool: True if GPU encoding is available, False otherwise
        """
        if self._gpu_check_done:
            return self.gpu_available
            
        try:
            # First check if nvidia-smi is available
            try:
                process = await asyncio.create_subprocess_exec(
                    'nvidia-smi',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                if process.returncode == 0:
                    self.logger.info("NVIDIA GPU detected via nvidia-smi")
                else:
                    self.logger.warning("nvidia-smi found but returned non-zero exit code")
                    self._gpu_check_done = True
                    self.gpu_available = False
                    return False
            except (OSError, FileNotFoundError):
                self.logger.warning("nvidia-smi not found, GPU encoding unavailable")
                self._gpu_check_done = True
                self.gpu_available = False
                return False

            # Then try a test encode using ffmpeg
            try:
                process = await asyncio.create_subprocess_exec(
                    'ffmpeg',
                    '-f', 'lavfi',
                    '-i', 'testsrc=duration=1:size=64x64',
                    '-c:v', 'h264_nvenc',
                    '-f', 'null',
                    '-',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode == 0:
                    self.logger.info("NVIDIA encoder test successful")
                    self._gpu_check_done = True
                    self.gpu_available = True
                    return True
                else:
                    self.logger.warning(f"NVIDIA encoder test failed: {stderr.decode()}")
            except Exception as e:
                self.logger.error(f"Error during GPU encoder test: {e}")
            
            self._gpu_check_done = True
            self.gpu_available = False
            return False
        except Exception as e:
            self.logger.error(f"GPU detection error: {e}", exc_info=True)
            self._gpu_check_done = True
            self.gpu_available = False
            return False

    def _calculate_encoding_params(self, target_size: int, duration: float) -> dict:
        """
        Calculate encoding parameters based on target size and duration.
        
        Args:
            target_size: Target file size in MB
            duration: Video duration in seconds
            
        Returns:
            dict: Dictionary of encoding parameters
        """
        try:
            # Calculate bitrate in bits per second
            target_bits = target_size * 8 * 1024 * 1024
            video_bitrate = int(target_bits / duration * 0.95)  # Reserve 5% for audio
            
            # Ensure reasonable bitrate limits
            video_bitrate = max(100_000, min(video_bitrate, 50_000_000))  # Between 100kbps and 50Mbps
            
            # Calculate maxrate and bufsize
            maxrate = min(int(video_bitrate * 1.5), 50_000_000)  # Cap at 50Mbps
            bufsize = min(int(video_bitrate * 2), 50_000_000)    # Cap at 50Mbps

            self.logger.debug(f"Calculated encoding params: bitrate={video_bitrate}, maxrate={maxrate}, bufsize={bufsize}")
            
            return {
                'b:v': f'{video_bitrate}',
                'maxrate': f'{maxrate}',
                'bufsize': f'{bufsize}'
            }
        except Exception as e:
            self.logger.error(f"Error calculating encoding parameters: {e}")
            # Return safe defaults
            return {
                'b:v': '2000k',
                'maxrate': '4000k',
                'bufsize': '6000k'
            }

    def _calculate_target_size(self, input_file: str, resolution: str) -> float:
        """
        Calculate target size based on input file and desired resolution.
        
        Args:
            input_file: Path to input video file
            resolution: Target resolution (480p, 720p, 1080p)
            
        Returns:
            float: Target size in MB
        """
        try:
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"Input file not found: {input_file}")
                
            # Get input file size in MB
            input_size = os.path.getsize(input_file) / (1024 * 1024)
            
            # Use resolution-specific size limits
            max_sizes = self.MAX_SIZES
            
            # Calculate target based on resolution
            if resolution == '480p':
                target = min(input_size * 0.35, max_sizes['480p'])  # 35% of original
            elif resolution == '720p':
                target = min(input_size * 0.55, max_sizes['720p'])  # 55% of original
            else:  # 1080p
                target = min(input_size * 0.75, max_sizes['1080p'])  # 75% of original
            
            # Ensure minimum reasonable size
            min_size = max_sizes[resolution] * 0.1  # At least 10% of max size
            target = max(target, min_size)
            
            self.logger.info(f"Target size for {resolution}: {target:.1f}MB (from {input_size:.1f}MB)")
            return target
        except FileNotFoundError as e:
            self.logger.error(f"File error calculating target size: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error calculating target size: {e}")
            # Fall back to resolution defaults
            defaults = {'480p': 95, '720p': 190, '1080p': 290}
            return defaults.get(resolution, 190)

    async def _verify_file(self, file_path: str, max_retries: int = 5) -> bool:
        """
        Verify that a file exists and is a valid video file.
        
        Args:
            file_path: Path to the file to verify
            max_retries: Maximum number of verification attempts
            
        Returns:
            bool: True if file is valid, False otherwise
        """
        self.logger.debug(f"Verifying file: {file_path}")
        
        for i in range(max_retries):
            try:
                # Check if file exists and is stable (not being written)
                if os.path.exists(file_path):
                    initial_size = os.path.getsize(file_path)
                    await asyncio.sleep(2)
                    current_size = os.path.getsize(file_path)
                    
                    if current_size == initial_size:
                        # Try to probe the file
                        probe = ffmpeg.probe(file_path, v='error')
                        
                        # Verify video and audio streams exist
                        has_video = any(s['codec_type'] == 'video' for s in probe.get('streams', []))
                        has_audio = any(s['codec_type'] == 'audio' for s in probe.get('streams', []))
                        
                        if has_video:
                            if not has_audio:
                                self.logger.warning(f"File {file_path} has video but no audio stream")
                            return True
                        else:
                            self.logger.warning(f"No video stream found in {file_path}")
                
                self.logger.debug(f"Verification attempt {i+1}/{max_retries} for {file_path}")
                await asyncio.sleep(2)
            except ffmpeg.Error as e:
                self.logger.warning(f"FFmpeg probe error: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")
            except Exception as e:
                self.logger.warning(f"Verification error: {e}")
        
        self.logger.error(f"File verification failed after {max_retries} attempts: {file_path}")
        return False

    def _update_progress_line(self, text: str):
        """
        Update the progress line in the terminal.
        
        Args:
            text: Text to display
        """
        try:
            # Clear previous line
            sys.stdout.write('\r' + ' ' * self.last_line_length)
            sys.stdout.write('\r' + text)
            sys.stdout.flush()
            self.last_line_length = len(text)
        except Exception as e:
            self.logger.warning(f"Error updating progress line: {e}")

    def _calculate_dynamic_target(self, input_size: float, progress: float, 
                               current_size: float, base_target: int) -> float:
        """
        Calculate dynamic target size based on encoding progress.
        
        Args:
            input_size: Size of input file in MB
            progress: Current encoding progress (0-100)
            current_size: Current output file size in MB
            base_target: Base target size in MB
            
        Returns:
            float: Adjusted target size in MB
        """
        try:
            if progress < 10:
                # Too early to estimate accurately
                return base_target
                
            # Project final size based on current progress
            projected_size = (current_size / (progress / 100))
            
            # Apply reasonable limits
            if projected_size < self.MIN_SIZE_FACTOR * base_target:
                # If projected size is too small, use minimum
                return self.MIN_SIZE_FACTOR * base_target
            elif projected_size < base_target * self.TARGET_MARGIN:
                # If projection is reasonable, use it
                return projected_size
            
            # If projection is too large, cap at target margin
            return base_target * self.TARGET_MARGIN
        except Exception as e:
            self.logger.error(f"Error calculating dynamic target: {e}")
            return base_target

    def _detect_input_codec(self, input_file: str) -> str:
        """
        Detect the codec of the input file.
        
        Args:
            input_file: Path to the input video file
            
        Returns:
            str: Detected codec ('h264', 'hevc', or 'unknown')
        """
        try:
            probe = ffmpeg.probe(input_file)
            for stream in probe['streams']:
                if stream['codec_type'] == 'video':
                    codec_name = stream.get('codec_name', '').lower()
                    if any(name in codec_name for name in ['hevc', '265', 'h265']):
                        self.logger.info(f"Detected HEVC codec in {input_file}")
                        return 'hevc'
                    elif any(name in codec_name for name in ['h264', 'avc', '264']):
                        self.logger.info(f"Detected H.264 codec in {input_file}")
                        return 'h264'
            
            self.logger.warning(f"Could not determine codec for {input_file}, defaulting to 'unknown'")
            return 'unknown'
        except ffmpeg.Error as e:
            self.logger.error(f"FFmpeg error during codec detection: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")
            return 'unknown'
        except Exception as e:
            self.logger.error(f"Error detecting input codec: {e}")
            return 'unknown'

    async def encode_video(self, 
                         input_file: str, 
                         output_file: str, 
                         target_size: Optional[int] = None, 
                         resolution: str = '720p',
                         progress_callback: Optional[Callable] = None) -> Tuple[str, Dict]:
        """
        Encode a video file using the CPU encoder.
        
        Args:
            input_file: Path to the input video file
            output_file: Path where the output video should be saved
            target_size: Target file size in MB (if None, calculated automatically)
            resolution: Desired resolution ('480p', '720p', or '1080p')
            progress_callback: Optional callback for progress updates
            
        Returns:
            Tuple containing the output file path and process information
            
        Raises:
            FileNotFoundError: If the input file doesn't exist
            ValueError: If parameters are invalid
            Exception: For FFmpeg or encoding errors
        """
        try:
            # Validate inputs
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"Input file not found: {input_file}")
                
            if resolution not in self.quality_params:
                raise ValueError(f"Invalid resolution: {resolution}. Must be one of {list(self.quality_params.keys())}")
            
            # Calculate target size if not provided
            if target_size is None or target_size <= 0:
                target_size = int(self._calculate_target_size(input_file, resolution))
            
            # Ensure output directory exists
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                self.logger.info(f"Created output directory: {output_dir}")
            
            # Delegate to CPU encoder
            self.logger.info(f"Starting CPU encoding: {input_file} -> {output_file} ({resolution}, target: {target_size}MB)")
            result_path, process_info = await self.cpu_encoder.encode_video(
                input_file, output_file, target_size, resolution, progress_callback
            )
            
            # Verify the encoded file
            if not await self._verify_file(result_path):
                raise Exception("Encoded file verification failed")
                
            # Check final file size
            final_size = os.path.getsize(result_path) / (1024 * 1024)
            max_allowed = self.MAX_SIZES[resolution]
            
            if final_size > max_allowed:
                self.logger.warning(
                    f"Encoded file exceeds maximum size: {final_size:.2f}MB > {max_allowed}MB. "
                    f"Consider re-encoding with lower quality."
                )
            
            self.logger.info(f"Encoding completed successfully: {result_path} ({final_size:.2f}MB)")
            return result_path, process_info
            
        except FileNotFoundError as e:
            self.logger.error(f"File not found: {e}")
            raise
        except ValueError as e:
            self.logger.error(f"Invalid parameter: {e}")
            raise
        except asyncio.CancelledError:
            self.logger.info("Encoding cancelled by user")
            # Clean up partial output
            if os.path.exists(output_file):
                try:
                    os.remove(output_file)
                    self.logger.info(f"Removed incomplete output file: {output_file}")
                except Exception as cleanup_error:
                    self.logger.warning(f"Failed to remove incomplete output file: {cleanup_error}")
            raise
        except Exception as e:
            self.logger.error(f"Encoding error: {e}", exc_info=True)
            # Clean up partial output
            if os.path.exists(output_file):
                try:
                    os.remove(output_file)
                    self.logger.info(f"Removed incomplete output file: {output_file}")
                except Exception as cleanup_error:
                    self.logger.warning(f"Failed to remove incomplete output file: {cleanup_error}")
            raise Exception(f"Video encoding failed: {str(e)}")

    async def _verify_encoded_file(self, file_path: str) -> bool:
        """
        Verify that the encoded file is valid and contains both video and audio streams.
        
        Args:
            file_path: Path to the file to verify
            
        Returns:
            bool: True if file is valid, False otherwise
        """
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"File does not exist: {file_path}")
                return False
                
            if os.path.getsize(file_path) == 0:
                self.logger.error(f"File is empty: {file_path}")
                return False
            
            # Try to probe the file
            probe = ffmpeg.probe(file_path)
            
            # Check for video stream
            video_stream = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)
            if not video_stream:
                self.logger.error(f"No video stream found in {file_path}")
                return False
                
            # Check for audio stream (warning only)
            audio_stream = next((s for s in probe['streams'] if s['codec_type'] == 'audio'), None)
            if not audio_stream:
                self.logger.warning(f"No audio stream found in {file_path}")
            
            # Check video duration
            if float(probe['format']['duration']) < 1.0:
                self.logger.error(f"Video duration too short: {probe['format']['duration']} seconds")
                return False
                
            self.logger.debug(f"File verification successful: {file_path}")
            return True
            
        except ffmpeg.Error as e:
            self.logger.error(f"FFmpeg error verifying file: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error verifying encoded file: {e}")
            return False

    def _calculate_bitrate(self, target_size: int, duration: float) -> int:
        """
        Calculate video bitrate in bits per second.
        
        Args:
            target_size: Target file size in MB
            duration: Video duration in seconds
            
        Returns:
            int: Video bitrate in bits per second
        """
        try:
            # Reserve 5% for audio stream
            video_size_bits = target_size * 0.95 * 8 * 1024 * 1024
            
            # Calculate bits per second
            bitrate = int(video_size_bits / duration)
            
            # Apply reasonable limits
            bitrate = max(100_000, min(bitrate, 50_000_000))  # Between 100kbps and 50Mbps
            
            self.logger.debug(f"Calculated bitrate: {bitrate} bits/s for {target_size}MB over {duration}s")
            return bitrate
            
        except Exception as e:
            self.logger.error(f"Error calculating bitrate: {e}")
            return 2_000_000  # Default to 2Mbps

    def _format_eta(self, seconds: float) -> str:
        """
        Format estimated time remaining in a human-readable format.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            str: Formatted time string
        """
        if seconds < 0 or seconds > 86400:  # 24 hours max
            return "unknown"
            
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    def _parse_time(self, time_str: str) -> float:
        """
        Parse FFmpeg time string (HH:MM:SS.ms) to seconds.
        
        Args:
            time_str: Time string in FFmpeg format
            
        Returns:
            float: Time in seconds
        """
        try:
            h, m, s = time_str.split(':')
            return float(h) * 3600 + float(m) * 60 + float(s)
        except Exception as e:
            self.logger.warning(f"Error parsing time string '{time_str}': {e}")
            return 0.0

    def cancel_encoding(self):
        """Cancel any ongoing encoding process."""
        try:
            if hasattr(self, 'cpu_encoder') and self.cpu_encoder:
                self.cpu_encoder.cancel()
                self.logger.info("Encoding cancelled by user")
        except Exception as e:
            self.logger.error(f"Error cancelling encoding: {e}")

    async def batch_encode(self, input_files: list, output_dir: str, 
                        resolutions: list = ['720p'], 
                        progress_callback: Optional[Callable] = None) -> Dict[str, Dict]:
        """
        Batch encode multiple video files.
        
        Args:
            input_files: List of input file paths
            output_dir: Directory to save encoded files
            resolutions: List of resolutions to encode (e.g. ['480p', '720p'])
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dict: Dictionary mapping input files to encoding results
        """
        results = {}
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        for input_file in input_files:
            file_results = {}
            try:
                self.logger.info(f"Processing {input_file}")
                base_name = os.path.splitext(os.path.basename(input_file))[0]
                
                for resolution in resolutions:
                    try:
                        output_file = os.path.join(output_dir, f"{base_name}_{resolution}.mp4")
                        result_path, _ = await self.encode_video(
                            input_file=input_file,
                            output_file=output_file,
                            resolution=resolution,
                            progress_callback=progress_callback
                        )
                        file_results[resolution] = {
                            'status': 'success',
                            'path': result_path,
                            'size_mb': os.path.getsize(result_path) / (1024 * 1024)
                        }
                    except Exception as e:
                        self.logger.error(f"Error encoding {input_file} to {resolution}: {e}")
                        file_results[resolution] = {
                            'status': 'failed',
                            'error': str(e)
                        }
            except Exception as e:
                self.logger.error(f"Failed to process {input_file}: {e}")
                file_results['status'] = 'failed'
                file_results['error'] = str(e)
            
            results[input_file] = file_results
        
        return results