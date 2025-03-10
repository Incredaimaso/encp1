import ffmpeg
import os
import time
import asyncio
from typing import Dict, Tuple
from cpu_encoder import CPUEncoder

class VideoEncoder:
    def __init__(self):
        self.quality_params = {
            '480p': {
                'target_size': 95,  # MB
                'audio_bitrate': '64k',
                'height': 480,
                'crf': 23  # Quality factor
            },
            '720p': {
                'target_size': 190,
                'audio_bitrate': '96k',
                'height': 720,
                'crf': 24
            },
            '1080p': {
                'target_size': 285,
                'audio_bitrate': '128k',
                'height': 1080,
                'crf': 25
            }
        }
        self.nvenc_params = {
            'preset': 'p5',
            'tune': 'hq',
            'movflags': '+faststart',
            'rc': 'vbr_hq',  # Better quality VBR mode
            'rc-lookahead': 32
        }
        self.cpu_encoder = CPUEncoder()
        self.gpu_available = self._check_gpu()
    
    def _check_gpu(self) -> bool:
        try:
            # Try to run a small GPU encode test
            test_input = ffmpeg.input('testsrc=duration=1:size=64x64', f='lavfi')
            test_output = ffmpeg.output(test_input, '-', c='h264_nvenc')
            ffmpeg.run(test_output, capture_stdout=True, capture_stderr=True)
            return True
        except ffmpeg.Error:
            print("NVIDIA GPU not available, falling back to CPU encoding")
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

    async def encode_video(self, input_file: str, output_file: str, 
                          target_size: int, resolution: str,
                          progress_callback=None) -> Tuple[str, Dict]:
        if not self.gpu_available:
            return await self.cpu_encoder.encode_video(
                input_file, output_file, target_size, resolution, progress_callback
            )
            
        # Verify input file first
        if not await self._verify_file(input_file):
            raise Exception("Input file verification failed")

        try:
            input_size = os.path.getsize(input_file)/(1024*1024)
            if input_size > 1500:  # 1.5GB limit
                raise Exception(f"Input file too large: {input_size:.1f}MB (max: 1.5GB)")

            # Get quality settings
            params = self.quality_params[resolution]
            probe = ffmpeg.probe(input_file)
            duration = float(probe['format']['duration'])

            # Calculate video bitrate leaving room for audio
            audio_size = int(params['audio_bitrate'].replace('k', '')) * duration / 8 / 1024  # MB
            available_size = params['target_size'] - audio_size
            video_bitrate = int((available_size * 8 * 1024 * 1024) / duration)

            # Build FFmpeg command with strict size control
            stream = ffmpeg.input(input_file)
            stream = ffmpeg.output(stream, output_file,
                **{
                    'c:v': 'h264_nvenc',
                    'b:v': f'{video_bitrate}',
                    'maxrate': f'{int(video_bitrate * 1.2)}',
                    'bufsize': f'{int(video_bitrate * 2)}',
                    'preset': self.nvenc_params['preset'],
                    'tune': self.nvenc_params['tune'],
                    'rc': self.nvenc_params['rc'],
                    'rc-lookahead': self.nvenc_params['rc-lookahead'],
                    'crf': params['crf'],
                    'vf': f'scale=-2:{params["height"]}',
                    'c:a': 'aac',
                    'b:a': params['audio_bitrate'],
                    'movflags': self.nvenc_params['movflags'],
                    'y': None,  # Overwrite output
                    'loglevel': 'error'
                }
            )

            # Validate video stream
            video_stream = next((s for s in probe['streams'] 
                               if s['codec_type'] == 'video'), None)
            if not video_stream:
                raise Exception("No video stream found")

            try:
                # Start encoding process
                process = ffmpeg.run_async(
                    stream, 
                    pipe_stdout=True,
                    pipe_stderr=True,
                    overwrite_output=True
                )

                # Monitor progress with file size limits
                start_time = time.time()
                last_update = 0
                last_size = 0

                # Enhanced size monitoring
                while process.poll() is None:
                    await asyncio.sleep(0.5)
                    
                    if os.path.exists(output_file):
                        current_size = os.path.getsize(output_file)/(1024*1024)
                        target_mb = params['target_size']
                        
                        # Stop if size exceeds limit
                        if current_size >= target_mb:
                            print(f"Size limit reached: {current_size:.1f}MB")
                            process.terminate()
                            raise Exception(f"Encoding stopped: Size would exceed {target_mb}MB limit")
                        
                        elapsed = time.time() - start_time
                        speed = current_size / (elapsed * 1024 * 1024)  # MB/s
                        progress = (current_size / target_mb) * 100
                        
                        if current_size != last_size and time.time() - last_update >= 1:
                            status = (
                                f"🎬 Encoding {resolution}\n"
                                f"⚡ Speed: {speed:.2f} MB/s\n"
                                f"📊 Size: {current_size:.1f}MB / {target_mb}MB\n"
                                f"📈 Progress: {progress:.1f}%\n"
                                f"🎯 Target Bitrate: {video_bitrate}"
                            )
                            await progress_callback(current_size, target_mb, status)
                            last_update = time.time()
                            last_size = current_size

                if process.returncode != 0:
                    raise Exception(f"Encoding failed with code {process.returncode}")

                return output_file, process

            except ffmpeg.Error as e:
                raise Exception(f"Encoding failed: {e.stderr.decode() if e.stderr else str(e)}")
                
        except Exception as e:
            raise Exception(f"Encoding preparation failed: {str(e)}")

    def _calculate_bitrate(self, target_size: int, duration: float) -> int:
        # Convert target size from MB to bits
        target_bits = target_size * 8 * 1024 * 1024
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
