import ffmpeg
import os
import time
import asyncio
import psutil
from typing import Dict, Tuple

class CPUEncoder:
    def __init__(self):
        self.quality_params = {
            '480p': {
                'target_size': 110,  # MB
                'audio_bitrate': '64k',
                'height': 480,
                'crf': 23,
                'preset': 'medium'
            },
            '720p': {
                'target_size': 210,
                'audio_bitrate': '96k',
                'height': 720,
                'crf': 24,
                'preset': 'medium'
            },
            '1080p': {
                'target_size': 310,
                'audio_bitrate': '128k',
                'height': 1080,
                'crf': 25,
                'preset': 'medium'
            }
        }
        self.ram_mb = (psutil.virtual_memory().total / (1024 * 1024)) * 0.6  # Use 60% of total RAM
        self.x264_params = {
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
        self.process_priority = 10  # Nice value for Linux (lower means higher priority)

    async def encode_video(self, input_file: str, output_file: str, 
                          target_size: int, resolution: str,
                          progress_callback=None) -> Tuple[str, Dict]:
        try:
            params = self.quality_params[resolution]
            probe = ffmpeg.probe(input_file)
            duration = float(probe['format']['duration'])
            
            # Calculate bitrate for target size
            video_bitrate = int((params['target_size'] * 8 * 1024 * 1024 * 0.95) / duration)
            
            # Build encoding parameters
            stream = ffmpeg.input(input_file)
            stream = ffmpeg.output(stream, output_file,
                **{
                    'c:v': 'libx264',  # CPU encoder
                    'b:v': f'{video_bitrate}',
                    'maxrate': f'{int(video_bitrate * 1.2)}',
                    'bufsize': f'{int(video_bitrate * 2)}',
                    'preset': self.x264_params['preset'],
                    'tune': self.x264_params['tune'],
                    'threads': self.x264_params['threads'],
                    'thread-input': self.x264_params['thread-input'],
                    'thread-output': self.x264_params['thread-output'],
                    'asm': self.x264_params['asm'],
                    'prefetch-factor': self.x264_params['prefetch-factor'],
                    'cache-size': self.x264_params['cache-size'],
                    'crf': params['crf'],
                    'vf': f'scale=-2:{params["height"]}:flags=lanczos',  # Better scaling
                    'c:a': 'copy',
                    'c:s': 'copy',
                    'map': ['0:v', '0:a', '0:s'],
                    'movflags': self.x264_params['movflags'],
                    'y': None,
                    'loglevel': 'error'
                }
            )

            # Start encoding with process management
            process = ffmpeg.run_async(
                stream,
                pipe_stdout=True,
                pipe_stderr=True,
                overwrite_output=True
            )
            
            # Set process priority
            try:
                psutil.Process(process.pid).nice(self.process_priority)
            except Exception as e:
                print(f"Failed to set process priority: {e}")

            start_time = time.time()
            last_update = 0
            last_size = 0
            failed_updates = 0

            while process.poll() is None:
                try:
                    await asyncio.sleep(0.5)
                    if os.path.exists(output_file):
                        current_size = os.path.getsize(output_file)/(1024*1024)
                        elapsed = time.time() - start_time
                        speed = current_size / elapsed if elapsed > 0 else 0

                        if current_size != last_size and time.time() - last_update >= 1:
                            try:
                                progress = (current_size / params['target_size']) * 100
                                cpu_percent = psutil.cpu_percent(interval=None)
                                status = (
                                    f"🎬 Encoding {resolution} (CPU)\n"
                                    f"⚡ Speed: {speed:.2f} MB/s\n"
                                    f"📊 Size: {current_size:.1f}MB / {params['target_size']}MB\n"
                                    f"📈 Progress: {progress:.1f}%\n"
                                    f"💻 CPU Usage: {cpu_percent}%\n"
                                    f"🎯 Preset: {params['preset']}"
                                )
                                await progress_callback(current_size, params['target_size'], status)
                                last_update = time.time()
                                last_size = current_size
                                failed_updates = 0
                            except Exception as e:
                                failed_updates += 1
                                if failed_updates >= 5:
                                    raise Exception(f"Too many failed progress updates: {e}")
                                print(f"Progress update failed: {e}")

                except asyncio.CancelledError:
                    process.terminate()
                    raise
                except Exception as e:
                    if "Connection" in str(e):
                        print(f"Connection error in encoder: {e}")
                        await asyncio.sleep(1)
                        continue
                    raise

            if process.returncode != 0:
                raise Exception(f"Encoding failed with code {process.returncode}")

            return output_file, process

        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                raise
            raise Exception(f"CPU encoding failed: {str(e)}")
