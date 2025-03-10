import ffmpeg
import os
import time
import asyncio
from typing import Dict, Tuple

class CPUEncoder:
    def __init__(self):
        self.quality_params = {
            '480p': {
                'target_size': 95,  # MB
                'audio_bitrate': '64k',
                'height': 480,
                'crf': 23,
                'preset': 'medium'
            },
            '720p': {
                'target_size': 190,
                'audio_bitrate': '96k',
                'height': 720,
                'crf': 24,
                'preset': 'medium'
            },
            '1080p': {
                'target_size': 285,
                'audio_bitrate': '128k',
                'height': 1080,
                'crf': 25,
                'preset': 'medium'
            }
        }
        self.x264_params = {
            'preset': 'medium',
            'tune': 'film',
            'movflags': '+faststart',
            'threads': os.cpu_count()
        }

    async def encode_video(self, input_file: str, output_file: str, 
                          target_size: int, resolution: str,
                          progress_callback=None) -> Tuple[str, Dict]:
        try:
            params = self.quality_params[resolution]
            probe = ffmpeg.probe(input_file)
            duration = float(probe['format']['duration'])
            
            # Calculate bitrate for target size
            audio_size = int(params['audio_bitrate'].replace('k', '')) * duration / 8 / 1024  # MB
            available_size = params['target_size'] - audio_size
            video_bitrate = int((available_size * 8 * 1024 * 1024) / duration)
            
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
                    'crf': params['crf'],
                    'vf': f'scale=-2:{params["height"]}',
                    'c:a': 'aac',
                    'b:a': params['audio_bitrate'],
                    'movflags': self.x264_params['movflags'],
                    'y': None,
                    'loglevel': 'error'
                }
            )
            
            # Start encoding
            process = ffmpeg.run_async(
                stream,
                pipe_stdout=True,
                pipe_stderr=True
            )
            
            # Monitor progress
            start_time = time.time()
            last_update = 0
            last_size = 0
            
            while process.poll() is None:
                await asyncio.sleep(0.5)
                if os.path.exists(output_file):
                    current_size = os.path.getsize(output_file)/(1024*1024)
                    elapsed = time.time() - start_time
                    speed = current_size / elapsed if elapsed > 0 else 0
                    
                    if current_size != last_size and time.time() - last_update >= 1:
                        progress = (current_size / params['target_size']) * 100
                        status = (
                            f"🎬 Encoding {resolution} (CPU)\n"
                            f"⚡ Speed: {speed:.2f} MB/s\n"
                            f"📊 Size: {current_size:.1f}MB / {params['target_size']}MB\n"
                            f"📈 Progress: {progress:.1f}%\n"
                            f"🎯 Preset: {params['preset']}"
                        )
                        await progress_callback(current_size, params['target_size'], status)
                        last_update = time.time()
                        last_size = current_size
            
            return output_file, process
            
        except Exception as e:
            raise Exception(f"CPU encoding failed: {str(e)}")
